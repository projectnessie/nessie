/*
 * Copyright (C) 2022 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.projectnessie.versioned.storage.cache;

import static java.util.Collections.singletonList;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.serializeObj;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.cache.CaffeineStatsCounter;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.function.LongSupplier;
import org.immutables.value.Value;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.serialize.ProtoSerialization;

@Value.Immutable
abstract class CaffeineCacheBackend implements CacheBackend {

  public static final int JAVA_OBJ_HEADER = 32;
  public static final String CACHE_NAME = "nessie-objects";

  static ImmutableCaffeineCacheBackend.Builder builder() {
    return ImmutableCaffeineCacheBackend.builder();
  }

  /** Cache capacity in MB. */
  abstract long capacity();

  @Nullable
  abstract MeterRegistry meterRegistry();

  @Value.Derived
  Cache<CacheKeyValue, CacheKeyValue> cache() {
    Caffeine<CacheKeyValue, CacheKeyValue> cacheBuilder =
        Caffeine.newBuilder().maximumWeight(capacity() * 1024L * 1024L).weigher(this::weigher);
    MeterRegistry meterRegistry = meterRegistry();
    if (meterRegistry != null) {
      cacheBuilder.recordStats(() -> new CaffeineStatsCounter(meterRegistry, CACHE_NAME));
      meterRegistry.gauge(
          "cache_capacity_mb", singletonList(Tag.of("cache", CACHE_NAME)), "", x -> capacity());
    }
    return cacheBuilder.build();
  }

  @Override
  public Persist wrap(@Nonnull Persist persist) {
    ObjCacheImpl cache = new ObjCacheImpl(this, persist.config());
    return new CachingPersistImpl(persist, cache);
  }

  private int weigher(CacheKeyValue key, CacheKeyValue data) {
    return key.heapSize();
  }

  @Override
  public Obj get(@Nonnull String repositoryId, @Nonnull ObjId id, LongSupplier clock) {
    CacheKeyValue key = cacheKey(repositoryId, id);
    CacheKeyValue value = cache().getIfPresent(key);
    if (value == null) {
      return null;
    }
    long expire = value.expiresAt;
    if (expire != -1L && expire - clock.getAsLong() <= 0L) {
      cache().invalidate(key);
      return null;
    }
    return ProtoSerialization.deserializeObj(id, value.value);
  }

  @Override
  public void put(@Nonnull String repositoryId, @Nonnull Obj obj, LongSupplier clock) {
    long expiresAt = obj.type().cachedObjectExpiresAtMicros(obj, clock);
    if (expiresAt == 0L) {
      return;
    }

    try {
      byte[] serialized = serializeObj(obj, Integer.MAX_VALUE, Integer.MAX_VALUE);
      CacheKeyValue keyValue = cacheKeyValue(repositoryId, obj.id(), serialized, expiresAt);
      cache().put(keyValue, keyValue);
    } catch (ObjTooLargeException e) {
      // this should never happen
      throw new RuntimeException(e);
    }
  }

  @Override
  public void remove(@Nonnull String repositoryId, @Nonnull ObjId id) {
    CacheKeyValue key = cacheKey(repositoryId, id);
    cache().invalidate(key);
  }

  @Override
  public void clear(@Nonnull String repositoryId) {
    cache().asMap().keySet().removeIf(k -> k.repositoryId.equals(repositoryId));
  }

  static CacheKeyValue cacheKey(String repositoryId, ObjId id) {
    return new CacheKeyValue(repositoryId, id);
  }

  private static CacheKeyValue cacheKeyValue(
      String repositoryId, ObjId id, byte[] value, long expiresAt) {
    return new CacheKeyValue(repositoryId, id, value, expiresAt);
  }

  /**
   * Class used for both the cache key and cache value including the expiration timestamp. This is
   * (should be) more efficient (think: mono-morphic vs bi-morphic call sizes) and more GC/heap
   * friendly (less object instances) than having different object types.
   */
  static final class CacheKeyValue {

    static final int HEAP_OVERHEAD = 16 + 3 * JAVA_OBJ_HEADER;
    final String repositoryId;
    final ObjId id;

    final byte[] value;
    final long expiresAt;

    CacheKeyValue(String repositoryId, ObjId id) {
      this(repositoryId, id, null, 0L);
    }

    CacheKeyValue(String repositoryId, ObjId id, final byte[] value, long expiresAt) {
      this.repositoryId = repositoryId;
      this.id = id;
      this.value = value;
      this.expiresAt = expiresAt;
    }

    int heapSize() {
      int size = HEAP_OVERHEAD + id.size() + repositoryId.length();
      byte[] v = value;
      if (v != null) {
        size += JAVA_OBJ_HEADER + v.length;
      }
      return size;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof CacheKeyValue)) {
        return false;
      }
      CacheKeyValue cacheKey = (CacheKeyValue) o;
      return repositoryId.equals(cacheKey.repositoryId) && id.equals(cacheKey.id);
    }

    @Override
    public int hashCode() {
      return repositoryId.hashCode() * 31 + id.hashCode();
    }

    @Override
    public String toString() {
      return "{" + repositoryId + ", " + id + '}';
    }
  }
}
