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
  Cache<CacheKey, byte[]> cache() {
    Caffeine<CacheKey, byte[]> cacheBuilder =
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
    ObjCacheImpl cache = new ObjCacheImpl(this, persist.config().repositoryId());
    return new CachingPersistImpl(persist, cache);
  }

  private int weigher(CacheKey key, byte[] data) {
    return key.heapSize() + JAVA_OBJ_HEADER + data.length;
  }

  @Override
  public Obj get(@Nonnull String repositoryId, @Nonnull ObjId id) {
    CacheKey key = cacheKey(repositoryId, id);
    byte[] bytes = cache().getIfPresent(key);
    return bytes != null ? ProtoSerialization.deserializeObj(id, bytes) : null;
  }

  @Override
  public void put(@Nonnull String repositoryId, @Nonnull Obj obj) {
    CacheKey key = cacheKey(repositoryId, obj.id());
    try {
      cache().put(key, serializeObj(obj, Integer.MAX_VALUE, Integer.MAX_VALUE));
    } catch (ObjTooLargeException e) {
      // this should never happen
      throw new RuntimeException(e);
    }
  }

  @Override
  public void remove(@Nonnull String repositoryId, @Nonnull ObjId id) {
    CacheKey key = cacheKey(repositoryId, id);
    cache().invalidate(key);
  }

  @Override
  public void clear(@Nonnull String repositoryId) {
    cache().asMap().keySet().removeIf(k -> k.repositoryId.equals(repositoryId));
  }

  private CacheKey cacheKey(String repositoryId, ObjId id) {
    return new CacheKey(repositoryId, id);
  }

  static final class CacheKey {

    static final int HEAP_OVERHEAD = 3 * JAVA_OBJ_HEADER;
    final String repositoryId;
    final ObjId id;

    CacheKey(String repositoryId, ObjId id) {
      this.repositoryId = repositoryId;
      this.id = id;
    }

    int heapSize() {
      return HEAP_OVERHEAD + id.size() + repositoryId.length();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof CacheKey)) {
        return false;
      }
      CacheKey cacheKey = (CacheKey) o;
      return repositoryId.equals(cacheKey.repositoryId) && id.equals(cacheKey.id);
    }

    @Override
    public int hashCode() {
      return repositoryId.hashCode() * 31 + id.hashCode();
    }

    @Override
    public String toString() {
      return "CacheKey{" + repositoryId + ", " + id + '}';
    }
  }
}
