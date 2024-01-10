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
import static org.projectnessie.versioned.storage.common.persist.ObjType.CACHE_UNLIMITED;
import static org.projectnessie.versioned.storage.common.persist.ObjType.NOT_CACHED;
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
    if (expire != CACHE_UNLIMITED && expire - clock.getAsLong() <= 0L) {
      cache().invalidate(key);
      return null;
    }
    return ProtoSerialization.deserializeObj(id, value.value);
  }

  @Override
  public void put(@Nonnull String repositoryId, @Nonnull Obj obj, LongSupplier clock) {
    long expiresAt = obj.type().cachedObjectExpiresAtMicros(obj, clock);
    if (expiresAt == NOT_CACHED) {
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

    /*
    org.projectnessie.versioned.storage.cache.CaffeineCacheBackend$CacheKeyValue object internals:
    OFF  SZ                                                       TYPE DESCRIPTION                  VALUE
      0   8                                                            (object header: mark)        0x0000000000000001 (non-biasable; age: 0)
      8   4                                                            (object header: class)       0x010c4800
     12   4                                           java.lang.String CacheKeyValue.repositoryId   null
     16   8                                                       long CacheKeyValue.expiresAt      0
     24   4   org.projectnessie.versioned.storage.common.persist.ObjId CacheKeyValue.id             null
     28   4                                                     byte[] CacheKeyValue.value          null
    Instance size: 32 bytes
    Space losses: 0 bytes internal + 0 bytes external = 0 bytes total
    */
    static final int OBJ_SIZE = 32;
    /*
    Array overhead: 16 bytes
    */
    static final int ARRAY_OVERHEAD = 16;
    /*
    java.lang.String object internals:
    OFF  SZ      TYPE DESCRIPTION               VALUE
      0   8           (object header: mark)     0x0000000000000001 (non-biasable; age: 0)
      8   4           (object header: class)    0x0000e8d8
     12   4       int String.hash               0
     16   1      byte String.coder              0
     17   1   boolean String.hashIsZero         false
     18   2           (alignment/padding gap)
     20   4    byte[] String.value              []
    Instance size: 24 bytes
    Space losses: 2 bytes internal + 0 bytes external = 2 bytes total
    */
    static final int STRING_OBJ_OVERHEAD = 24 + ARRAY_OVERHEAD;
    final String repositoryId;
    // ObjId256 heap size: 40 bytes (assumed, jol)
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
      int size = OBJ_SIZE;
      size += STRING_OBJ_OVERHEAD + repositoryId.length();
      size += id.heapSize();
      byte[] v = value;
      if (v != null) {
        size += ARRAY_OVERHEAD + v.length;
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
