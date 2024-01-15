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
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.projectnessie.versioned.storage.common.persist.ObjType.CACHE_UNLIMITED;
import static org.projectnessie.versioned.storage.common.persist.ObjType.NOT_CACHED;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.serializeObj;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.cache.CaffeineStatsCounter;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.function.LongSupplier;
import org.checkerframework.checker.index.qual.NonNegative;
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

  abstract LongSupplier clockNanos();

  @Value.Derived
  Cache<CacheKeyValue, byte[]> cache() {
    Caffeine<CacheKeyValue, byte[]> cacheBuilder =
        Caffeine.newBuilder()
            .maximumWeight(capacity() * 1024L * 1024L)
            .weigher(this::weigher)
            .expireAfter(
                new Expiry<CacheKeyValue, byte[]>() {
                  @Override
                  public long expireAfterCreate(
                      CacheKeyValue key, byte[] value, long currentTimeNanos) {
                    long expire = key.expiresAtNanosEpoch;
                    if (expire == CACHE_UNLIMITED) {
                      return Long.MAX_VALUE;
                    }
                    if (expire == NOT_CACHED) {
                      return 0L;
                    }
                    long remaining = expire - currentTimeNanos;
                    return Math.max(0L, remaining);
                  }

                  @Override
                  public long expireAfterUpdate(
                      CacheKeyValue key,
                      byte[] value,
                      long currentTimeNanos,
                      @NonNegative long currentDurationNanos) {
                    return expireAfterCreate(key, value, currentTimeNanos);
                  }

                  @Override
                  public long expireAfterRead(
                      CacheKeyValue key,
                      byte[] value,
                      long currentTimeNanos,
                      @NonNegative long currentDurationNanos) {
                    return currentDurationNanos;
                  }
                })
            .ticker(clockNanos()::getAsLong);
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

  private int weigher(CacheKeyValue key, byte[] value) {
    int size = key.heapSize();
    if (value != null) {
      size += ARRAY_OVERHEAD + value.length;
    }
    size += CAFFEINE_OBJ_OVERHEAD;
    return size;
  }

  @Override
  public Obj get(@Nonnull String repositoryId, @Nonnull ObjId id) {
    CacheKeyValue key = cacheKey(repositoryId, id);
    byte[] value = cache().getIfPresent(key);
    return value != null ? ProtoSerialization.deserializeObj(id, value, null) : null;
  }

  @Override
  public void put(@Nonnull String repositoryId, @Nonnull Obj obj) {
    long expiresAt =
        obj.type()
            .cachedObjectExpiresAtMicros(obj, () -> NANOSECONDS.toMicros(clockNanos().getAsLong()));
    if (expiresAt == NOT_CACHED) {
      return;
    }

    try {
      byte[] serialized = serializeObj(obj, Integer.MAX_VALUE, Integer.MAX_VALUE, true);
      long expiresAtNanos =
          expiresAt == CACHE_UNLIMITED ? CACHE_UNLIMITED : MICROSECONDS.toNanos(expiresAt);
      CacheKeyValue keyValue = cacheKeyValue(repositoryId, obj.id(), expiresAtNanos);
      cache().put(keyValue, serialized);
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
      String repositoryId, ObjId id, long expiresAtNanosEpoch) {
    return new CacheKeyValue(repositoryId, id, expiresAtNanosEpoch);
  }

  /**
   * Class used for both the cache key and cache value including the expiration timestamp. This is
   * (should be) more efficient (think: mono-morphic vs bi-morphic call sizes) and more GC/heap
   * friendly (less object instances) than having different object types.
   */
  static final class CacheKeyValue {

    final String repositoryId;
    // ObjId256 heap size: 40 bytes (assumed, jol)
    final ObjId id;

    // Revisit this field before 2262-04-11T23:47:16.854Z (64-bit signed long overflow) ;) ;)
    final long expiresAtNanosEpoch;

    CacheKeyValue(String repositoryId, ObjId id) {
      this(repositoryId, id, 0L);
    }

    CacheKeyValue(String repositoryId, ObjId id, long expiresAtNanosEpoch) {
      this.repositoryId = repositoryId;
      this.id = id;
      this.expiresAtNanosEpoch = expiresAtNanosEpoch;
    }

    int heapSize() {
      int size = OBJ_SIZE;
      size += STRING_OBJ_OVERHEAD + repositoryId.length();
      size += id.heapSize();
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

  /*
  org.projectnessie.versioned.storage.cache.CaffeineCacheBackend$CacheKeyValue object internals:
  OFF  SZ                                                       TYPE DESCRIPTION                  VALUE
    0   8                                                            (object header: mark)        0x0000000000000001 (non-biasable; age: 0)
    8   4                                                            (object header: class)       0x010c4800
   12   4                                           java.lang.String CacheKeyValue.repositoryId   null
   16   8                                                       long CacheKeyValue.expiresAt      0
   24   4   org.projectnessie.versioned.storage.common.persist.ObjId CacheKeyValue.id             null
   28   4                                                            (object alignment gap)
  Instance size: 32 bytes
  Space losses: 0 bytes internal + 4 bytes external = 4 bytes total
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
  /*
  Assume an overhead of 2 objects for each entry (java.util.concurrent.ConcurrentHashMap$Node is 32 bytes) in Caffeine.
  */
  static final int CAFFEINE_OBJ_OVERHEAD = 2 * 32;
}
