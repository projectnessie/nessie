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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.projectnessie.versioned.storage.common.persist.ObjType.CACHE_UNLIMITED;
import static org.projectnessie.versioned.storage.common.persist.ObjType.NOT_CACHED;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.deserializeReference;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.serializeObj;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.serializeReference;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.cache.CaffeineStatsCounter;
import jakarta.annotation.Nonnull;
import java.lang.ref.SoftReference;
import java.time.Duration;
import org.checkerframework.checker.index.qual.NonNegative;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;
import org.projectnessie.versioned.storage.serialize.ProtoSerialization;

class CaffeineCacheBackend implements CacheBackend {

  public static final String CACHE_NAME = "nessie-objects";
  private static final CacheKeyValue NON_EXISTING_SENTINEL =
      new CacheKeyValue("x", ObjId.EMPTY_OBJ_ID, 0L, new byte[0], null);

  private final CacheConfig config;
  final Cache<CacheKeyValue, CacheKeyValue> cache;

  private final long refCacheTtlNanos;
  private final long refCacheNegativeTtlNanos;

  CaffeineCacheBackend(CacheConfig config) {
    this.config = config;

    refCacheTtlNanos = config.referenceTtl().orElse(Duration.ZERO).toNanos();
    refCacheNegativeTtlNanos = config.referenceNegativeTtl().orElse(Duration.ZERO).toNanos();

    Caffeine<CacheKeyValue, CacheKeyValue> cacheBuilder =
        Caffeine.newBuilder()
            .maximumWeight(config.capacityMb() * 1024L * 1024L)
            .weigher(this::weigher)
            .expireAfter(
                new Expiry<CacheKeyValue, CacheKeyValue>() {
                  @Override
                  public long expireAfterCreate(
                      CacheKeyValue key, CacheKeyValue value, long currentTimeNanos) {
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
                      CacheKeyValue value,
                      long currentTimeNanos,
                      @NonNegative long currentDurationNanos) {
                    return expireAfterCreate(key, value, currentTimeNanos);
                  }

                  @Override
                  public long expireAfterRead(
                      CacheKeyValue key,
                      CacheKeyValue value,
                      long currentTimeNanos,
                      @NonNegative long currentDurationNanos) {
                    return currentDurationNanos;
                  }
                })
            .ticker(config.clockNanos()::getAsLong);
    config
        .meterRegistry()
        .ifPresent(
            meterRegistry -> {
              cacheBuilder.recordStats(() -> new CaffeineStatsCounter(meterRegistry, CACHE_NAME));
              meterRegistry.gauge(
                  "cache_capacity_mb",
                  singletonList(Tag.of("cache", CACHE_NAME)),
                  "",
                  x -> config.capacityMb());
            });

    this.cache = cacheBuilder.build();
  }

  @Override
  public Persist wrap(@Nonnull Persist persist) {
    ObjCacheImpl cache = new ObjCacheImpl(this, persist.config());
    return new CachingPersistImpl(persist, cache);
  }

  private int weigher(CacheKeyValue key, CacheKeyValue value) {
    int size = key.heapSize();
    size += CAFFEINE_OBJ_OVERHEAD;
    return size;
  }

  @Override
  public Obj get(@Nonnull String repositoryId, @Nonnull ObjId id) {
    CacheKeyValue key = cacheKey(repositoryId, id);
    CacheKeyValue value = cache.getIfPresent(key);
    if (value == null) {
      return null;
    }
    if (value == NON_EXISTING_SENTINEL) {
      return NOT_FOUND_OBJ_SENTINEL;
    }
    return value.getObj();
  }

  @Override
  public void put(@Nonnull String repositoryId, @Nonnull Obj obj) {
    putLocal(repositoryId, obj);
  }

  @Override
  public void putLocal(@Nonnull String repositoryId, @Nonnull Obj obj) {
    long expiresAt =
        obj.type()
            .cachedObjectExpiresAtMicros(
                obj, () -> NANOSECONDS.toMicros(config.clockNanos().getAsLong()));
    if (expiresAt == NOT_CACHED) {
      return;
    }

    try {
      byte[] serialized = serializeObj(obj, Integer.MAX_VALUE, Integer.MAX_VALUE, true);
      long expiresAtNanos =
          expiresAt == CACHE_UNLIMITED ? CACHE_UNLIMITED : MICROSECONDS.toNanos(expiresAt);
      CacheKeyValue keyValue =
          cacheKeyValue(repositoryId, obj.id(), expiresAtNanos, serialized, obj);
      cache.put(keyValue, keyValue);
    } catch (ObjTooLargeException e) {
      // this should never happen
      throw new RuntimeException(e);
    }
  }

  @Override
  public void putNegative(@Nonnull String repositoryId, @Nonnull ObjId id, @Nonnull ObjType type) {
    long expiresAt =
        type.negativeCacheExpiresAtMicros(
            () -> NANOSECONDS.toMicros(config.clockNanos().getAsLong()));
    if (expiresAt == NOT_CACHED) {
      remove(repositoryId, id);
      return;
    }

    long expiresAtNanos =
        expiresAt == CACHE_UNLIMITED ? CACHE_UNLIMITED : MICROSECONDS.toNanos(expiresAt);
    CacheKeyValue keyValue = cacheKeyValue(repositoryId, id, expiresAtNanos);

    cache.put(keyValue, NON_EXISTING_SENTINEL);
  }

  @Override
  public void remove(@Nonnull String repositoryId, @Nonnull ObjId id) {
    CacheKeyValue key = cacheKey(repositoryId, id);
    cache.invalidate(key);
  }

  @Override
  public void clear(@Nonnull String repositoryId) {
    cache.asMap().keySet().removeIf(k -> k.repositoryId.equals(repositoryId));
  }

  private ObjId refObjId(String name) {
    return ObjId.objIdFromByteArray(("r:" + name).getBytes(UTF_8));
  }

  @Override
  public void removeReference(@Nonnull String repositoryId, @Nonnull String name) {
    if (refCacheTtlNanos <= 0L) {
      return;
    }
    ObjId id = refObjId(name);
    CacheKeyValue key = cacheKey(repositoryId, id);
    cache.invalidate(key);
  }

  @Override
  public void putReference(@Nonnull String repositoryId, @Nonnull Reference r) {
    putReferenceLocal(repositoryId, r);
  }

  @Override
  public void putReferenceLocal(@Nonnull String repositoryId, @Nonnull Reference r) {
    if (refCacheTtlNanos <= 0L) {
      return;
    }
    ObjId id = refObjId(r.name());
    CacheKeyValue keyValue =
        cacheKeyValue(
            repositoryId,
            id,
            config.clockNanos().getAsLong() + refCacheTtlNanos,
            serializeReference(r),
            r);
    cache.put(keyValue, keyValue);
  }

  @Override
  public void putReferenceNegative(@Nonnull String repositoryId, @Nonnull String name) {
    if (refCacheNegativeTtlNanos <= 0L) {
      return;
    }
    ObjId id = refObjId(name);
    CacheKeyValue key =
        cacheKeyValue(repositoryId, id, config.clockNanos().getAsLong() + refCacheNegativeTtlNanos);
    cache.put(key, NON_EXISTING_SENTINEL);
  }

  @Override
  public Reference getReference(@Nonnull String repositoryId, @Nonnull String name) {
    if (refCacheTtlNanos <= 0L) {
      return null;
    }
    ObjId id = refObjId(name);
    CacheKeyValue value = cache.getIfPresent(cacheKey(repositoryId, id));
    if (value == null) {
      return null;
    }
    if (value == NON_EXISTING_SENTINEL) {
      return NON_EXISTENT_REFERENCE_SENTINEL;
    }
    return value.getReference();
  }

  static CacheKeyValue cacheKey(String repositoryId, ObjId id) {
    return new CacheKeyValue(repositoryId, id);
  }

  private static CacheKeyValue cacheKeyValue(
      String repositoryId, ObjId id, long expiresAtNanosEpoch) {
    return new CacheKeyValue(repositoryId, id, expiresAtNanosEpoch);
  }

  private static CacheKeyValue cacheKeyValue(
      String repositoryId, ObjId id, long expiresAtNanosEpoch, byte[] serialized, Object object) {
    return new CacheKeyValue(repositoryId, id, expiresAtNanosEpoch, serialized, object);
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

    final byte[] serialized;
    java.lang.ref.Reference<Object> object;

    CacheKeyValue(String repositoryId, ObjId id) {
      this(repositoryId, id, 0L);
    }

    CacheKeyValue(String repositoryId, ObjId id, long expiresAtNanosEpoch) {
      this(repositoryId, id, expiresAtNanosEpoch, null, null);
    }

    CacheKeyValue(
        String repositoryId, ObjId id, long expiresAtNanosEpoch, byte[] serialized, Object object) {
      this.repositoryId = repositoryId;
      this.id = id;
      this.expiresAtNanosEpoch = expiresAtNanosEpoch;
      this.serialized = serialized;
      this.object = new SoftReference<>(object, null);
    }

    int heapSize() {
      int size = OBJ_SIZE;
      size += STRING_OBJ_OVERHEAD + repositoryId.length();
      size += id.heapSize();
      byte[] s = serialized;
      if (s != null) {
        size += ARRAY_OVERHEAD + s.length;
      }
      size += SOFT_REFERENCE_OVERHEAD;
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

    Obj getObj() {
      Obj obj = (Obj) this.object.get();
      if (obj == null) {
        obj = ProtoSerialization.deserializeObj(id, 0L, this.serialized, null);
        // re-create the soft reference - but don't care about JMM side effects
        this.object = new SoftReference<>(obj);
      }
      return obj;
    }

    Reference getReference() {
      Reference ref = (Reference) this.object.get();
      if (ref == null) {
        ref = deserializeReference(this.serialized);
        // re-create the soft reference - but don't care about JMM side effects
        this.object = new SoftReference<>(ref);
      }
      return ref;
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
   28   4                                                     byte[] CacheKeyValue.serialized     null
   32   4                                    java.lang.ref.Reference CacheKeyValue.object         null
   36   4                                                            (object alignment gap)
  Instance size: 40 bytes
  Space losses: 0 bytes internal + 4 bytes external = 4 bytes total
  */
  static final int OBJ_SIZE = 40;
  /*
  Array overhead: 16 bytes
  */
  static final int ARRAY_OVERHEAD = 16;
  static final int SOFT_REFERENCE_OVERHEAD = 32;
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
