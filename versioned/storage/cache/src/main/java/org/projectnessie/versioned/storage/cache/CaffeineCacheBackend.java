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
import com.github.benmanes.caffeine.cache.Scheduler;
import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.binder.BaseUnits;
import io.micrometer.core.instrument.binder.cache.CaffeineStatsCounter;
import jakarta.annotation.Nonnull;
import java.lang.ref.SoftReference;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.IntConsumer;
import java.util.function.LongSupplier;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;
import org.projectnessie.versioned.storage.serialize.ProtoSerialization;

class CaffeineCacheBackend implements CacheBackend {

  public static final String CACHE_NAME = "nessie-objects";

  static final long ONE_MB = 1024L * 1024L;
  public static final String METER_CACHE_CAPACITY_MB = "cache_capacity_mb";
  public static final String METER_CACHE_CAPACITY = "cache.capacity";
  public static final String METER_CACHE_ADMIT_CAPACITY = "cache.capacity.admitted";
  public static final String METER_CACHE_WEIGHT = "cache.weight";
  public static final String METER_CACHE_REJECTED_WEIGHT = "cache.rejected-weight";

  private final CacheConfig config;
  final Cache<CacheKey, CacheValue> cache;

  private final long refCacheTtlNanos;
  private final long refCacheNegativeTtlNanos;
  private final boolean enableSoftReferences;
  private final long capacityBytes;
  private final long admitWeight;
  private final AtomicLong rejections = new AtomicLong();
  private final IntConsumer rejectionsWeight;
  private final LongSupplier weightSupplier;
  private final Lock aboveCapacityLock;

  CaffeineCacheBackend(CacheConfig config) {
    this.config = config;

    refCacheTtlNanos = config.referenceTtl().orElse(Duration.ZERO).toNanos();
    refCacheNegativeTtlNanos = config.referenceNegativeTtl().orElse(Duration.ZERO).toNanos();
    enableSoftReferences = config.enableSoftReferences().orElse(false);

    capacityBytes = config.capacityMb() * ONE_MB;
    admitWeight = capacityBytes + (long) (capacityBytes * config.cacheCapacityOvershoot());

    Caffeine<CacheKey, CacheValue> cacheBuilder =
        Caffeine.newBuilder()
            .executor(config.executor())
            .scheduler(Scheduler.disabledScheduler())
            .ticker(config.clockNanos()::getAsLong)
            .maximumWeight(capacityBytes)
            .weigher(this::weigher)
            .expireAfter(
                new Expiry<>() {
                  @Override
                  public long expireAfterCreate(
                      @Nonnull CacheKey key, @Nonnull CacheValue value, long currentTimeNanos) {
                    long expire = value.expiresAtNanosEpoch;
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
                      @Nonnull CacheKey key,
                      @Nonnull CacheValue value,
                      long currentTimeNanos,
                      long currentDurationNanos) {
                    return expireAfterCreate(key, value, currentTimeNanos);
                  }

                  @Override
                  public long expireAfterRead(
                      @Nonnull CacheKey key,
                      @Nonnull CacheValue value,
                      long currentTimeNanos,
                      long currentDurationNanos) {
                    return currentDurationNanos;
                  }
                });
    rejectionsWeight =
        config
            .meterRegistry()
            .map(
                reg -> {
                  cacheBuilder.recordStats(() -> new CaffeineStatsCounter(reg, CACHE_NAME));
                  // legacy gauge (using mb)
                  Gauge.builder(METER_CACHE_CAPACITY_MB, "", x -> config.capacityMb())
                      .description(
                          "Total capacity of the objects cache in megabytes (x1024), prefer the new "
                              + METER_CACHE_CAPACITY
                              + " metric.")
                      .tag("cache", CACHE_NAME)
                      .register(reg);

                  // new gauges (providing base unit)
                  Gauge.builder(METER_CACHE_CAPACITY, "", x -> capacityBytes)
                      .description("Total capacity of the objects cache in bytes.")
                      .tag("cache", CACHE_NAME)
                      .baseUnit(BaseUnits.BYTES)
                      .register(reg);
                  Gauge.builder(METER_CACHE_ADMIT_CAPACITY, "", x -> admitWeight)
                      .description("Admitted capacity of the objects cache in bytes.")
                      .tag("cache", CACHE_NAME)
                      .baseUnit(BaseUnits.BYTES)
                      .register(reg);
                  Gauge.builder(METER_CACHE_WEIGHT, "", x -> (double) currentWeightReported())
                      .description("Current reported weight of the objects cache in bytes.")
                      .tag("cache", CACHE_NAME)
                      .baseUnit(BaseUnits.BYTES)
                      .register(reg);
                  var rejectedWeightSummary =
                      DistributionSummary.builder(METER_CACHE_REJECTED_WEIGHT)
                          .description("Weight of of rejected cache-puts in bytes.")
                          .tag("cache", CACHE_NAME)
                          .baseUnit(BaseUnits.BYTES)
                          .register(reg);
                  return (IntConsumer) rejectedWeightSummary::record;
                })
            .orElse(x -> {});

    this.cache = cacheBuilder.build();

    var eviction = cache.policy().eviction().orElseThrow();
    weightSupplier = () -> eviction.weightedSize().orElse(0L);

    aboveCapacityLock = new ReentrantLock();
  }

  @VisibleForTesting
  long currentWeightReported() {
    return weightSupplier.getAsLong();
  }

  @VisibleForTesting
  long rejections() {
    return rejections.get();
  }

  @VisibleForTesting
  long admitWeight() {
    return admitWeight;
  }

  @Override
  public Persist wrap(@Nonnull Persist persist) {
    ObjCacheImpl cache = new ObjCacheImpl(this, persist.config());
    return new CachingPersistImpl(persist, cache);
  }

  private int weigher(CacheKey key, CacheValue value) {
    int size = key.heapSize() + value.heapSize();
    size += CAFFEINE_OBJ_OVERHEAD;
    return size;
  }

  @Override
  public Obj get(@Nonnull String repositoryId, @Nonnull ObjId id) {
    CacheKey key = new CacheKey(repositoryId, id);
    CacheValue value = cache.getIfPresent(key);
    if (value == null) {
      return null;
    }
    if (value.serialized == null) {
      return NOT_FOUND_OBJ_SENTINEL;
    }
    return value.getObj(id);
  }

  @Override
  public void put(@Nonnull String repositoryId, @Nonnull Obj obj) {
    putLocal(repositoryId, obj);
  }

  @VisibleForTesting
  void cachePut(CacheKey key, CacheValue value) {
    var w = weigher(key, value);
    var currentWeight = weightSupplier.getAsLong();
    if (currentWeight < capacityBytes) {
      cache.put(key, value);
      return;
    }

    aboveCapacityLock.lock();
    try {
      cache.cleanUp();
      currentWeight = weightSupplier.getAsLong();
      if (currentWeight + w < admitWeight) {
        cache.put(key, value);
      } else {
        rejections.incrementAndGet();
        rejectionsWeight.accept(w);
      }
    } finally {
      aboveCapacityLock.unlock();
    }
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
      cachePut(
          new CacheKey(repositoryId, obj.id()),
          new CacheValue(expiresAtNanos, serialized, obj, enableSoftReferences));
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
    cachePut(new CacheKey(repositoryId, id), new CacheValue(expiresAtNanos, null, null, false));
  }

  @Override
  public void remove(@Nonnull String repositoryId, @Nonnull ObjId id) {
    CacheKey key = new CacheKey(repositoryId, id);
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
    CacheKey key = new CacheKey(repositoryId, id);
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
    cachePut(
        new CacheKey(repositoryId, id),
        new CacheValue(
            config.clockNanos().getAsLong() + refCacheTtlNanos,
            serializeReference(r),
            r,
            enableSoftReferences));
  }

  @Override
  public void putReferenceNegative(@Nonnull String repositoryId, @Nonnull String name) {
    if (refCacheNegativeTtlNanos <= 0L) {
      return;
    }
    ObjId id = refObjId(name);
    cachePut(
        new CacheKey(repositoryId, id),
        new CacheValue(
            config.clockNanos().getAsLong() + refCacheNegativeTtlNanos, null, null, false));
  }

  @Override
  public Reference getReference(@Nonnull String repositoryId, @Nonnull String name) {
    if (refCacheTtlNanos <= 0L) {
      return null;
    }
    ObjId id = refObjId(name);
    CacheValue value = cache.getIfPresent(new CacheKey(repositoryId, id));
    if (value == null) {
      return null;
    }
    if (value.serialized == null) {
      return NON_EXISTENT_REFERENCE_SENTINEL;
    }
    return value.getReference();
  }

  /**
   * Caffeine retains the original key when replacing a value, so keys must not retain payloads or
   * expiration timestamps belonging to a previous value.
   *
   * @param id ObjId256 heap size: 40 bytes (assumed, jol)
   */
  record CacheKey(String repositoryId, ObjId id) {

    int heapSize() {
      int size = KEY_SIZE;
      size += STRING_OBJ_OVERHEAD + repositoryId.length();
      size += id.heapSize();
      return size;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof CacheKey cacheKey)) {
        return false;
      }
      return repositoryId.equals(cacheKey.repositoryId) && id.equals(cacheKey.id);
    }

    @Override
    public @Nonnull String toString() {
      return "{" + repositoryId + ", " + id + '}';
    }
  }

  static final class CacheValue {

    final long expiresAtNanosEpoch;
    // A null payload represents a negative cache entry, with its own expiration timestamp.
    final byte[] serialized;
    java.lang.ref.Reference<Object> object;

    CacheValue(
        long expiresAtNanosEpoch, byte[] serialized, Object object, boolean enableSoftReferences) {
      this.expiresAtNanosEpoch = expiresAtNanosEpoch;
      this.serialized = serialized;
      this.object = enableSoftReferences ? new SoftReference<>(object, null) : null;
    }

    int heapSize() {
      int size = VALUE_SIZE;
      if (serialized != null) {
        size += ARRAY_OVERHEAD + serialized.length;
      }
      if (object != null) {
        size += SOFT_REFERENCE_OVERHEAD;
      }
      return size;
    }

    Obj getObj(ObjId id) {
      var softRef = this.object;
      if (softRef == null) {
        return ProtoSerialization.deserializeObj(id, 0L, this.serialized, null);
      }
      Obj obj = (Obj) softRef.get();
      if (obj == null) {
        obj = ProtoSerialization.deserializeObj(id, 0L, this.serialized, null);
        // re-create the soft reference - but don't care about JMM side effects
        this.object = new SoftReference<>(obj);
      }
      return obj;
    }

    Reference getReference() {
      var softRef = this.object;
      if (softRef == null) {
        return deserializeReference(this.serialized);
      }
      Reference ref = (Reference) softRef.get();
      if (ref == null) {
        ref = deserializeReference(this.serialized);
        // re-create the soft reference - but don't care about JMM side effects
        this.object = new SoftReference<>(ref);
      }
      return ref;
    }
  }

  // Estimated layouts with compressed references and 8-byte object alignment:
  // CacheKey: 12-byte header + two references, rounded to 24 bytes.
  // CacheValue: 12-byte header + long + two references, rounded to 32 bytes.
  static final int KEY_SIZE = 24;
  static final int VALUE_SIZE = 32;
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
