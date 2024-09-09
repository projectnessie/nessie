/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.catalog.secrets.cache;

import static java.util.Collections.singletonList;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.cache.CaffeineStatsCounter;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import org.checkerframework.checker.index.qual.NonNegative;
import org.projectnessie.catalog.secrets.Secret;
import org.projectnessie.catalog.secrets.SecretType;
import org.projectnessie.catalog.secrets.SecretsProvider;

public class CachingSecretsBackend {

  private static final Secret CACHE_NEGATIVE_SENTINEL = Map::of;

  public static final String CACHE_NAME = "nessie-secrets";
  private static final long NOT_CACHED = 0L;

  @VisibleForTesting final Cache<CacheKeyValue, Secret> cache;
  private final long ttlNanos;
  private final LongSupplier clock;

  public CachingSecretsBackend(SecretsCacheConfig config) {
    OptionalLong ttl = config.ttlMillis();
    this.ttlNanos = ttl.isPresent() ? TimeUnit.MILLISECONDS.toNanos(ttl.getAsLong()) : 0L;
    this.clock = config.clockNanos();

    Caffeine<CacheKeyValue, Secret> cacheBuilder =
        Caffeine.newBuilder()
            .expireAfter(
                new Expiry<CacheKeyValue, Secret>() {
                  @Override
                  public long expireAfterCreate(
                      CacheKeyValue key, Secret value, long currentTimeNanos) {
                    long expire = key.expiresAtNanosEpoch;
                    if (expire == NOT_CACHED) {
                      return 0L;
                    }
                    long remaining = expire - currentTimeNanos;
                    return Math.max(0L, remaining);
                  }

                  @Override
                  public long expireAfterUpdate(
                      CacheKeyValue key,
                      Secret value,
                      long currentTimeNanos,
                      @NonNegative long currentDurationNanos) {
                    return currentDurationNanos;
                  }

                  @Override
                  public long expireAfterRead(
                      CacheKeyValue key,
                      Secret value,
                      long currentTimeNanos,
                      @NonNegative long currentDurationNanos) {
                    return currentDurationNanos;
                  }
                })
            .ticker(clock::getAsLong)
            .maximumSize(config.maxElements());
    config
        .meterRegistry()
        .ifPresent(
            meterRegistry -> {
              cacheBuilder.recordStats(() -> new CaffeineStatsCounter(meterRegistry, CACHE_NAME));
              meterRegistry.gauge(
                  "cache_max_size",
                  singletonList(Tag.of("cache", CACHE_NAME)),
                  "",
                  x -> config.maxElements());
              meterRegistry.gauge(
                  "cache_element_ttl",
                  singletonList(Tag.of("cache", CACHE_NAME)),
                  "",
                  x -> config.ttlMillis().orElse(0));
            });

    this.cache = cacheBuilder.build();
  }

  <S extends Secret> Optional<S> resolveSecret(
      String repositoryId,
      SecretsProvider backend,
      URI name,
      SecretType secretType,
      Class<S> secretJavaType) {
    long ttl = ttlNanos;
    long expires = ttl != 0L ? clock.getAsLong() + ttl : 0L;

    CacheKeyValue key = new CacheKeyValue(repositoryId, name, expires);

    Secret fromCache =
        cache.get(
            key,
            k -> {
              @SuppressWarnings("unchecked")
              Optional<Secret> loaded =
                  (Optional<Secret>) backend.getSecret(name, secretType, secretJavaType);
              return loaded.orElse(CACHE_NEGATIVE_SENTINEL);
            });
    if (fromCache == CACHE_NEGATIVE_SENTINEL) {
      return Optional.empty();
    }
    @SuppressWarnings("unchecked")
    S casted = (S) fromCache;
    return Optional.of(casted);
  }

  static final class CacheKeyValue {
    final String repositoryId;
    final String name;

    // Revisit this field before 2262-04-11T23:47:16.854Z (64-bit signed long overflow) ;) ;)
    final long expiresAtNanosEpoch;

    CacheKeyValue(String repositoryId, URI name, long expiresAtNanosEpoch) {
      this.repositoryId = repositoryId;
      this.name = name.toString();
      this.expiresAtNanosEpoch = expiresAtNanosEpoch;
    }

    int heapSize() {
      int size = OBJ_SIZE;
      size += STRING_OBJ_OVERHEAD + repositoryId.length();
      size += STRING_OBJ_OVERHEAD + name.length();
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
      return repositoryId.equals(cacheKey.repositoryId) && name.equals(cacheKey.name);
    }

    @Override
    public int hashCode() {
      return repositoryId.hashCode() * 31 + name.hashCode();
    }

    @Override
    public String toString() {
      return "{" + repositoryId + ", " + name + '}';
    }
  }

  /*
  CacheKeyValue object internals:
  OFF  SZ               TYPE DESCRIPTION                  VALUE
    0   8                    (object header: mark)        0x0000000000000001 (non-biasable; age: 0)
    8   4                    (object header: class)       0x010c4800
   12   4   java.lang.String CacheKeyValue.repositoryId   null
   16   8               long CacheKeyValue.expiresAt      0
   24   4   java.lang.String CacheKeyValue.id             null
   28   4                    (object alignment gap)
  Instance size: 32 bytes
  */
  static final int OBJ_SIZE = 32;
  // rough estimate, probably good enough
  static final int MAP_OBJ_OVERHEAD = OBJ_SIZE * 2;
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
  */
  static final int STRING_OBJ_OVERHEAD = 24 + ARRAY_OVERHEAD;
  /*
  Assume an overhead of 2 objects for each entry (java.util.concurrent.ConcurrentHashMap$Node is 32 bytes) in Caffeine.
  */
  static final int CAFFEINE_OBJ_OVERHEAD = 2 * 32;
}
