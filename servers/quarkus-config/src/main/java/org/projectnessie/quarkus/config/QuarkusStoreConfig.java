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
package org.projectnessie.quarkus.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithConverter;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import org.projectnessie.versioned.storage.common.config.StoreConfig;

@ConfigMapping(prefix = QuarkusStoreConfig.NESSIE_VERSION_STORE_PERSIST)
public interface QuarkusStoreConfig extends StoreConfig {

  String NESSIE_VERSION_STORE_PERSIST = "nessie.version.store.persist";
  String CONFIG_CACHE_INVALIDATIONS_VALID_TOKENS = "cache-invalidations.valid-tokens";
  String CONFIG_CACHE_INVALIDATIONS_SERVICE_NAMES = "cache-invalidations.service-names";
  String CONFIG_CACHE_INVALIDATIONS_URI = "cache-invalidations.uri";
  String CONFIG_CACHE_INVALIDATIONS_BATCH_SIZE = "cache-invalidations.batch-size";
  String CONFIG_CACHE_INVALIDATIONS_SERVICE_NAME_LOOKUP_INTERVAL =
      "cache-invalidations.service-name-lookup-interval";
  String CONFIG_CACHE_INVALIDATIONS_REQUEST_TIMEOUT = "cache-invalidations.request-timeout";

  @WithName(CONFIG_REPOSITORY_ID)
  @WithDefault(DEFAULT_REPOSITORY_ID)
  // Use RepoIdConverter for the "key-prefix" property because it can be an empty string,
  // but the default converter will turn empty strings into `null`.
  @WithConverter(RepoIdConverter.class)
  @Override
  String repositoryId();

  @WithName(CONFIG_COMMIT_RETRIES)
  @WithDefault("" + DEFAULT_COMMIT_RETRIES)
  @Override
  int commitRetries();

  @WithName(CONFIG_COMMIT_TIMEOUT_MILLIS)
  @WithDefault("" + DEFAULT_COMMIT_TIMEOUT_MILLIS)
  @Override
  long commitTimeoutMillis();

  @WithName(CONFIG_RETRY_INITIAL_SLEEP_MILLIS_LOWER)
  @WithDefault("" + DEFAULT_RETRY_INITIAL_SLEEP_MILLIS_LOWER)
  @Override
  long retryInitialSleepMillisLower();

  @WithName(CONFIG_RETRY_INITIAL_SLEEP_MILLIS_UPPER)
  @WithDefault("" + DEFAULT_RETRY_INITIAL_SLEEP_MILLIS_UPPER)
  @Override
  long retryInitialSleepMillisUpper();

  @WithName(CONFIG_RETRY_MAX_SLEEP_MILLIS)
  @WithDefault("" + DEFAULT_RETRY_MAX_SLEEP_MILLIS)
  @Override
  long retryMaxSleepMillis();

  @WithName(CONFIG_PARENTS_PER_COMMIT)
  @WithDefault("" + DEFAULT_PARENTS_PER_COMMIT)
  @Override
  int parentsPerCommit();

  @WithName(CONFIG_MAX_SERIALIZED_INDEX_SIZE)
  @WithDefault("" + DEFAULT_MAX_SERIALIZED_INDEX_SIZE)
  @Override
  int maxSerializedIndexSize();

  @WithName(CONFIG_MAX_INCREMENTAL_INDEX_SIZE)
  @WithDefault("" + DEFAULT_MAX_INCREMENTAL_INDEX_SIZE)
  @Override
  int maxIncrementalIndexSize();

  @WithName(CONFIG_MAX_REFERENCE_STRIPES_PER_COMMIT)
  @WithDefault("" + DEFAULT_MAX_REFERENCE_STRIPES_PER_COMMIT)
  @Override
  int maxReferenceStripesPerCommit();

  @WithName(CONFIG_ASSUMED_WALL_CLOCK_DRIFT_MICROS)
  @WithDefault("" + DEFAULT_ASSUMED_WALL_CLOCK_DRIFT_MICROS)
  @Override
  long assumedWallClockDriftMicros();

  @SuppressWarnings("removal")
  @WithName(CONFIG_NAMESPACE_VALIDATION)
  @WithDefault("" + DEFAULT_NAMESPACE_VALIDATION)
  @Override
  boolean validateNamespaces();

  @WithName(CONFIG_PREVIOUS_HEAD_COUNT)
  @WithDefault("" + DEFAULT_PREVIOUS_HEAD_COUNT)
  @Override
  int referencePreviousHeadCount();

  @WithName(CONFIG_PREVIOUS_HEAD_TIME_SPAN_SECONDS)
  @WithDefault("" + DEFAULT_PREVIOUS_HEAD_TIME_SPAN_SECONDS)
  @Override
  long referencePreviousHeadTimeSpanSeconds();

  String CONFIG_CACHE_CAPACITY_MB = "cache-capacity-mb";

  /**
   * Fixed amount of heap used to cache objects, set to 0 to disable the cache entirely. Must not be
   * used with fractional cache sizing. See description for {@code cache-capacity-fraction-of-heap}
   * for the default value.
   */
  @WithName(CONFIG_CACHE_CAPACITY_MB)
  OptionalInt cacheCapacityMB();

  /**
   * Nessie keeps so called soft-references of the cached Java objects in addition to the serialized
   * representation around.
   *
   * <p>This toggle optionally enables this behavior.
   */
  @WithName(CONFIG_CACHE_ENABLE_SOFT_REFERENCES)
  @WithDefault("" + DEFAULT_CONFIG_CACHE_ENABLE_SOFT_REFERENCES)
  Optional<Boolean> cacheEnableSoftReferences();

  String CONFIG_CACHE_CAPACITY_FRACTION_MIN_SIZE_MB = "cache-capacity-fraction-min-size-mb";

  String CONFIG_CACHE_ENABLE_SOFT_REFERENCES = "cache-enable-soft-references";

  /** When using fractional cache sizing, this amount in MB is the minimum cache size. */
  @WithName(CONFIG_CACHE_CAPACITY_FRACTION_MIN_SIZE_MB)
  @WithDefault("" + DEFAULT_CACHE_CAPACITY_FRACTION_MIN_SIZE_MB)
  OptionalInt cacheCapacityFractionMinSizeMb();

  String CONFIG_CACHE_CAPACITY_FRACTION_OF_HEAP = "cache-capacity-fraction-of-heap";

  /**
   * Fraction of Java’s max heap size to use for cache objects, set to 0 to disable. Must not be
   * used with fixed cache sizing. If neither this value nor a fixed size is configured, a default
   * of {@code .6} (60%) is assumed.
   */
  @WithName(CONFIG_CACHE_CAPACITY_FRACTION_OF_HEAP)
  @WithDefault("" + DEFAULT_CACHE_CAPACITY_FRACTION_OF_HEAP)
  OptionalDouble cacheCapacityFractionOfHeap();

  String CONFIG_CACHE_CAPACITY_FRACTION_ADJUST_MB = "cache-capacity-fraction-adjust-mb";

  /**
   * When using fractional cache sizing, this amount in MB of the heap will always be "kept free"
   * when calculating the cache size.
   */
  @WithName(CONFIG_CACHE_CAPACITY_FRACTION_ADJUST_MB)
  @WithDefault("" + DEFAULT_CACHE_CAPACITY_FRACTION_ADJUST_MB)
  OptionalInt cacheCapacityFractionAdjustMB();

  String CONFIG_CACHE_CAPACITY_OVERSHOOT = "cache-capacity-overshoot";

  /**
   * Admitted cache-capacity-overshoot fraction, defaults to {@code 0.1} (10 %).
   *
   * <p>New elements are admitted to be added to the cache, if the cache's size is less than {@code
   * cache-capacity * (1 + cache-capacity-overshoot}.
   *
   * <p>Cache eviction happens asynchronously. Situations when eviction cannot keep up with the
   * amount of data added could lead to out-of-memory situations.
   *
   * <p>Value must be greater than 0, if present.
   */
  @WithName(CONFIG_CACHE_CAPACITY_OVERSHOOT)
  @WithDefault("" + DEFAULT_CONFIG_CAPACITY_OVERSHOOT)
  OptionalDouble cacheCapacityOvershoot();

  @WithName(CONFIG_REFERENCE_CACHE_TTL)
  @Override
  Optional<Duration> referenceCacheTtl();

  @WithName(CONFIG_REFERENCE_NEGATIVE_CACHE_TTL)
  @Override
  Optional<Duration> referenceCacheNegativeTtl();

  /**
   * Host names or IP addresses or kubernetes headless-service name of all Nessie server instances
   * accessing the same repository.
   *
   * <p>This value is automatically configured via the <a href="../../guides/kubernetes/">Nessie
   * Helm chart</a> or the Kubernetes operator (not released yet), you don't need any additional
   * configuration for distributed cache invalidations - it's setup and configured automatically. If
   * you have your own Helm chart or custom deployment, make sure to configure the IPs of all Nessie
   * instances here.
   *
   * <p>Names that start with an equal sign are not resolved but used "as is".
   */
  @WithName(CONFIG_CACHE_INVALIDATIONS_SERVICE_NAMES)
  Optional<List<String>> cacheInvalidationServiceNames();

  /** List of cache-invalidation tokens to authenticate incoming cache-invalidation messages. */
  @WithName(CONFIG_CACHE_INVALIDATIONS_VALID_TOKENS)
  Optional<List<String>> cacheInvalidationValidTokens();

  /**
   * URI of the cache-invalidation endpoint, only available on the Quarkus management port, defaults
   * to 9000.
   */
  @WithName(CONFIG_CACHE_INVALIDATIONS_URI)
  @WithDefault("/nessie-management/cache-coherency")
  String cacheInvalidationUri();

  /**
   * Interval of service-name lookups to resolve the {@linkplain #cacheInvalidationServiceNames()
   * service names} into IP addresses.
   */
  @WithName(CONFIG_CACHE_INVALIDATIONS_SERVICE_NAME_LOOKUP_INTERVAL)
  @WithDefault("PT10S")
  Duration cacheInvalidationServiceNameLookupInterval();

  @WithName(CONFIG_CACHE_INVALIDATIONS_BATCH_SIZE)
  @WithDefault("20")
  int cacheInvalidationBatchSize();

  @WithName(CONFIG_CACHE_INVALIDATIONS_REQUEST_TIMEOUT)
  Optional<Duration> cacheInvalidationRequestTimeout();
}
