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
package org.projectnessie.versioned.storage.common.config;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.function.Function;
import org.immutables.value.Value;

public interface StoreConfig {

  String CONFIG_REPOSITORY_ID = "repository-id";
  String DEFAULT_REPOSITORY_ID = "";

  String CONFIG_PARENTS_PER_COMMIT = "parents-per-commit";
  int DEFAULT_PARENTS_PER_COMMIT = 20;

  String CONFIG_COMMIT_TIMEOUT_MILLIS = "commit-timeout-millis";
  int DEFAULT_COMMIT_TIMEOUT_MILLIS = 5_000;

  String CONFIG_COMMIT_RETRIES = "commit-retries";
  int DEFAULT_COMMIT_RETRIES = Integer.MAX_VALUE;

  String CONFIG_RETRY_INITIAL_SLEEP_MILLIS_LOWER = "retry-initial-sleep-millis-lower";
  int DEFAULT_RETRY_INITIAL_SLEEP_MILLIS_LOWER = 5;

  String CONFIG_RETRY_INITIAL_SLEEP_MILLIS_UPPER = "retry-initial-sleep-millis-upper";
  int DEFAULT_RETRY_INITIAL_SLEEP_MILLIS_UPPER = 25;

  String CONFIG_RETRY_MAX_SLEEP_MILLIS = "retry-max-sleep-millis";
  int DEFAULT_RETRY_MAX_SLEEP_MILLIS = 250;

  String CONFIG_MAX_INCREMENTAL_INDEX_SIZE = "max-incremental-index-size";
  int DEFAULT_MAX_INCREMENTAL_INDEX_SIZE = 50 * 1024;

  String CONFIG_MAX_SERIALIZED_INDEX_SIZE = "max-serialized-index-size";
  int DEFAULT_MAX_SERIALIZED_INDEX_SIZE = 200 * 1024;

  String CONFIG_MAX_REFERENCE_STRIPES_PER_COMMIT = "max-reference-stripes-per-commit";
  int DEFAULT_MAX_REFERENCE_STRIPES_PER_COMMIT = 50;

  String CONFIG_ASSUMED_WALL_CLOCK_DRIFT_MICROS = "assumed-wall-clock-drift-micros";
  long DEFAULT_ASSUMED_WALL_CLOCK_DRIFT_MICROS = 5_000_000L;

  String CONFIG_NAMESPACE_VALIDATION = "namespace-validation";
  boolean DEFAULT_NAMESPACE_VALIDATION = true;

  String CONFIG_PREVIOUS_HEAD_COUNT = "ref-previous-head-count";
  int DEFAULT_PREVIOUS_HEAD_COUNT = 20;

  String CONFIG_PREVIOUS_HEAD_TIME_SPAN_SECONDS = "ref-previous-head-time-span-seconds";
  long DEFAULT_PREVIOUS_HEAD_TIME_SPAN_SECONDS = 5 * 60;

  String CONFIG_REFERENCE_CACHE_TTL = "reference-cache-ttl";

  String CONFIG_REFERENCE_NEGATIVE_CACHE_TTL = "reference-cache-negative-ttl";

  boolean DEFAULT_CONFIG_CACHE_ENABLE_SOFT_REFERENCES = false;
  int DEFAULT_CACHE_CAPACITY_FRACTION_MIN_SIZE_MB = 64;
  int DEFAULT_CACHE_CAPACITY_FRACTION_ADJUST_MB = 256;
  double DEFAULT_CACHE_CAPACITY_FRACTION_OF_HEAP = 0.6d;
  double DEFAULT_CONFIG_CAPACITY_OVERSHOOT = 0.1d;

  /**
   * Whether namespace validation is enabled, changing this to false will break the Nessie
   * specification.
   *
   * <p>Committing operations by default enforce that all (parent) namespaces exist.
   *
   * <p>This configuration setting is only present for a few Nessie releases to work around
   * potential migration issues and is subject to removal.
   *
   * @since 0.52.0
   * @deprecated This setting will be removed.
   * @hidden
   */
  @Deprecated(forRemoval = true)
  @SuppressWarnings({"DeprecatedIsStillUsed"})
  @Value.Default
  default boolean validateNamespaces() {
    return DEFAULT_NAMESPACE_VALIDATION;
  }

  /**
   * Nessie repository ID (optional) that identifies a particular Nessie storage repository.
   *
   * <p>When remote (shared) database is used, multiple Nessie repositories may co-exist in the same
   * database (and in the same schema). In that case this configuration parameter can be used to
   * distinguish those repositories.
   */
  @Value.Default
  default String repositoryId() {
    return DEFAULT_REPOSITORY_ID;
  }

  /**
   * maximum retries for CAS-like operations. Used when committing to Nessie, when the HEAD (or tip)
   * of a branch changed during the commit, this value defines the maximum number of retries.
   * Default means unlimited.
   *
   * @see #commitTimeoutMillis()
   * @see #retryInitialSleepMillisLower()
   * @see #retryInitialSleepMillisUpper()
   * @see #retryMaxSleepMillis()
   */
  @Value.Default
  default int commitRetries() {
    return DEFAULT_COMMIT_RETRIES;
  }

  /**
   * Timeout for CAS-like operations in milliseconds.
   *
   * @see #commitRetries()
   * @see #retryInitialSleepMillisLower()
   * @see #retryInitialSleepMillisUpper()
   * @see #retryMaxSleepMillis()
   */
  @Value.Default
  default long commitTimeoutMillis() {
    return DEFAULT_COMMIT_TIMEOUT_MILLIS;
  }

  /**
   * When the commit logic has to retry an operation due to a concurrent, conflicting update to the
   * database state, usually a concurrent change to a branch HEAD, this parameter defines the
   * <em>initial</em> <em>lower</em> bound of the exponential backoff.
   *
   * @see #commitRetries()
   * @see #commitTimeoutMillis()
   * @see #retryInitialSleepMillisUpper()
   * @see #retryMaxSleepMillis()
   */
  @Value.Default
  default long retryInitialSleepMillisLower() {
    return DEFAULT_RETRY_INITIAL_SLEEP_MILLIS_LOWER;
  }

  /**
   * When the commit logic has to retry an operation due to a concurrent, conflicting update to the
   * database state, usually a concurrent change to a branch HEAD, this parameter defines the
   * <em>initial</em> <em>upper</em> bound of the exponential backoff.
   *
   * @see #commitRetries()
   * @see #commitTimeoutMillis()
   * @see #retryInitialSleepMillisLower()
   * @see #retryMaxSleepMillis()
   */
  @Value.Default
  default long retryInitialSleepMillisUpper() {
    return DEFAULT_RETRY_INITIAL_SLEEP_MILLIS_UPPER;
  }

  /**
   * When the commit logic has to retry an operation due to a concurrent, conflicting update to the
   * database state, usually a concurrent change to a branch HEAD, this parameter defines the
   * <em>maximum</em> sleep time. Each retry doubles the lower and upper bounds of the random sleep
   * time, unless the doubled upper bound would exceed the value of this configuration property.
   *
   * @see #commitRetries()
   * @see #commitTimeoutMillis()
   * @see #retryInitialSleepMillisLower()
   * @see #retryInitialSleepMillisUpper()
   */
  @Value.Default
  default long retryMaxSleepMillis() {
    return DEFAULT_RETRY_MAX_SLEEP_MILLIS;
  }

  /**
   * Number of parent-commit-hashes stored in each commit. This is used to allow bulk-fetches when
   * accessing the commit log.
   */
  @Value.Default
  default int parentsPerCommit() {
    return DEFAULT_PARENTS_PER_COMMIT;
  }

  /**
   * The maximum allowed serialized size of the content index structure in a <em>Nessie commit</em>,
   * called <em>incremental index</em>. This value is used to determine, when elements in an
   * incremental index, which were kept from previous commits, need to be pushed to a new or updated
   * <em>reference index</em>.
   *
   * <p>Note: this value <em>must</em> be smaller than a database's <em>hard item/row size
   * limit</em>.
   */
  @Value.Default
  default int maxIncrementalIndexSize() {
    return DEFAULT_MAX_INCREMENTAL_INDEX_SIZE;
  }

  /**
   * The maximum allowed serialized size of the content index structure in a <em>reference
   * index</em> segment. This value is used to determine, when elements in a reference index segment
   * need to be split.
   *
   * <p>Note: this value <em>must</em> be smaller than a database's <em>hard item/row size
   * limit</em>.
   */
  @Value.Default
  default int maxSerializedIndexSize() {
    return DEFAULT_MAX_SERIALIZED_INDEX_SIZE;
  }

  /**
   * Maximum number of referenced index objects stored inside commit objects.
   *
   * <p>If the external reference index for this commit consists of up to this amount of stripes,
   * the references to the stripes will be stored inside the commit object. If there are more than
   * this amount of stripes, an external <em>index segment</em> will be created instead.
   */
  @Value.Default
  default int maxReferenceStripesPerCommit() {
    return DEFAULT_MAX_REFERENCE_STRIPES_PER_COMMIT;
  }

  /** Assumed wall-clock drift between multiple Nessie instances in microseconds. */
  @Value.Default
  default long assumedWallClockDriftMicros() {
    return DEFAULT_ASSUMED_WALL_CLOCK_DRIFT_MICROS;
  }

  /** The {@link Clock} to use, do not change for production. */
  @Value.Default
  default Clock clock() {
    return Clock.systemUTC();
  }

  /**
   * Named references keep a history of up to this amount of previous HEAD pointers, and up to the
   * configured age.
   */
  @Value.Default
  default int referencePreviousHeadCount() {
    return DEFAULT_PREVIOUS_HEAD_COUNT;
  }

  /**
   * Named references keep a history of previous HEAD pointers with this age in seconds, and up to
   * the configured amount.
   */
  @Value.Default
  default long referencePreviousHeadTimeSpanSeconds() {
    return DEFAULT_PREVIOUS_HEAD_TIME_SPAN_SECONDS;
  }

  /**
   * Defines the duration how long references shall be kept in the cache. Defaults to not cache
   * references. If reference caching is enabled, it is highly recommended to also enable negative
   * reference caching.
   *
   * <p>It is safe to enable this for single node Nessie deployments.
   *
   * <p>Recommended value is currently {@code PT5M} for distributed and high values like {@code
   * PT1H} for single node Nessie deployments.
   *
   * <p><em>This feature is experimental except for single Nessie node deployments! If in doubt,
   * leave this un-configured!</em>
   */
  Optional<Duration> referenceCacheTtl();

  /**
   * Defines the duration how long sentinels for non-existing references shall be kept in the cache
   * (negative reference caching).
   *
   * <p>Defaults to {@code reference-cache-ttl}. Has no effect, if {@code reference-cache-ttl} is
   * not configured. Default is not enabled. If reference caching is enabled, it is highly
   * recommended to also enable negative reference caching.
   *
   * <p>It is safe to enable this for single node Nessie deployments.
   *
   * <p>Recommended value is currently {@code PT5M} for distributed and high values like {@code
   * PT1H} for single node Nessie deployments.
   *
   * <p><em>This feature is experimental except for single Nessie node deployments! If in doubt,
   * leave this un-configured!</em>
   */
  Optional<Duration> referenceCacheNegativeTtl();

  /**
   * Retrieves the current timestamp in microseconds since epoch, using the configured {@link
   * #clock()}.
   */
  @Value.Redacted
  @Value.Auxiliary
  @Value.NonAttribute
  default long currentTimeMicros() {
    Instant instant = clock().instant();
    long time = instant.getEpochSecond();
    long nano = instant.getNano();
    return SECONDS.toMicros(time) + NANOSECONDS.toMicros(nano);
  }

  @Value.Immutable
  interface Adjustable extends StoreConfig {
    static Adjustable empty() {
      return ImmutableAdjustable.builder().build();
    }

    default Adjustable from(StoreConfig config) {
      return ImmutableAdjustable.builder().from(this).from(config).build();
    }

    default Adjustable fromFunction(Function<String, String> configFunction) {
      Adjustable a = this;
      String v;

      v = configFunction.apply(CONFIG_REPOSITORY_ID);
      if (v != null) {
        a = a.withRepositoryId(v);
      }
      v = configFunction.apply(CONFIG_COMMIT_RETRIES);
      if (v != null) {
        a = a.withCommitRetries(Integer.parseInt(v.trim()));
      }
      v = configFunction.apply(CONFIG_COMMIT_TIMEOUT_MILLIS);
      if (v != null) {
        a = a.withCommitTimeoutMillis(Long.parseLong(v.trim()));
      }
      v = configFunction.apply(CONFIG_RETRY_INITIAL_SLEEP_MILLIS_LOWER);
      if (v != null) {
        a = a.withRetryInitialSleepMillisLower(Long.parseLong(v.trim()));
      }
      v = configFunction.apply(CONFIG_RETRY_INITIAL_SLEEP_MILLIS_UPPER);
      if (v != null) {
        a = a.withRetryInitialSleepMillisUpper(Long.parseLong(v.trim()));
      }
      v = configFunction.apply(CONFIG_RETRY_MAX_SLEEP_MILLIS);
      if (v != null) {
        a = a.withRetryMaxSleepMillis(Long.parseLong(v.trim()));
      }
      v = configFunction.apply(CONFIG_PARENTS_PER_COMMIT);
      if (v != null) {
        a = a.withParentsPerCommit(Integer.parseInt(v.trim()));
      }
      v = configFunction.apply(CONFIG_MAX_INCREMENTAL_INDEX_SIZE);
      if (v != null) {
        a = a.withMaxIncrementalIndexSize(Integer.parseInt(v.trim()));
      }
      v = configFunction.apply(CONFIG_MAX_SERIALIZED_INDEX_SIZE);
      if (v != null) {
        a = a.withMaxSerializedIndexSize(Integer.parseInt(v.trim()));
      }
      v = configFunction.apply(CONFIG_MAX_REFERENCE_STRIPES_PER_COMMIT);
      if (v != null) {
        a = a.withMaxReferenceStripesPerCommit(Integer.parseInt(v.trim()));
      }
      v = configFunction.apply(CONFIG_ASSUMED_WALL_CLOCK_DRIFT_MICROS);
      if (v != null) {
        a = a.withAssumedWallClockDriftMicros(Integer.parseInt(v.trim()));
      }
      v = configFunction.apply(CONFIG_NAMESPACE_VALIDATION);
      if (v != null) {
        a = a.withValidateNamespaces(Boolean.parseBoolean(v.trim()));
      }
      v = configFunction.apply(CONFIG_PREVIOUS_HEAD_COUNT);
      if (v != null) {
        a = a.withReferencePreviousHeadCount(Integer.parseInt(v.trim()));
      }
      v = configFunction.apply(CONFIG_PREVIOUS_HEAD_TIME_SPAN_SECONDS);
      if (v != null) {
        a = a.withReferencePreviousHeadTimeSpanSeconds(Long.parseLong(v.trim()));
      }
      v = configFunction.apply(CONFIG_REFERENCE_CACHE_TTL);
      if (v != null) {
        a = a.withReferenceCacheTtl(Duration.parse(v.trim()));
      }
      v = configFunction.apply(CONFIG_REFERENCE_NEGATIVE_CACHE_TTL);
      if (v != null) {
        a = a.withReferenceCacheNegativeTtl(Duration.parse(v.trim()));
      }
      return a;
    }

    /** See {@link StoreConfig#repositoryId()}. */
    Adjustable withRepositoryId(String repositoryId);

    /** See {@link StoreConfig#commitRetries()}. */
    Adjustable withCommitRetries(int commitRetries);

    /** See {@link StoreConfig#commitTimeoutMillis()}. */
    Adjustable withCommitTimeoutMillis(long commitTimeoutMillis);

    /** See {@link StoreConfig#retryInitialSleepMillisLower()}. */
    Adjustable withRetryInitialSleepMillisLower(long retryInitialSleepMillisLower);

    /** See {@link StoreConfig#retryInitialSleepMillisUpper()}. */
    Adjustable withRetryInitialSleepMillisUpper(long retryInitialSleepMillisUpper);

    /** See {@link StoreConfig#retryMaxSleepMillis()}. */
    Adjustable withRetryMaxSleepMillis(long retryMaxSleepMillis);

    /** See {@link StoreConfig#parentsPerCommit()}. */
    Adjustable withParentsPerCommit(int parentsPerCommit);

    /** See {@link StoreConfig#maxIncrementalIndexSize()}. */
    Adjustable withMaxIncrementalIndexSize(int maxIncrementalIndexSize);

    /** See {@link StoreConfig#maxSerializedIndexSize()}. */
    Adjustable withMaxSerializedIndexSize(int maxSerializedIndexSize);

    /** See {@link StoreConfig#maxReferenceStripesPerCommit()}. */
    Adjustable withMaxReferenceStripesPerCommit(int maxReferenceStripesPerCommit);

    /** See {@link StoreConfig#assumedWallClockDriftMicros()}. */
    Adjustable withAssumedWallClockDriftMicros(long assumedWallClockDriftMicros);

    /** See {@link StoreConfig#validateNamespaces ()} ()}. */
    Adjustable withValidateNamespaces(boolean validateNamespaces);

    /** See {@link StoreConfig#clock()}. */
    Adjustable withClock(Clock clock);

    Adjustable withReferencePreviousHeadCount(int referencePreviousHeadCount);

    Adjustable withReferencePreviousHeadTimeSpanSeconds(long referencePreviousHeadTimeSpanSeconds);

    /** See {@link StoreConfig#referenceCacheTtl()}. */
    Adjustable withReferenceCacheTtl(Duration referenceCacheTtl);

    /** See {@link StoreConfig#referenceCacheNegativeTtl()}. */
    Adjustable withReferenceCacheNegativeTtl(Duration referencecacheNegativeTtl);
  }
}
