/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie.versioned.persist.adapter;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.time.Clock;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import org.immutables.value.Value;

/**
 * Base database-adapter configuration type.
 *
 * <p>{@link org.projectnessie.versioned.persist.adapter.DatabaseAdapter} implementations that need
 * more configuration options must extend this interface, have the {@link Value.Immutable}
 * annotation and declare the {@code with...} methods implemented by "immutables".
 */
public interface DatabaseAdapterConfig {

  String DEFAULT_REPOSITORY_ID = "";
  int DEFAULT_PARENTS_PER_COMMIT = 20;
  int DEFAULT_KEY_LIST_DISTANCE = 20;
  int DEFAULT_MAX_ENTITY_SIZE = 250_000;
  int DEFAULT_MAX_KEY_LIST_ENTITY_SIZE = 1_000_000;
  float DEFAULT_KEY_LIST_HASH_LOAD_FACTOR = 0.65f;
  int DEFAULT_KEY_LIST_ENTITY_PREFETCH = 0;
  int DEFAULT_COMMIT_TIMEOUT = 500;
  int DEFAULT_COMMIT_RETRIES = Integer.MAX_VALUE;
  int DEFAULT_PARENTS_PER_REFLOG_ENTRY = 20;
  int DEFAULT_RETRY_INITIAL_SLEEP_MILLIS_LOWER = 5;
  int DEFAULT_RETRY_INITIAL_SLEEP_MILLIS_UPPER = 25;
  int DEFAULT_RETRY_MAX_SLEEP_MILLIS = 75;
  long DEFAULT_ASSUMED_WALL_CLOCK_DRIFT_MICROS = 5_000_000L;
  boolean DEFAULT_NAMESPACE_VALIDATION = true;

  /**
   * Committing operations by default enforce that all (parent) namespaces exist.
   *
   * <p>This configuration setting is only present for a few Nessie releases to work around
   * potential migration issues and is subject to removal.
   *
   * @since 0.52.0
   */
  @Value.Default
  default boolean validateNamespaces() {
    return DEFAULT_NAMESPACE_VALIDATION;
  }

  /**
   * A free-form string that identifies a particular Nessie storage repository.
   *
   * <p>When remote (shared) storage is used, multiple Nessie repositories may co-exist in the same
   * database (and in the same schema). In that case this configuration parameter can be used to
   * distinguish those repositories.
   *
   * <p>All {@link org.projectnessie.versioned.persist.adapter.DatabaseAdapter} implementations must
   * respect this parameter.
   */
  @Value.Default
  default String getRepositoryId() {
    return DEFAULT_REPOSITORY_ID;
  }

  /**
   * The number of parent-commit-hashes stored in {@link
   * org.projectnessie.versioned.persist.adapter.CommitLogEntry#getParents()}. Defaults to {@value
   * #DEFAULT_PARENTS_PER_COMMIT}.
   */
  @Value.Default
  default int getParentsPerCommit() {
    return DEFAULT_PARENTS_PER_COMMIT;
  }

  /**
   * Each {@code n}-th {@link org.projectnessie.versioned.persist.adapter.CommitLogEntry}, where
   * {@code n ==} value of this parameter, will contain a "full" {@link
   * org.projectnessie.versioned.persist.adapter.KeyList}. Defaults to {@value
   * #DEFAULT_KEY_LIST_DISTANCE}.
   */
  @Value.Default
  default int getKeyListDistance() {
    return DEFAULT_KEY_LIST_DISTANCE;
  }

  /**
   * Maximum size of a database object/row.
   *
   * <p>This parameter is respected for {@link org.projectnessie.versioned.persist.adapter.KeyList}
   * in a {@link org.projectnessie.versioned.persist.adapter.CommitLogEntry}.
   *
   * <p>Not all kinds of databases have hard limits on the maximum size of a database object/row.
   *
   * <p>This value must not be "on the edge" - means: it must leave enough room for a somewhat
   * large-ish list via {@link
   * org.projectnessie.versioned.persist.adapter.CommitLogEntry#getKeyListsIds()},
   * database-serialization overhead and similar.
   *
   * <p>Values {@code <=0} are illegal, defaults to {@value #DEFAULT_MAX_ENTITY_SIZE}.
   */
  @Value.Default
  default int getMaxKeyListSize() {
    return DEFAULT_MAX_ENTITY_SIZE;
  }

  /**
   * Maximum size of a database object/row.
   *
   * <p>This parameter is respected for {@link
   * org.projectnessie.versioned.persist.adapter.KeyListEntity}.
   *
   * <p>Not all kinds of databases have hard limits on the maximum size of a database object/row.
   *
   * <p>This value must not be "on the edge" - means: it must leave enough room for
   * database-serialization overhead and similar.
   *
   * <p>Values {@code <=0} are illegal, defaults to {@value #DEFAULT_MAX_KEY_LIST_ENTITY_SIZE}.
   */
  @Value.Default
  default int getMaxKeyListEntitySize() {
    return DEFAULT_MAX_KEY_LIST_ENTITY_SIZE;
  }

  /**
   * Configures key-list hash-table load-factor for the open-addressing hash map used for key-lists.
   *
   * <p>Small segments and load factors above 0.65 are bad, as those would lead to many database
   * fetches. Good configurations, that usually lead to only one additional key-list segment fetch
   * are these:
   *
   * <ul>
   *   <li>segment size of 128kB or more with a load factor of 0.65
   *   <li>segment sizes of 64kB or more with a load factor of 0.45
   * </ul>
   */
  @Value.Default
  default float getKeyListHashLoadFactor() {
    return DEFAULT_KEY_LIST_HASH_LOAD_FACTOR;
  }

  /**
   * The number of adjacent key-list-entities to fetch, defaults to {@value
   * #DEFAULT_KEY_LIST_ENTITY_PREFETCH}.
   *
   * <p>Applied to key-lists written by Nessie since 0.31.0 using the {@link
   * CommitLogEntry.KeyListVariant#OPEN_ADDRESSING} format.
   */
  @Value.Default
  default int getKeyListEntityPrefetch() {
    return DEFAULT_KEY_LIST_ENTITY_PREFETCH;
  }

  /**
   * Timeout for CAS-like operations in milliseconds. Default is {@value #DEFAULT_COMMIT_TIMEOUT}
   * milliseconds.
   *
   * @see #getCommitRetries()
   * @see #getRetryInitialSleepMillisLower()
   * @see #getRetryInitialSleepMillisUpper()
   * @see #getRetryMaxSleepMillis()
   */
  @Value.Default
  default long getCommitTimeout() {
    return DEFAULT_COMMIT_TIMEOUT;
  }

  /**
   * Maximum retries for CAS-like operations. Default is unlimited.
   *
   * @see #getCommitTimeout()
   * @see #getRetryInitialSleepMillisLower()
   * @see #getRetryInitialSleepMillisUpper()
   * @see #getRetryMaxSleepMillis()
   */
  @Value.Default
  default int getCommitRetries() {
    return DEFAULT_COMMIT_RETRIES;
  }

  /**
   * When the database adapter has to retry an operation due to a concurrent, conflicting update to
   * the database state, this parameter defines the <em>initial</em> lower bound of the sleep time.
   * Default is {@value #DEFAULT_RETRY_INITIAL_SLEEP_MILLIS_LOWER} ms.
   *
   * @see #getCommitRetries()
   * @see #getCommitTimeout()
   * @see #getRetryInitialSleepMillisUpper()
   * @see #getRetryMaxSleepMillis()
   */
  @Value.Default
  default long getRetryInitialSleepMillisLower() {
    return DEFAULT_RETRY_INITIAL_SLEEP_MILLIS_LOWER;
  }

  /**
   * When the database adapter has to retry an operation due to a concurrent, conflicting update to
   * the database state, this parameter defines the <em>initial</em> upper bound of the sleep time.
   * Default is {@value #DEFAULT_RETRY_INITIAL_SLEEP_MILLIS_UPPER} ms.
   *
   * @see #getCommitRetries()
   * @see #getCommitTimeout()
   * @see #getRetryInitialSleepMillisLower()
   * @see #getRetryMaxSleepMillis()
   */
  @Value.Default
  default long getRetryInitialSleepMillisUpper() {
    return DEFAULT_RETRY_INITIAL_SLEEP_MILLIS_UPPER;
  }

  /**
   * When the database adapter has to retry an operation due to a concurrent, conflicting update to
   * the database state, this parameter defines the <em>maximum</em> sleep time. Each retry doubles
   * the {@link #getRetryInitialSleepMillisLower() lower} and {@link
   * #getRetryInitialSleepMillisUpper() upper} bounds of the random sleep time, unless the doubled
   * upper bound would exceed the value of this configuration property. Default is {@value
   * #DEFAULT_RETRY_MAX_SLEEP_MILLIS} ms.
   *
   * @see #getCommitRetries()
   * @see #getCommitTimeout()
   * @see #getRetryInitialSleepMillisLower()
   * @see #getRetryInitialSleepMillisUpper()
   */
  @Value.Default
  default long getRetryMaxSleepMillis() {
    return DEFAULT_RETRY_MAX_SLEEP_MILLIS;
  }

  /** The {@link Clock} to use. */
  @Value.Default
  default Clock getClock() {
    return Clock.systemUTC();
  }

  /** Returns the current time in microseconds since epoch. */
  @Value.Redacted
  @Value.Auxiliary
  @Value.NonAttribute
  default long currentTimeInMicros() {
    Instant instant = getClock().instant();
    long time = instant.getEpochSecond();
    long nano = instant.getNano();
    return TimeUnit.SECONDS.toMicros(time) + NANOSECONDS.toMicros(nano);
  }

  /**
   * The number of Ancestor (reflogId, commitHash) stored in {@link RefLog#getParents()}. Defaults
   * to {@value #DEFAULT_PARENTS_PER_REFLOG_ENTRY}.
   */
  @Value.Default
  default int getParentsPerRefLogEntry() {
    return DEFAULT_PARENTS_PER_REFLOG_ENTRY;
  }

  /** The assumed wall-clock drift between multiple Nessie instances in microseconds. */
  @Value.Default
  default long getAssumedWallClockDriftMicros() {
    return DEFAULT_ASSUMED_WALL_CLOCK_DRIFT_MICROS;
  }
}
