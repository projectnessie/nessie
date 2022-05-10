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

import java.time.Clock;
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
  int DEFAULT_MAX_KEY_LIST_SIZE = 250_000;
  int DEFAULT_COMMIT_TIMEOUT = 500;
  int DEFAULT_COMMIT_RETRIES = Integer.MAX_VALUE;
  int DEFAULT_PARENTS_PER_REFLOG_ENTRY = 20;
  int DEFAULT_RETRY_INITIAL_SLEEP_MILLIS_LOWER = 5;
  int DEFAULT_RETRY_INITIAL_SLEEP_MILLIS_UPPER = 25;
  int DEFAULT_RETRY_MAX_SLEEP_MILLIS = 75;

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
   * <p>This parameter is respected for {@link
   * org.projectnessie.versioned.persist.adapter.CommitLogEntry} with an {@link
   * org.projectnessie.versioned.persist.adapter.KeyList}.
   *
   * <p>Not all kinds of databases have hard limits on the maximum size of a database object/row.
   *
   * <p>This value must not be "on the edge" - means: it must leave enough room for a somewhat
   * large-ish list via {@link
   * org.projectnessie.versioned.persist.adapter.CommitLogEntry#getKeyListsIds()},
   * database-serialization overhead and similar.
   *
   * <p>Values {@code <=0} are illegal, defaults to {@value #DEFAULT_MAX_KEY_LIST_SIZE}.
   */
  @Value.Default
  default int getMaxKeyListSize() {
    return DEFAULT_MAX_KEY_LIST_SIZE;
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

  /**
   * The number of Ancestor (reflogId, commitHash) stored in {@link RefLog#getParents()}. Defaults
   * to {@value #DEFAULT_PARENTS_PER_REFLOG_ENTRY}.
   */
  @Value.Default
  default int getParentsPerRefLogEntry() {
    return DEFAULT_PARENTS_PER_REFLOG_ENTRY;
  }
}
