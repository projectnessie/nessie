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

  String DEFAULT_KEY_PREFIX = "";
  int DEFAULT_PARENTS_PER_COMMIT = 20;
  int DEFAULT_KEY_LIST_DISTANCE = 20;
  int DEFAULT_MAX_KEY_LIST_SIZE = 250_000;
  int DEFAULT_COMMIT_TIMEOUT = 500;
  int DEFAULT_COMMIT_RETRIES = Integer.MAX_VALUE;

  /**
   * Prefix for all primary-keys used by a {@link
   * org.projectnessie.versioned.persist.adapter.DatabaseAdapter} instance.
   */
  @Value.Default
  default String getKeyPrefix() {
    return DEFAULT_KEY_PREFIX;
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
   * <p>Values {@code <=0} are illegal, defaults to {@link #getDefaultMaxKeyListSize()}.
   */
  @Value.Default
  default int getMaxKeyListSize() {
    return getDefaultMaxKeyListSize();
  }

  /**
   * Database adapter implementations that actually do have a hard technical or highly recommended
   * limit on a maximum db-object / db-row size limitation should override this method and return a
   * "good" value.
   *
   * <p>As for {@link #getMaxKeyListSize()}, this value must not be "on the edge" - means: it must
   * leave enough room for a somewhat large-ish list via {@link
   * org.projectnessie.versioned.persist.adapter.CommitLogEntry#getKeyListsIds()},
   * database-serialization overhead * and similar.
   *
   * <p>Defaults to {@value #DEFAULT_MAX_KEY_LIST_SIZE}.
   */
  default int getDefaultMaxKeyListSize() {
    return DEFAULT_MAX_KEY_LIST_SIZE;
  }

  /**
   * Timeout for CAS-like operations in milliseconds. Default is {@value #DEFAULT_COMMIT_TIMEOUT}
   * milliseconds.
   */
  @Value.Default
  default long getCommitTimeout() {
    return DEFAULT_COMMIT_TIMEOUT;
  }

  /** Maximum retries for CAS-like operations. Default is unlimited. */
  @Value.Default
  default int getCommitRetries() {
    return DEFAULT_COMMIT_RETRIES;
  }

  /** The {@link Clock} to use. */
  @Value.Default
  default Clock getClock() {
    return Clock.systemUTC();
  }
}
