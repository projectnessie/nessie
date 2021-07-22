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

import org.immutables.value.Value;

/**
 * Base database-adapter configuration type.
 *
 * <p>See {@link org.projectnessie.versioned.persist.adapter.spi.DefaultDatabaseAdapterConfig} for
 * the default, immutable-annotated type.
 *
 * <p>{@link DatabaseAdapter} implementations that need more configuration options must extend this
 * interface, have the {@link Value.Immutable} annotation and declare the {@code with...} methods
 * implemented by "immutables".
 */
public interface DatabaseAdapterConfig {

  int DEFAULT_PARENTS_PER_COMMIT = 20;
  int DEFAULT_KEY_LIST_DISTANCE = 20;
  int DEFAULT_COMMIT_TIMEOUT = 500;
  int DEFAULT_COMMIT_RETRIES = Integer.MAX_VALUE;

  @Value.Default
  default String getDefaultBranch() {
    return "main";
  }

  DatabaseAdapterConfig withDefaultBranch(String defaultBranch);

  /** Prefix for all primary-keys used by a {@link DatabaseAdapter} instance. */
  @Value.Default
  default String getKeyPrefix() {
    return "";
  }

  DatabaseAdapterConfig withKeyPrefix(String keyPrefix);

  /**
   * The number of parent-commit-hashes stored in {@link CommitLogEntry#getParents()}. Defaults to
   * {@value #DEFAULT_PARENTS_PER_COMMIT}.
   */
  @Value.Default
  default int getParentsPerCommit() {
    return DEFAULT_PARENTS_PER_COMMIT;
  }

  DatabaseAdapterConfig withParentsPerCommit(int parentsPerCommit);

  /**
   * Each {@code n}-th {@link CommitLogEntry}, where {@code n ==} value of this parameter, will
   * contain a "full" {@link EmbeddedKeyList}. Defaults to {@value #DEFAULT_KEY_LIST_DISTANCE}.
   */
  @Value.Default
  default int getKeyListDistance() {
    return DEFAULT_KEY_LIST_DISTANCE;
  }

  DatabaseAdapterConfig withKeyListDistance(int keyListDistance);

  /**
   * Timeout for CAS-like operations in milliseconds. Default is {@value #DEFAULT_COMMIT_TIMEOUT}
   * milliseconds.
   */
  @Value.Default
  default long getCommitTimeout() {
    return DEFAULT_COMMIT_TIMEOUT;
  }

  DatabaseAdapterConfig withCommitTimeout(long commitTimeout);

  /** Maximum retries for CAS-like operations. Default is unlimited. */
  @Value.Default
  default int getCommitRetries() {
    return DEFAULT_COMMIT_RETRIES;
  }

  DatabaseAdapterConfig withCommitRetries(int commitRetries);
}
