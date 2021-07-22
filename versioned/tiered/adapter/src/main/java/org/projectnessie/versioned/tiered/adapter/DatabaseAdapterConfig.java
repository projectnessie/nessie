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
package org.projectnessie.versioned.tiered.adapter;

import org.immutables.value.Value;

/**
 * Base database-adapter configuration type.
 *
 * <p>See {@link org.projectnessie.versioned.tiered.adapter.spi.DefaultDatabaseAdapterConfig} for
 * the default, immutable-annotated type.
 */
public interface DatabaseAdapterConfig {
  @Value.Default
  default String getDefaultBranch() {
    return "main";
  }

  DatabaseAdapterConfig withDefaultBranch(String defaultBranch);

  @Value.Default
  default String getKeyPrefix() {
    return "";
  }

  DatabaseAdapterConfig withKeyPrefix(String keyPrefix);

  @Value.Default
  default int getParentsPerCommit() {
    return 20;
  }

  DatabaseAdapterConfig withParentsPerCommit(int parentsPerCommit);

  @Value.Default
  default int getKeyListDistance() {
    return 20;
  }

  DatabaseAdapterConfig withKeyListDistance(int keyListDistance);

  /** Timeout for CAS-like operations in milliseconds, default is 500 milliseconds. */
  @Value.Default
  default long getCommitTimeout() {
    return 500;
  }

  DatabaseAdapterConfig withCommitTimeout(long commitTimeout);

  /** Maximum retries for CAS-like operations, default is unlimited. */
  @Value.Default
  default int getCommitRetries() {
    return Integer.MAX_VALUE;
  }

  DatabaseAdapterConfig withCommitRetries(int commitRetries);
}
