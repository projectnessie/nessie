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
package org.projectnessie.versioned.impl;

import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;
import org.projectnessie.versioned.impl.ImmutableTieredVersionStoreConfig.Builder;

@Immutable
public interface TieredVersionStoreConfig {
  String DEFAULT_COMMIT_RETRY_COUNT = "5";
  String DEFAULT_P2_COMMIT_RETRY_COUNT = "5";

  /**
   * Whether to block on collapsing the InternalBranch commit log before returning valid L1s.
   * @return {@code true} to block on collapsing the InternalBranch commit log
   */
  @Default
  default boolean waitOnCollapse() {
    return false;
  }

  @Default
  default int commitRetryCount() {
    return Integer.parseInt(DEFAULT_COMMIT_RETRY_COUNT);
  }

  @Default
  default int p2CommitRetryCount() {
    return Integer.parseInt(DEFAULT_P2_COMMIT_RETRY_COUNT);
  }

  static Builder builder() {
    return ImmutableTieredVersionStoreConfig.builder();
  }

  static TieredVersionStoreConfig testConfig() {
    return builder().waitOnCollapse(true).build();
  }
}
