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

import java.util.List;
import java.util.Optional;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;
import org.projectnessie.versioned.BranchName;

/**
 * Configuration object for {@link TieredVersionStore}, none of these options is meant to be
 * user-configurable.
 */
@Immutable
public interface TieredVersionStoreConfig {

  /**
   * Number of attempts for {@link TieredVersionStore#commit(BranchName, Optional, Object, List)}.
   *
   * @return commit attempts
   */
  @Default
  default int getCommitAttempts() {
    return 5;
  }

  /**
   * Number of attempts for {@code UpdateState.collapseIntentionLog(UpdateState, Store,
   * InternalBranch, TieredVersionStoreConfig)}.
   *
   * @return collapse-intention-log attempts
   */
  @Default
  default int getP2CommitAttempts() {
    return 5;
  }

  /**
   * Whether {@code UpdateState.collapseIntentionLog(UpdateState, Store, InternalBranch,
   * TieredVersionStoreConfig)} waits for the intention-log-collapse to complete.
   *
   * @return default=production-setting={@code false}
   */
  @Default
  default boolean waitOnCollapse() {
    return false;
  }

  /**
   * Whether trace-scopes/spans should be emitted for commit + collapse-intention-log, should
   * reflect the {@code Store}'s configuration.
   *
   * @return whether trace-scopes/spans should be emitted
   */
  @Default
  default boolean enableTracing() {
    return false;
  }
}
