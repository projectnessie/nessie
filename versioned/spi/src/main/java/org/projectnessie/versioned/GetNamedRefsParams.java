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
package org.projectnessie.versioned;

import javax.annotation.Nullable;
import org.immutables.value.Value;

/**
 * Parameters that control the values that shall be returned via for each {@link ReferenceInfo} via
 * functionality to retrieve the named references.
 */
@Value.Immutable
public interface GetNamedRefsParams {

  GetNamedRefsParams DEFAULT = builder().build();

  static ImmutableGetNamedRefsParams.Builder builder() {
    return ImmutableGetNamedRefsParams.builder();
  }

  /**
   * Named reference to use as the base for the computation of {@link
   * ReferenceInfo#getAheadBehind()}. If this parameter is not {@code null}, the branch must exist.
   *
   * @return name of the base reference. Can be {@code null}, if not needed.
   */
  @Nullable
  NamedRef getBaseReference();

  /**
   * Whether to compute {@link ReferenceInfo#getAheadBehind()}, requires a non-{@code null} {@link
   * #getBaseReference()}, defaults to {@code false}.
   */
  @Value.Default
  default boolean isComputeAheadBehind() {
    return false;
  }

  /** Whether to identify the common ancestor on the default branch. */
  @Value.Default
  default boolean isComputeCommonAncestor() {
    return false;
  }

  /**
   * Whether to retrieve the commit-meta information of each reference's HEAD commit in {@link
   * ReferenceInfo#getHeadCommitMeta()}, defaults to {@code false}.
   */
  @Value.Default
  default boolean isRetrieveCommitMetaForHead() {
    return false;
  }

  /** Whether to retrieve branches, defaults to {@code true}. */
  @Value.Default
  default boolean isRetrieveBranches() {
    return true;
  }

  /** Whether to retrieve tags, defaults to {@code true}. */
  @Value.Default
  default boolean isRetrieveTags() {
    return true;
  }
}
