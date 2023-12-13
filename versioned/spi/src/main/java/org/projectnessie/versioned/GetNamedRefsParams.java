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

import jakarta.annotation.Nullable;
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
   * ReferenceInfo#getAheadBehind()}. If this parameter is not {@code null}, the given reference
   * must exist.
   *
   * @return name of the base reference. Can be {@code null}, if not needed.
   */
  @Nullable
  NamedRef getBaseReference();

  /** Whether to retrieve branches, defaults to {@code true}. */
  @Value.Default
  default RetrieveOptions getBranchRetrieveOptions() {
    return RetrieveOptions.BARE;
  }

  /** Whether to retrieve tags, defaults to {@code true}. */
  @Value.Default
  default RetrieveOptions getTagRetrieveOptions() {
    return RetrieveOptions.BARE;
  }

  @Value.Immutable
  interface RetrieveOptions {

    /** Constant to not retrieve any information (omit tags or branches). */
    RetrieveOptions OMIT = RetrieveOptions.builder().isRetrieve(false).build();

    /** Constant to retrieve only the HEAD commit hash. */
    RetrieveOptions BARE = RetrieveOptions.builder().build();

    /** Constant to retrieve the HEAD commit hash plus the commit-meta of the HEAD commit. */
    RetrieveOptions COMMIT_META =
        RetrieveOptions.builder().isRetrieveCommitMetaForHead(true).build();

    /**
     * Constant to retrieve the HEAD commit hash, the commit-meta of the HEAD commit, the
     * common-ancestor to the base reference and the commits ahead and behind relative to the base
     * reference.
     */
    RetrieveOptions BASE_REFERENCE_RELATED_AND_COMMIT_META =
        RetrieveOptions.builder()
            .isComputeAheadBehind(true)
            .isComputeCommonAncestor(true)
            .isRetrieveCommitMetaForHead(true)
            .build();

    static ImmutableRetrieveOptions.Builder builder() {
      return ImmutableRetrieveOptions.builder();
    }

    /** Whether to retrieve the reference information at all, defaults to {@code true}. */
    @Value.Default
    default boolean isRetrieve() {
      return true;
    }

    /**
     * Whether to compute {@link ReferenceInfo#getAheadBehind()}, requires a non-{@code null} {@link
     * #getBaseReference()}, defaults to {@code false}.
     */
    @Value.Default
    default boolean isComputeAheadBehind() {
      return false;
    }

    /** Whether to identify the common ancestor on the default branch, defaults to {@code false}. */
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
  }
}
