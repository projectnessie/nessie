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
package org.projectnessie.versioned;

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.immutables.value.Value;

@Value.Immutable
public interface MergeResult<COMMIT> {

  /** Indicates whether the merge or transplant operation has been applied. */
  @Value.Default
  default boolean wasApplied() {
    return false;
  }

  /** Indicates whether the merge or transplant operation was successful without any conflicts. */
  @Value.Default
  default boolean wasSuccessful() {
    return false;
  }

  /** Commit-ID of the target branch after the merge/transplant operation. */
  @Nullable
  Hash getResultantTargetHash();

  /** Commit-ID of the identified common ancestor, only returned for a merge operation. */
  @Nullable
  Hash getCommonAncestor();

  /** Name of the target branch. */
  BranchName getTargetBranch();

  /** Head commit-ID of the target branch identified by the merge or transplant operation. */
  Hash getEffectiveTargetHash();

  /** The expected commit-ID of the target branch, as specified by the caller. */
  @Nullable
  Hash getExpectedHash();

  /** List of commit-IDs to be merged or transplanted. */
  List<COMMIT> getSourceCommits();

  /**
   * List of commit-IDs between {@link #getExpectedHash()} and {@link #getEffectiveTargetHash()}, if
   * the expected hash was provided.
   */
  @Nullable
  List<COMMIT> getTargetCommits();

  /** Details of all keys encountered during the merge or transplant operation. */
  Map<Key, KeyDetails> getDetails();

  @Value.Immutable
  interface KeyDetails {
    MergeType getMergeType();

    @Value.Default
    default ConflictType getConflictType() {
      return ConflictType.NONE;
    }

    List<Hash> getSourceCommits();

    List<Hash> getTargetCommits();

    static ImmutableKeyDetails.Builder builder() {
      return ImmutableKeyDetails.builder();
    }

    static KeyDetails keyDetails(MergeType mergeType, ConflictType conflictType) {
      return KeyDetails.builder().mergeType(mergeType).conflictType(conflictType).build();
    }
  }

  enum ConflictType {
    NONE,
    UNRESOLVABLE
  }

  static <COMMIT> ImmutableMergeResult.Builder<COMMIT> builder() {
    return ImmutableMergeResult.builder();
  }
}
