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

import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Map;
import org.immutables.value.Value;
import org.projectnessie.model.Conflict;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.MergeBehavior;

@Value.Immutable
public interface MergeResult<COMMIT> extends Result {

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

  /** The source ref. */
  NamedRef getSourceRef();

  /** Name of the target branch. */
  BranchName getTargetBranch();

  /** Head commit-ID of the target branch identified by the merge or transplant operation. */
  Hash getEffectiveTargetHash();

  /** The expected commit-ID of the target branch, as specified by the caller. */
  @Nullable
  Hash getExpectedHash();

  /** List of commit-IDs to be merged or transplanted. */
  @Deprecated // for removal and replaced with something else
  List<COMMIT> getSourceCommits();

  /**
   * List of commit-IDs between {@link #getExpectedHash()} and {@link #getEffectiveTargetHash()}, if
   * the expected hash was provided.
   */
  @Nullable
  @Deprecated // for removal and replaced with something else
  List<COMMIT> getTargetCommits();

  /**
   * List of new commits that where created and added to the target branch.
   *
   * <p>The returned list will always be empty if the merge or transplant operation failed. It will
   * also always be empty in dry-run mode. Furthermore, it will also be empty if the operation
   * resulted in a fast-forward merge or transplant, because no new commit is created in this case.
   * Otherwise, if commits were squashed, the returned list will contain exactly one element: the
   * squashed commit; conversely, if individual commits were preserved, the list will generally
   * contain as many commits as there were {@linkplain #getSourceCommits() source commits} to rebase
   * (unless some source commits were filtered out).
   *
   * <p>The REST API does not expose this property currently; it is used by the Nessie events
   * notification system.
   */
  List<COMMIT> getCreatedCommits();

  /** Details of all keys encountered during the merge or transplant operation. */
  Map<ContentKey, KeyDetails> getDetails();

  @Value.Immutable
  interface KeyDetails {
    @Value.Parameter(order = 1)
    MergeBehavior getMergeBehavior();

    @Deprecated // for removal, #getConflict() is a proper replacement
    @Value.Default
    @Value.Parameter(order = 2)
    default ConflictType getConflictType() {
      return ConflictType.NONE;
    }

    @Deprecated // for removal and replaced with something else
    List<Hash> getSourceCommits();

    @Deprecated // for removal and replaced with something else
    List<Hash> getTargetCommits();

    /** Optional message, usually present in case of a conflict. */
    @Nullable
    @Value.Parameter(order = 3)
    Conflict getConflict();

    static ImmutableKeyDetails.Builder builder() {
      return ImmutableKeyDetails.builder();
    }

    static KeyDetails keyDetails(MergeBehavior mergeBehavior, Conflict conflict) {
      return ImmutableKeyDetails.of(
          mergeBehavior,
          conflict != null ? ConflictType.UNRESOLVABLE : ConflictType.NONE,
          conflict);
    }
  }

  @Deprecated // for removal
  enum ConflictType {
    NONE,
    UNRESOLVABLE
  }

  static <COMMIT extends Hashable> ImmutableMergeResult.Builder<COMMIT> builder() {
    return ImmutableMergeResult.builder();
  }
}
