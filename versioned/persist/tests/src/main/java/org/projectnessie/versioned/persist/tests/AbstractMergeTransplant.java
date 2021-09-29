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
package org.projectnessie.versioned.persist.tests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.ByteString;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.ContentsId;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitAttempt;
import org.projectnessie.versioned.persist.adapter.KeyWithBytes;

/** Check that merge and transplant operations work correctly. */
public abstract class AbstractMergeTransplant {
  // Puts the metadata through a transformation, that appends a suffix
  private final Function<ByteString, ByteString> transformMeta =
      inBytes -> ByteString.copyFromUtf8(inBytes.toStringUtf8() + "_suffix");
  private final Predicate<String> isTransformed = meta -> meta.endsWith("_suffix");

  private final DatabaseAdapter databaseAdapter;

  protected AbstractMergeTransplant(DatabaseAdapter databaseAdapter) {
    this.databaseAdapter = databaseAdapter;
  }

  @Test
  void merge() throws Exception {
    mergeTransplant(
        (target, expectedHead, branch, commitHashes, i) ->
            databaseAdapter.merge(commitHashes[i], target, expectedHead, Function.identity()));

    BranchName branch = BranchName.of("branch");
    BranchName branch2 = BranchName.of("branch2");
    databaseAdapter.create(branch2, databaseAdapter.toHash(branch));
    assertThatThrownBy(
            () ->
                databaseAdapter.merge(
                    databaseAdapter.toHash(branch), branch2, Optional.empty(), Function.identity()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("No hashes to merge from '");
  }

  @Test
  void transplant() throws Exception {
    Hash[] commits =
        mergeTransplant(
            (target, expectedHead, branch, commitHashes, i) ->
                databaseAdapter.transplant(
                    target,
                    expectedHead,
                    Arrays.asList(commitHashes).subList(0, i + 1),
                    Function.identity()));

    BranchName conflict = BranchName.of("conflict");

    // no conflict, when transplanting the commits from against the current HEAD of the
    // conflict-branch
    Hash noConflictHead = databaseAdapter.toHash(conflict);
    databaseAdapter.transplant(
        conflict, Optional.of(noConflictHead), Arrays.asList(commits), Function.identity());

    // again, no conflict (same as above, just again)
    databaseAdapter.transplant(
        conflict, Optional.empty(), Arrays.asList(commits), Function.identity());

    assertThatThrownBy(
            () ->
                databaseAdapter.transplant(
                    conflict, Optional.empty(), Collections.emptyList(), Function.identity()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("No hashes to transplant given.");
  }

  @Test
  void testPropResetOnMerge() throws Exception {
    BranchName main = BranchName.of("main");
    BranchName branch = BranchName.of("branch");
    databaseAdapter.create(branch, databaseAdapter.toHash(main));
    /*
     * Verify that merge operation is enacting on the transformation logic. Merge first few commits
     * with a transformation logic, and merge next few commits without transformation logic.
     * Commits are done on the "branch" and merged into "main"
     */

    // With metadata properties reset
    List<Hash> branchCommitsWithPropReset = IntStream.range(0, 3)
        .mapToObj(i -> commit(branch, i)).collect(Collectors.toList());
    Hash lastBranchCommit = branchCommitsWithPropReset
        .get(branchCommitsWithPropReset.size() - 1);
    databaseAdapter.merge(lastBranchCommit, main, Optional.empty(), transformMeta);
    Assertions.assertThat(databaseAdapter.commitLog(databaseAdapter.toHash(main))
        .map(c -> c.getMetadata().toStringUtf8()).allMatch(isTransformed))
        .isTrue();

    // Without metadata properties reset
    List<Hash> branchCommitsWithoutPropReset = IntStream.range(3, 6)
        .mapToObj(i -> commit(branch, i)).collect(Collectors.toList());
    lastBranchCommit = branchCommitsWithoutPropReset
        .get(branchCommitsWithoutPropReset.size() - 1);
    Optional<Hash> expectedHashMain = Optional.of(databaseAdapter.toHash(main));
    databaseAdapter.merge(lastBranchCommit, main, expectedHashMain, Function.identity());
    List<String> allCommitMeta = databaseAdapter.commitLog(databaseAdapter.toHash(main))
        .map(c -> c.getMetadata().toStringUtf8()).collect(Collectors.toList());
    Assertions
        .assertThat(IntStream.range(3, allCommitMeta.size()).mapToObj(i -> allCommitMeta.get(i))
            .allMatch(isTransformed)).isTrue();

    // Validate that the transformed commits are untouched
    Assertions
        .assertThat(
            IntStream.range(0, 3).mapToObj(i -> allCommitMeta.get(i)).noneMatch(isTransformed))
        .isTrue();
  }

  @Test
  void testPropResetOnTransplant() throws Exception {
    BranchName main = BranchName.of("main");
    BranchName branch = BranchName.of("branch");
    databaseAdapter.create(branch, databaseAdapter.toHash(main));

    // With metadata properties reset
    List<Hash> branchCommitsWithPropReset = IntStream.range(0, 3)
        .mapToObj(i -> commit(branch, i)).collect(Collectors.toList());
    Collections.reverse(branchCommitsWithPropReset);
    databaseAdapter
        .transplant(main, Optional.empty(), branchCommitsWithPropReset, transformMeta);
    Assertions.assertThat(databaseAdapter.commitLog(databaseAdapter.toHash(main))
        .map(c -> c.getMetadata().toStringUtf8()).allMatch(isTransformed))
        .isTrue();

    // Without metadata properties reset
    List<Hash> branchCommitsWithoutPropReset = Collections.singletonList(commit(branch, 3));
    Optional<Hash> expectedHashMain = Optional.of(databaseAdapter.toHash(main));
    databaseAdapter
        .transplant(main, expectedHashMain, branchCommitsWithoutPropReset, Function.identity());
    List<String> allCommitMeta = databaseAdapter.commitLog(databaseAdapter.toHash(main))
        .map(c -> c.getMetadata().toStringUtf8()).collect(Collectors.toList());
    // Expecting 2 commits, top one without reset - commit 3, next one with reset - commit 0_suffix
    Assertions.assertThat(isTransformed.test(allCommitMeta.get(0))).isFalse();
    Assertions.assertThat(isTransformed.test(allCommitMeta.get(1))).isTrue();
  }

  @FunctionalInterface
  interface MergeOrTransplant {
    void apply(
        BranchName target,
        Optional<Hash> expectedHead,
        BranchName branch,
        Hash[] commitHashes,
        int i)
        throws Exception;
  }

  private Hash[] mergeTransplant(MergeOrTransplant mergeOrTransplant) throws Exception {
    BranchName main = BranchName.of("main");
    BranchName branch = BranchName.of("branch");
    BranchName conflict = BranchName.of("conflict");

    databaseAdapter.create(branch, databaseAdapter.toHash(main));

    Hash[] commits = new Hash[3];
    IntStream.range(0, commits.length).forEach(i -> commits[i] = commit(branch, i));

    for (int i = 0; i < commits.length; i++) {
      BranchName target = BranchName.of("transplant-" + i);
      databaseAdapter.create(target, databaseAdapter.toHash(main));

      mergeOrTransplant.apply(target, Optional.empty(), branch, commits, i);

      try (Stream<CommitLogEntry> targetLog =
          databaseAdapter.commitLog(databaseAdapter.toHash(target))) {
        assertThat(targetLog).hasSize(i + 1);
      }
    }

    // prepare conflict for keys 0 + 1

    Hash conflictBase = databaseAdapter.create(conflict, databaseAdapter.toHash(main));
    ImmutableCommitAttempt.Builder commit =
        ImmutableCommitAttempt.builder()
            .commitToBranch(conflict)
            .commitMetaSerialized(ByteString.copyFromUtf8("commit conflict"));
    for (int k = 0; k < 2; k++) {
      commit.addPuts(
          KeyWithBytes.of(
              Key.of("key", Integer.toString(k)),
              ContentsId.of("C" + k),
              (byte) 0,
              ByteString.copyFromUtf8("conflict value for " + k)));
    }
    databaseAdapter.commit(commit.build());

    assertThatThrownBy(
            () -> mergeOrTransplant.apply(conflict, Optional.of(conflictBase), branch, commits, 2))
        .isInstanceOf(ReferenceConflictException.class)
        .hasMessage("The following keys have been changed in conflict: 'key.0', 'key.1'");

    return commits;
  }

  private Hash commit(BranchName branch, int commitSeq) {
    try {
      ImmutableCommitAttempt.Builder commit =
          ImmutableCommitAttempt.builder()
              .commitToBranch(branch)
              .commitMetaSerialized(ByteString.copyFromUtf8("commit " + commitSeq));
      for (int k = 0; k < 3; k++) {
        commit.addPuts(
            KeyWithBytes.of(
                Key.of("key", Integer.toString(k)),
                ContentsId.of("C" + k),
                (byte) 0,
                ByteString.copyFromUtf8("value " + commitSeq + " for " + k)));
      }
      return databaseAdapter.commit(commit.build());
    } catch (ReferenceConflictException | ReferenceNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
}
