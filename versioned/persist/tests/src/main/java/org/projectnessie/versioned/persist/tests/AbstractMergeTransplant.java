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
import static org.projectnessie.versioned.persist.adapter.DatabaseAdapterConfig.DEFAULT_KEY_LIST_DISTANCE;

import com.google.protobuf.ByteString;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.ContentAndState;
import org.projectnessie.versioned.persist.adapter.ContentId;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitParams;
import org.projectnessie.versioned.persist.adapter.KeyFilterPredicate;
import org.projectnessie.versioned.persist.adapter.KeyWithBytes;
import org.projectnessie.versioned.persist.adapter.MergeParams;
import org.projectnessie.versioned.persist.adapter.TransplantParams;
import org.projectnessie.versioned.testworker.OnRefOnly;
import org.projectnessie.versioned.testworker.SimpleStoreWorker;

/** Check that merge and transplant operations work correctly. */
public abstract class AbstractMergeTransplant {

  private final DatabaseAdapter databaseAdapter;

  protected AbstractMergeTransplant(DatabaseAdapter databaseAdapter) {
    this.databaseAdapter = databaseAdapter;
  }

  @ParameterizedTest
  @CsvSource(
      value = {
        "3,true",
        "3,false",
        DEFAULT_KEY_LIST_DISTANCE + ",true",
        DEFAULT_KEY_LIST_DISTANCE + ",false",
        (DEFAULT_KEY_LIST_DISTANCE + 1) + ",true",
        (DEFAULT_KEY_LIST_DISTANCE + 1) + ",false"
      })
  void merge(int numCommits, boolean keepIndividualCommits) throws Exception {
    AtomicInteger unifier = new AtomicInteger();
    Function<List<ByteString>, ByteString> metadataUpdater =
        commitMeta ->
            ByteString.copyFromUtf8(
                commitMeta.stream().map(ByteString::toStringUtf8).collect(Collectors.joining(";"))
                    + " merged "
                    + unifier.getAndIncrement());

    mergeTransplant(
        numCommits,
        (target, expectedHead, branch, commitHashes, i) ->
            databaseAdapter.merge(
                MergeParams.builder()
                    .toBranch(target)
                    .expectedHead(expectedHead)
                    .mergeFromHash(
                        keepIndividualCommits
                            ? commitHashes[i]
                            : commitHashes[commitHashes.length - 1])
                    .updateCommitMetadata(metadataUpdater)
                    .keepIndividualCommits(keepIndividualCommits)
                    .build()),
        keepIndividualCommits);

    BranchName branch = BranchName.of("branch");
    BranchName branch2 = BranchName.of("branch2");
    databaseAdapter.create(branch2, databaseAdapter.hashOnReference(branch, Optional.empty()));
    assertThatThrownBy(
            () ->
                databaseAdapter.merge(
                    MergeParams.builder()
                        .toBranch(branch2)
                        .mergeFromHash(databaseAdapter.hashOnReference(branch, Optional.empty()))
                        .updateCommitMetadata(l -> l.get(0))
                        .keepIndividualCommits(keepIndividualCommits)
                        .build()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("No hashes to merge from '");
  }

  @ParameterizedTest
  @CsvSource(
      value = {
        "3,true",
        "3,false",
        DEFAULT_KEY_LIST_DISTANCE + ",true",
        DEFAULT_KEY_LIST_DISTANCE + ",false",
        (DEFAULT_KEY_LIST_DISTANCE + 1) + ",true",
        (DEFAULT_KEY_LIST_DISTANCE + 1) + ",false"
      })
  void transplant(int numCommits, boolean keepIndividualCommits) throws Exception {
    AtomicInteger unifier = new AtomicInteger();
    Function<List<ByteString>, ByteString> metadataUpdater =
        commitMeta ->
            ByteString.copyFromUtf8(
                commitMeta.stream().map(ByteString::toStringUtf8).collect(Collectors.joining(";"))
                    + " transplanted "
                    + unifier.getAndIncrement());

    Hash[] commits =
        mergeTransplant(
            numCommits,
            (target, expectedHead, branch, commitHashes, i) ->
                databaseAdapter.transplant(
                    TransplantParams.builder()
                        .toBranch(target)
                        .expectedHead(expectedHead)
                        .sequenceToTransplant(
                            Arrays.asList(commitHashes)
                                .subList(0, keepIndividualCommits ? i + 1 : commitHashes.length))
                        .updateCommitMetadata(metadataUpdater)
                        .keepIndividualCommits(keepIndividualCommits)
                        .build()),
            keepIndividualCommits);

    BranchName conflict = BranchName.of("conflict");

    // no conflict, when transplanting the commits from against the current HEAD of the
    // conflict-branch
    Hash noConflictHead = databaseAdapter.hashOnReference(conflict, Optional.empty());
    Hash transplanted =
        databaseAdapter.transplant(
            TransplantParams.builder()
                .toBranch(conflict)
                .expectedHead(Optional.of(noConflictHead))
                .addSequenceToTransplant(commits)
                .updateCommitMetadata(metadataUpdater)
                .keepIndividualCommits(keepIndividualCommits)
                .build());
    int offset = unifier.get();

    checkTransplantedCommits(keepIndividualCommits, commits, transplanted, offset);

    // again, no conflict (same as above, just again)
    transplanted =
        databaseAdapter.transplant(
            TransplantParams.builder()
                .toBranch(conflict)
                .addSequenceToTransplant(commits)
                .updateCommitMetadata(metadataUpdater)
                .keepIndividualCommits(keepIndividualCommits)
                .build());
    offset = unifier.get();

    checkTransplantedCommits(keepIndividualCommits, commits, transplanted, offset);

    assertThatThrownBy(
            () ->
                databaseAdapter.transplant(
                    TransplantParams.builder()
                        .toBranch(conflict)
                        .updateCommitMetadata(l -> l.get(0))
                        .keepIndividualCommits(keepIndividualCommits)
                        .build()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("No hashes to transplant given.");
  }

  private void checkTransplantedCommits(
      boolean keepIndividualCommits, Hash[] commits, Hash transplanted, int offset)
      throws ReferenceNotFoundException {
    if (keepIndividualCommits) {
      try (Stream<CommitLogEntry> log =
          databaseAdapter.commitLog(transplanted).limit(commits.length)) {
        AtomicInteger testOffset = new AtomicInteger(offset);
        assertThat(log.map(CommitLogEntry::getMetadata).map(ByteString::toStringUtf8))
            .containsExactlyElementsOf(
                IntStream.range(0, commits.length)
                    .map(i -> commits.length - i - 1)
                    .mapToObj(i -> "commit " + i + " transplanted " + testOffset.decrementAndGet())
                    .collect(Collectors.toList()));
      }
    } else {
      try (Stream<CommitLogEntry> log = databaseAdapter.commitLog(transplanted).limit(1)) {
        AtomicInteger testOffset = new AtomicInteger(offset);
        assertThat(log)
            .first()
            .extracting(CommitLogEntry::getMetadata)
            .extracting(ByteString::toStringUtf8)
            .asString()
            .contains(
                IntStream.range(0, commits.length)
                        .mapToObj(i -> "commit " + i)
                        .collect(Collectors.joining(";"))
                    + " transplanted "
                    + testOffset.decrementAndGet());
      }
    }
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

  private Hash[] mergeTransplant(
      int numCommits, MergeOrTransplant mergeOrTransplant, boolean individualCommits)
      throws Exception {
    BranchName main = BranchName.of("main");
    BranchName branch = BranchName.of("branch");
    BranchName conflict = BranchName.of("conflict");

    databaseAdapter.create(branch, databaseAdapter.hashOnReference(main, Optional.empty()));

    Map<Key, ContentAndState<ByteString>> keysAndValue = new HashMap<>();

    Hash[] commits = new Hash[numCommits];
    for (int i = 0; i < commits.length; i++) {
      ImmutableCommitParams.Builder commit =
          ImmutableCommitParams.builder()
              .toBranch(branch)
              .commitMetaSerialized(ByteString.copyFromUtf8("commit " + i));
      for (int k = 0; k < 3; k++) {
        Key key = Key.of("key", Integer.toString(k));
        ByteString onRef =
            SimpleStoreWorker.INSTANCE.toStoreOnReferenceState(
                OnRefOnly.newOnRef("value " + i + " for " + k));
        keysAndValue.put(key, ContentAndState.of(onRef));
        commit.addPuts(
            KeyWithBytes.of(
                key,
                ContentId.of("C" + k),
                SimpleStoreWorker.INSTANCE.getPayload(
                    OnRefOnly.newOnRef("value " + i + " for " + k)),
                onRef));
      }
      commits[i] = databaseAdapter.commit(commit.build());
    }

    Hash mainHead = databaseAdapter.hashOnReference(main, Optional.empty());
    Hash targetHead = null;

    for (int i = 0; i < commits.length; i++) {
      BranchName target = BranchName.of("merge-transplant-" + i);
      databaseAdapter.create(target, mainHead);

      mergeOrTransplant.apply(target, Optional.empty(), branch, commits, i);

      targetHead = databaseAdapter.hashOnReference(target, Optional.empty());

      try (Stream<CommitLogEntry> targetLog = databaseAdapter.commitLog(targetHead)) {
        assertThat(targetLog).hasSize(individualCommits ? i + 1 : 1);
      }
    }

    assertThat(
            databaseAdapter.values(targetHead, keysAndValue.keySet(), KeyFilterPredicate.ALLOW_ALL))
        .isEqualTo(keysAndValue);

    // prepare conflict for keys 0 + 1

    Hash conflictBase = databaseAdapter.create(conflict, mainHead);
    ImmutableCommitParams.Builder commit =
        ImmutableCommitParams.builder()
            .toBranch(conflict)
            .commitMetaSerialized(ByteString.copyFromUtf8("commit conflict"));
    for (int k = 0; k < 2; k++) {
      commit.addPuts(
          KeyWithBytes.of(
              Key.of("key", Integer.toString(k)),
              ContentId.of("C" + k),
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
}
