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
import static org.projectnessie.versioned.store.DefaultStoreWorker.payloadForContent;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.projectnessie.model.Conflict;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.MergeBehavior;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ImmutableKeyDetails;
import org.projectnessie.versioned.ImmutableMergeResult;
import org.projectnessie.versioned.MergeConflictException;
import org.projectnessie.versioned.MergeResult;
import org.projectnessie.versioned.MergeResult.ConflictType;
import org.projectnessie.versioned.MergeResult.KeyDetails;
import org.projectnessie.versioned.MetadataRewriter;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.ResultType;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.ContentAndState;
import org.projectnessie.versioned.persist.adapter.ContentId;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitParams;
import org.projectnessie.versioned.persist.adapter.KeyFilterPredicate;
import org.projectnessie.versioned.persist.adapter.KeyWithBytes;
import org.projectnessie.versioned.persist.adapter.MergeParams;
import org.projectnessie.versioned.persist.adapter.MetadataRewriteParams;
import org.projectnessie.versioned.persist.adapter.TransplantParams;
import org.projectnessie.versioned.store.DefaultStoreWorker;
import org.projectnessie.versioned.testworker.OnRefOnly;

/** Check that merge and transplant operations work correctly. */
public abstract class AbstractMergeTransplant {

  private final DatabaseAdapter databaseAdapter;

  protected AbstractMergeTransplant(DatabaseAdapter databaseAdapter) {
    this.databaseAdapter = databaseAdapter;
  }

  @ParameterizedTest
  @CsvSource({
    "3,true",
    "3,false",
    DEFAULT_KEY_LIST_DISTANCE + ",true",
    DEFAULT_KEY_LIST_DISTANCE + ",false",
    (DEFAULT_KEY_LIST_DISTANCE + 1) + ",true",
    (DEFAULT_KEY_LIST_DISTANCE + 1) + ",false"
  })
  void merge(int numCommits, boolean keepIndividualCommits) throws Exception {
    AtomicInteger unifier = new AtomicInteger();
    MetadataRewriter<ByteString> metadataUpdater = createMetadataUpdater(unifier, "merged");

    BranchName sourceBranch = BranchName.of("branch");
    mergeTransplant(
        numCommits,
        (commitHashes, i) ->
            MergeParams.builder()
                .fromBranch(sourceBranch)
                .updateCommitMetadata(metadataUpdater)
                .keepIndividualCommits(keepIndividualCommits)
                .mergeFromHash(commitHashes[i]),
        params -> databaseAdapter.merge(params.build()),
        keepIndividualCommits,
        true);

    BranchName branch2 = BranchName.of("branch2");
    databaseAdapter.create(
        branch2, databaseAdapter.hashOnReference(sourceBranch, Optional.empty()));
    assertThatThrownBy(
            () ->
                databaseAdapter.merge(
                    MergeParams.builder()
                        .fromBranch(sourceBranch)
                        .toBranch(branch2)
                        .mergeFromHash(
                            databaseAdapter.hashOnReference(sourceBranch, Optional.empty()))
                        .updateCommitMetadata(metadataUpdater)
                        .keepIndividualCommits(keepIndividualCommits)
                        .build()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("No hashes to merge from '");
  }

  private static MetadataRewriter<ByteString> createMetadataUpdater(
      AtomicInteger unifier, String suffix) {
    return new MetadataRewriter<ByteString>() {
      @Override
      public ByteString rewriteSingle(ByteString metadata) {
        return ByteString.copyFromUtf8(
            metadata.toStringUtf8() + " " + suffix + " " + unifier.getAndIncrement());
      }

      @Override
      public ByteString squash(List<ByteString> metadata) {
        return ByteString.copyFromUtf8(
            metadata.stream().map(ByteString::toStringUtf8).collect(Collectors.joining(";"))
                + " "
                + suffix
                + " "
                + unifier.getAndIncrement());
      }
    };
  }

  @ParameterizedTest
  @CsvSource({
    "3,true",
    "3,false",
    DEFAULT_KEY_LIST_DISTANCE + ",true",
    DEFAULT_KEY_LIST_DISTANCE + ",false",
    (DEFAULT_KEY_LIST_DISTANCE + 1) + ",true",
    (DEFAULT_KEY_LIST_DISTANCE + 1) + ",false"
  })
  void transplant(int numCommits, boolean keepIndividualCommits) throws Exception {
    AtomicInteger unifier = new AtomicInteger();
    MetadataRewriter<ByteString> metadataUpdater = createMetadataUpdater(unifier, "transplanted");

    BranchName sourceBranch = BranchName.of("branch");
    Hash[] commits =
        mergeTransplant(
            numCommits,
            (commitHashes, i) ->
                TransplantParams.builder()
                    .fromBranch(sourceBranch)
                    .updateCommitMetadata(metadataUpdater)
                    .keepIndividualCommits(keepIndividualCommits)
                    .sequenceToTransplant(Arrays.asList(commitHashes).subList(0, i + 1)),
            params -> databaseAdapter.transplant(params.build()),
            keepIndividualCommits,
            false);

    BranchName conflict = BranchName.of("conflict");

    // no conflict, when transplanting the commits from against the current HEAD of the
    // conflict-branch
    Hash noConflictHead = databaseAdapter.hashOnReference(conflict, Optional.empty());
    Hash transplanted =
        databaseAdapter
            .transplant(
                TransplantParams.builder()
                    .fromBranch(sourceBranch)
                    .toBranch(conflict)
                    .expectedHead(Optional.of(noConflictHead))
                    .addSequenceToTransplant(commits)
                    .updateCommitMetadata(metadataUpdater)
                    .keepIndividualCommits(keepIndividualCommits)
                    .build())
            .getResultantTargetHash();
    int offset = unifier.get();

    checkTransplantedCommits(keepIndividualCommits, commits, transplanted, offset);

    // again, no conflict (same as above, just again)
    transplanted =
        databaseAdapter
            .transplant(
                TransplantParams.builder()
                    .fromBranch(sourceBranch)
                    .toBranch(conflict)
                    .addSequenceToTransplant(commits)
                    .updateCommitMetadata(metadataUpdater)
                    .keepIndividualCommits(keepIndividualCommits)
                    .build())
            .getResultantTargetHash();
    offset = unifier.get();

    checkTransplantedCommits(keepIndividualCommits, commits, transplanted, offset);

    assertThatThrownBy(
            () ->
                databaseAdapter.transplant(
                    TransplantParams.builder()
                        .fromBranch(sourceBranch)
                        .toBranch(conflict)
                        .updateCommitMetadata(metadataUpdater)
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
  interface MergeOrTransplant<
      PARAMS_BUILDER extends MetadataRewriteParams.Builder<PARAMS_BUILDER>> {
    MergeResult<CommitLogEntry> apply(PARAMS_BUILDER paramsBuilder) throws Exception;
  }

  private <PARAMS_BUILDER extends MetadataRewriteParams.Builder<PARAMS_BUILDER>>
      Hash[] mergeTransplant(
          int numCommits,
          BiFunction<Hash[], Integer, PARAMS_BUILDER> configurer,
          MergeOrTransplant<PARAMS_BUILDER> mergeOrTransplant,
          boolean individualCommits,
          boolean merge)
          throws Exception {
    BranchName main = BranchName.of("main");
    BranchName branch = BranchName.of("branch");

    databaseAdapter.create(branch, databaseAdapter.hashOnReference(main, Optional.empty()));

    Map<ContentKey, ContentAndState> keysAndValue = new HashMap<>();

    Hash[] commits = new Hash[numCommits];
    for (int i = 0; i < commits.length; i++) {
      ImmutableCommitParams.Builder commit =
          ImmutableCommitParams.builder()
              .toBranch(branch)
              .commitMetaSerialized(ByteString.copyFromUtf8("commit " + i));
      for (int k = 0; k < 3; k++) {
        ContentKey key = ContentKey.of("key-" + k);
        OnRefOnly value = OnRefOnly.newOnRef("value " + i + " for " + k);
        ByteString onRef = DefaultStoreWorker.instance().toStoreOnReferenceState(value);
        keysAndValue.put(key, ContentAndState.of((byte) payloadForContent(value), onRef));
        commit.addPuts(
            KeyWithBytes.of(key, ContentId.of("C" + k), (byte) payloadForContent(value), onRef));
      }
      commits[i] = databaseAdapter.commit(commit.build()).getCommit().getHash();
    }

    List<CommitLogEntry> commitLogEntries;
    try (Stream<CommitLogEntry> log =
        databaseAdapter.commitLog(commits[commits.length - 1]).limit(commits.length)) {
      commitLogEntries = log.collect(Collectors.toList());
    }

    Hash mainHead = databaseAdapter.hashOnReference(main, Optional.empty());
    Hash targetHead =
        mergeTransplantSuccess(
            configurer,
            mergeOrTransplant,
            merge,
            commits,
            commitLogEntries,
            mainHead,
            individualCommits);

    // Verify the content values on the target

    assertThat(
            databaseAdapter.values(targetHead, keysAndValue.keySet(), KeyFilterPredicate.ALLOW_ALL))
        .isEqualTo(keysAndValue);

    mergeTransplantConflict(
        configurer, mergeOrTransplant, merge, commits, commitLogEntries, mainHead);

    return commits;
  }

  private <PARAMS_BUILDER extends MetadataRewriteParams.Builder<PARAMS_BUILDER>>
      Hash mergeTransplantSuccess(
          BiFunction<Hash[], Integer, PARAMS_BUILDER> configurer,
          MergeOrTransplant<PARAMS_BUILDER> mergeOrTransplant,
          boolean merge,
          Hash[] commits,
          List<CommitLogEntry> commitLogEntries,
          Hash mainHead,
          boolean individualCommits)
          throws Exception {
    Hash targetHead = null;
    for (int i = 0; i < commits.length; i++) {
      BranchName target = BranchName.of("merge-transplant-" + i);
      databaseAdapter.create(target, mainHead);

      List<CommitLogEntry> expectedSourceCommits =
          commitLogEntries.subList(commits.length - 1 - i, commits.length);

      ImmutableMergeResult.Builder<CommitLogEntry> expectedMergeResult =
          successExpectedMergeResult(merge, target, mainHead, expectedSourceCommits);

      // Merge/transplant / Dry run

      MergeResult<CommitLogEntry> mergeResult =
          mergeOrTransplant.apply(
              configurer
                  .apply(commits, i)
                  .toBranch(target)
                  .expectedHead(Optional.empty())
                  .isDryRun(true));

      BranchName source = BranchName.of("branch");

      assertThat(mergeResult)
          .isEqualTo(
              expectedMergeResult
                  .resultType(merge ? ResultType.MERGE : ResultType.TRANSPLANT)
                  .sourceBranch(source)
                  .targetBranch(target)
                  .resultantTargetHash(mainHead)
                  .addedCommits(mergeResult.getAddedCommits())
                  .build());

      // Merge/transplant

      mergeResult =
          mergeOrTransplant.apply(
              configurer.apply(commits, i).toBranch(target).expectedHead(Optional.empty()));

      targetHead = databaseAdapter.hashOnReference(target, Optional.empty());

      assertThat(mergeResult)
          .isEqualTo(
              expectedMergeResult
                  .resultType(merge ? ResultType.MERGE : ResultType.TRANSPLANT)
                  .sourceBranch(source)
                  .targetBranch(target)
                  .resultantTargetHash(targetHead)
                  .wasApplied(true)
                  .addedCommits(mergeResult.getAddedCommits())
                  .build());

      if (individualCommits) {
        assertThat(mergeResult.getAddedCommits()).hasSize(expectedSourceCommits.size());
      } else {
        assertThat(mergeResult.getAddedCommits()).hasSize(1);
      }

      // Briefly check commit log

      try (Stream<CommitLogEntry> targetLog = databaseAdapter.commitLog(targetHead)) {
        assertThat(targetLog).hasSize(individualCommits ? i + 1 : 1);
      }
    }

    return targetHead;
  }

  private <PARAMS_BUILDER extends MetadataRewriteParams.Builder<PARAMS_BUILDER>>
      void mergeTransplantConflict(
          BiFunction<Hash[], Integer, PARAMS_BUILDER> configurer,
          MergeOrTransplant<PARAMS_BUILDER> mergeOrTransplant,
          boolean merge,
          Hash[] commits,
          List<CommitLogEntry> commitLogEntries,
          Hash mainHead)
          throws Exception {
    BranchName conflict = BranchName.of("conflict");

    // prepare conflict for keys 0 + 1

    Hash conflictBase = databaseAdapter.create(conflict, mainHead).getHash();
    ImmutableCommitParams.Builder commit =
        ImmutableCommitParams.builder()
            .toBranch(conflict)
            .commitMetaSerialized(ByteString.copyFromUtf8("commit conflict"));
    for (int k = 0; k < 2; k++) {
      OnRefOnly conflictValue = OnRefOnly.newOnRef("conflict value for " + k);
      commit.addPuts(
          KeyWithBytes.of(
              ContentKey.of("key-" + k),
              ContentId.of("C" + k),
              (byte) payloadForContent(conflictValue),
              DefaultStoreWorker.instance().toStoreOnReferenceState(conflictValue)));
    }
    Hash conflictHead = databaseAdapter.commit(commit.build()).getCommit().getHash();

    List<CommitLogEntry> expectedSourceCommits =
        commitLogEntries.subList(commits.length - 1 - 2, commits.length);

    MergeResult<CommitLogEntry> expectedMergeResult =
        conflictExpectedMergeResult(
            merge, conflict, conflictBase, conflictHead, expectedSourceCommits);

    // Merge/transplant w/ conflict / dry run

    assertThatThrownBy(
            () ->
                mergeOrTransplant.apply(
                    configurer
                        .apply(commits, 2)
                        .toBranch(conflict)
                        .expectedHead(Optional.of(conflictBase))
                        .isDryRun(true)))
        .isInstanceOf(MergeConflictException.class)
        .hasMessage("The following keys have been changed in conflict: 'key-0', 'key-1'")
        .asInstanceOf(InstanceOfAssertFactories.throwable(MergeConflictException.class))
        .extracting(MergeConflictException::getMergeResult)
        .isEqualTo(expectedMergeResult);

    // Merge/transplant w/ conflict

    assertThatThrownBy(
            () ->
                mergeOrTransplant.apply(
                    configurer
                        .apply(commits, 2)
                        .toBranch(conflict)
                        .expectedHead(Optional.of(conflictBase))))
        .isInstanceOf(MergeConflictException.class)
        .hasMessage("The following keys have been changed in conflict: 'key-0', 'key-1'")
        .asInstanceOf(InstanceOfAssertFactories.throwable(MergeConflictException.class))
        .extracting(MergeConflictException::getMergeResult)
        .isEqualTo(expectedMergeResult);
  }

  @SuppressWarnings("deprecation")
  private MergeResult<CommitLogEntry> conflictExpectedMergeResult(
      boolean merge,
      BranchName conflict,
      Hash conflictBase,
      Hash conflictHead,
      List<CommitLogEntry> expectedSourceCommits)
      throws ReferenceNotFoundException {

    List<CommitLogEntry> conflictLogEntries;
    try (Stream<CommitLogEntry> log = databaseAdapter.commitLog(conflictHead).limit(1)) {
      conflictLogEntries = log.collect(Collectors.toList());
    }

    ImmutableMergeResult.Builder<CommitLogEntry> expectedMergeResult =
        MergeResult.<CommitLogEntry>builder()
            .resultType(merge ? ResultType.MERGE : ResultType.TRANSPLANT)
            .resultantTargetHash(conflictHead)
            .sourceBranch(BranchName.of("branch"))
            .targetBranch(conflict)
            .effectiveTargetHash(conflictHead)
            .expectedHash(conflictBase)
            .commonAncestor(merge ? conflictBase : null)
            .addAllSourceCommits(expectedSourceCommits)
            .addAllTargetCommits(conflictLogEntries);

    for (int k = 0; k < 3; k++) {
      ContentKey key = ContentKey.of("key-" + k);

      ImmutableKeyDetails.Builder details =
          KeyDetails.builder()
              .mergeBehavior(MergeBehavior.NORMAL)
              .addAllSourceCommits(
                  expectedSourceCommits.stream()
                      .map(CommitLogEntry::getHash)
                      .collect(Collectors.toList()));

      if (k < 2) {
        details
            .conflict(Conflict.conflict(Conflict.ConflictType.UNKNOWN, key, "UNRESOLVABLE"))
            .conflictType(ConflictType.UNRESOLVABLE)
            .addTargetCommits(conflictHead);
      }

      expectedMergeResult.putDetails(key, details.build());
    }

    return expectedMergeResult.build();
  }

  @SuppressWarnings("deprecation")
  private static ImmutableMergeResult.Builder<CommitLogEntry> successExpectedMergeResult(
      boolean merge,
      BranchName targetBranch,
      Hash mainHead,
      List<CommitLogEntry> expectedSourceCommits) {
    ImmutableMergeResult.Builder<CommitLogEntry> expectedMergeResult =
        MergeResult.<CommitLogEntry>builder()
            .wasSuccessful(true)
            .targetBranch(targetBranch)
            .effectiveTargetHash(mainHead)
            .expectedHash(null)
            .commonAncestor(merge ? mainHead : null)
            .addAllSourceCommits(expectedSourceCommits);

    for (int k = 0; k < 3; k++) {
      ContentKey key = ContentKey.of("key-" + k);

      ImmutableKeyDetails.Builder details =
          KeyDetails.builder()
              .conflictType(ConflictType.NONE)
              .mergeBehavior(MergeBehavior.NORMAL)
              .addAllSourceCommits(
                  expectedSourceCommits.stream()
                      .map(CommitLogEntry::getHash)
                      .collect(Collectors.toList()));

      expectedMergeResult.putDetails(key, details.build());
    }
    return expectedMergeResult;
  }
}
