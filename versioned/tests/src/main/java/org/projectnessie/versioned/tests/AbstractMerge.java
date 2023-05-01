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
package org.projectnessie.versioned.tests;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.assertj.core.api.InstanceOfAssertFactories.list;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.projectnessie.versioned.testworker.OnRefOnly.newOnRef;
import static org.projectnessie.versioned.testworker.OnRefOnly.onRef;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.error.ReferenceConflicts;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Conflict;
import org.projectnessie.model.Conflict.ConflictType;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.MergeBehavior;
import org.projectnessie.model.MergeKeyBehavior;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Commit;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.MergeConflictException;
import org.projectnessie.versioned.MergeResult;
import org.projectnessie.versioned.MergeResult.KeyDetails;
import org.projectnessie.versioned.MetadataRewriter;
import org.projectnessie.versioned.Put;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.VersionStoreException;
import org.projectnessie.versioned.paging.PaginationIterator;
import org.projectnessie.versioned.testworker.OnRefOnly;

@ExtendWith(SoftAssertionsExtension.class)
public abstract class AbstractMerge extends AbstractNestedVersionStore {

  public static final BranchName MAIN_BRANCH = BranchName.of("foo");
  @InjectSoftAssertions protected SoftAssertions soft;

  private static final OnRefOnly V_1_1 = newOnRef("v1_1");
  private static final OnRefOnly V_1_2 = newOnRef("v1_2");
  private static final OnRefOnly V_1_4 = newOnRef("v1_4");
  private static final OnRefOnly V_2_1 = newOnRef("v2_1");
  private static final OnRefOnly V_2_2 = newOnRef("v2_2");
  private static final OnRefOnly V_3_1 = newOnRef("v3_1");
  private static final OnRefOnly V_4_1 = newOnRef("v4_1");
  private static final OnRefOnly V_5_1 = newOnRef("v5_1");
  private static final OnRefOnly VALUE_1 = newOnRef("value1");
  private static final OnRefOnly VALUE_2 = newOnRef("value2");
  private static final OnRefOnly VALUE_3 = newOnRef("value3");
  private static final OnRefOnly VALUE_4 = newOnRef("value4");
  private static final OnRefOnly VALUE_5 = newOnRef("value5");
  private static final OnRefOnly VALUE_6 = newOnRef("value6");

  protected AbstractMerge(VersionStore store) {
    super(store);
  }

  private Hash initialHash;
  private Hash firstCommit;
  private Hash thirdCommit;
  private List<Commit> commits;

  @BeforeEach
  protected void setupCommits() throws VersionStoreException {
    store().create(MAIN_BRANCH, Optional.empty());

    // The default common ancestor for all merge-tests.
    // The spec for 'VersionStore.merge' mentions "(...) until we arrive at a common ancestor",
    // but old implementations allowed a merge even if the "merge-from" and "merge-to" have no
    // common ancestor and did merge "everything" from the "merge-from" into "merge-to".
    // Note: "beginning-of-time" (aka creating a branch without specifying a "create-from")
    // creates a new commit-tree that is decoupled from other commit-trees.
    initialHash = commit("Default common ancestor").toBranch(MAIN_BRANCH);

    firstCommit =
        commit("First Commit")
            .put("t1", V_1_1)
            .put("t2", V_2_1)
            .put("t3", V_3_1)
            .toBranch(MAIN_BRANCH);

    Content t1 = store().getValue(MAIN_BRANCH, ContentKey.of("t1"));

    commit("Second Commit")
        .put("t1", V_1_2.withId(t1))
        .delete("t2")
        .delete("t3")
        .put("t4", V_4_1)
        .toBranch(MAIN_BRANCH);

    thirdCommit = commit("Third Commit").put("t2", V_2_2).unchanged("t4").toBranch(MAIN_BRANCH);

    commits = commitsList(MAIN_BRANCH, false).subList(0, 3);
  }

  private MetadataRewriter<CommitMeta> createMetadataRewriter(String suffix) {
    return new MetadataRewriter<CommitMeta>() {
      @Override
      public CommitMeta rewriteSingle(CommitMeta metadata) {
        return CommitMeta.fromMessage(metadata.getMessage() + suffix);
      }

      @Override
      public CommitMeta squash(List<CommitMeta> metadata) {
        return CommitMeta.fromMessage(
            metadata.stream()
                .map(cm -> cm.getMessage() + suffix)
                .collect(Collectors.joining("\n-----------------------------------\n")));
      }
    };
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  protected void mergeKeyBehaviorValidation(boolean dryRun) throws Exception {
    assumeThat(store().getClass().getName()).endsWith("VersionStoreImpl");

    MetadataRewriter<CommitMeta> metadataRewriter = createMetadataRewriter("");
    BranchName targetBranch = BranchName.of("mergeKeyBehaviorValidation");
    store().create(targetBranch, Optional.of(firstCommit));

    ContentKey keyNotUsed = ContentKey.of("not", "used");
    ContentKey keyUnused = ContentKey.of("un", "used");
    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                store()
                    .merge(
                        thirdCommit,
                        targetBranch,
                        Optional.empty(),
                        metadataRewriter,
                        false,
                        ImmutableMap.of(
                            keyNotUsed, MergeKeyBehavior.of(keyNotUsed, MergeBehavior.DROP),
                            keyUnused, MergeKeyBehavior.of(keyUnused, MergeBehavior.DROP)),
                        MergeBehavior.NORMAL,
                        dryRun,
                        false))
        .withMessage(
            "Not all merge key behaviors specified in the request have been used. The following keys were not used: [not.used, un.used]");

    ContentKey keyT3 = ContentKey.of("t3");
    for (MergeBehavior mergeBehavior :
        new MergeBehavior[] {MergeBehavior.DROP, MergeBehavior.FORCE}) {
      soft.assertThatIllegalArgumentException()
          .isThrownBy(
              () ->
                  store()
                      .merge(
                          thirdCommit,
                          targetBranch,
                          Optional.empty(),
                          metadataRewriter,
                          false,
                          ImmutableMap.of(
                              keyT3, MergeKeyBehavior.of(keyT3, mergeBehavior, V_3_1, V_1_1)),
                          MergeBehavior.NORMAL,
                          dryRun,
                          false))
          .withMessage(
              "MergeKeyBehavior.resolvedContent must be null for MergeBehavior.%s for t3",
              mergeBehavior);
    }

    Content c11 = store().getValue(firstCommit, ContentKey.of("t1"));

    for (MergeBehavior mergeBehavior : MergeBehavior.values()) {
      StorageAssertions checkpoint = storageCheckpoint();
      soft.assertThatThrownBy(
              () ->
                  store()
                      .merge(
                          thirdCommit,
                          targetBranch,
                          Optional.empty(),
                          metadataRewriter,
                          false,
                          ImmutableMap.of(
                              keyT3, MergeKeyBehavior.of(keyT3, mergeBehavior, c11, null)),
                          MergeBehavior.NORMAL,
                          dryRun,
                          false))
          .describedAs("MergeBehavior.%s", mergeBehavior)
          .hasMessage("The following keys have been changed in conflict: 't3'")
          .asInstanceOf(type(MergeConflictException.class))
          .extracting(MergeConflictException::getMergeResult)
          .extracting(
              MergeResult::wasApplied, MergeResult::wasSuccessful, r -> r.getDetails().get(keyT3))
          .containsExactly(
              false,
              false,
              KeyDetails.keyDetails(
                  mergeBehavior,
                  MergeResult.ConflictType.UNRESOLVABLE,
                  Conflict.conflict(
                      ConflictType.VALUE_DIFFERS,
                      keyT3,
                      "values of existing and expected content for key 't3' are different")));
      checkpoint.assertNoWrites();
      soft.assertAll();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  protected void mergeResolveConflict(boolean individualCommits) throws VersionStoreException {
    MetadataRewriter<CommitMeta> metadataRewriter = createMetadataRewriter("");
    BranchName sourceBranch = BranchName.of("mergeResolveConflict");
    store().create(sourceBranch, Optional.of(thirdCommit));

    ContentKey key2 = ContentKey.of("t2");
    Content contentT2 = store().getValue(MAIN_BRANCH, key2);

    Hash targetHead =
        commit("on-target-commit")
            .put("t2", onRef("v2_2-target", contentT2.getId()))
            .toBranch(MAIN_BRANCH);
    Hash sourceHead =
        commit("on-source-commit")
            .put("t2", onRef("v2_2-source", contentT2.getId()))
            .toBranch(sourceBranch);
    contentT2 = store().getValue(MAIN_BRANCH, key2);

    soft.assertThatThrownBy(
            () ->
                store()
                    .merge(
                        sourceHead,
                        MAIN_BRANCH,
                        Optional.empty(),
                        metadataRewriter,
                        individualCommits,
                        emptyMap(),
                        MergeBehavior.NORMAL,
                        false,
                        false))
        .isInstanceOf(MergeConflictException.class);

    Content resolvedContent = onRef("resolved", contentT2.getId());
    Content wrongExpectedContent = onRef("wrong", contentT2.getId());

    if (!store().getClass().getName().endsWith("VersionStoreImpl")) {
      soft.assertThatIllegalArgumentException()
          .isThrownBy(
              () ->
                  store()
                      .merge(
                          sourceHead,
                          MAIN_BRANCH,
                          Optional.empty(),
                          metadataRewriter,
                          individualCommits,
                          singletonMap(
                              key2,
                              MergeKeyBehavior.of(
                                  key2,
                                  MergeBehavior.NORMAL,
                                  wrongExpectedContent,
                                  resolvedContent)),
                          MergeBehavior.NORMAL,
                          false,
                          false))
          .withMessage(
              "MergeKeyBehavior.resolvedContent and MergeKeyBehavior.expectedTargetContent are not supported for this storage model");
      return;
    }

    if (individualCommits) {
      soft.assertThatIllegalArgumentException()
          .isThrownBy(
              () ->
                  store()
                      .merge(
                          sourceHead,
                          MAIN_BRANCH,
                          Optional.empty(),
                          metadataRewriter,
                          individualCommits,
                          singletonMap(
                              key2,
                              MergeKeyBehavior.of(
                                  key2,
                                  MergeBehavior.NORMAL,
                                  wrongExpectedContent,
                                  resolvedContent)),
                          MergeBehavior.NORMAL,
                          false,
                          false))
          .withMessage(
              "MergeKeyBehavior.expectedTargetContent and MergeKeyBehavior.resolvedContent are only supported for squashing merge/transplant operations.");
      return;
    }

    soft.assertThatThrownBy(
            () ->
                store()
                    .merge(
                        sourceHead,
                        MAIN_BRANCH,
                        Optional.empty(),
                        metadataRewriter,
                        individualCommits,
                        singletonMap(
                            key2,
                            MergeKeyBehavior.of(
                                key2, MergeBehavior.NORMAL, wrongExpectedContent, resolvedContent)),
                        MergeBehavior.NORMAL,
                        false,
                        false))
        .isInstanceOf(ReferenceConflictException.class)
        .asInstanceOf(type(ReferenceConflictException.class))
        .extracting(ReferenceConflictException::getReferenceConflicts)
        .extracting(ReferenceConflicts::conflicts, list(Conflict.class))
        .containsExactly(
            Conflict.conflict(
                ConflictType.VALUE_DIFFERS,
                key2,
                "values of existing and expected content for key 't2' are different"));

    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                store()
                    .merge(
                        sourceHead,
                        MAIN_BRANCH,
                        Optional.empty(),
                        metadataRewriter,
                        individualCommits,
                        singletonMap(
                            key2,
                            MergeKeyBehavior.of(key2, MergeBehavior.NORMAL, null, resolvedContent)),
                        MergeBehavior.NORMAL,
                        false,
                        false))
        .withMessage(
            "MergeKeyBehavior.resolvedContent requires setting MergeKeyBehavior.expectedTarget as well for key t2");

    MergeResult<Commit> result =
        store()
            .merge(
                sourceHead,
                MAIN_BRANCH,
                Optional.empty(),
                metadataRewriter,
                individualCommits,
                singletonMap(
                    key2,
                    MergeKeyBehavior.of(key2, MergeBehavior.NORMAL, contentT2, resolvedContent)),
                MergeBehavior.NORMAL,
                false,
                false);
    ReferenceInfo<CommitMeta> branch =
        store().getNamedRef(MAIN_BRANCH.getName(), GetNamedRefsParams.DEFAULT);
    soft.assertThat(result)
        .extracting(
            MergeResult::wasApplied,
            MergeResult::wasSuccessful,
            MergeResult::getResultantTargetHash,
            MergeResult::getCommonAncestor,
            MergeResult::getEffectiveTargetHash)
        .containsExactly(true, true, branch.getHash(), thirdCommit, targetHead);

    Content mergedContent = store().getValue(MAIN_BRANCH, key2);
    soft.assertThat(mergedContent).isEqualTo(resolvedContent);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  protected void mergeIntoEmptyBranch3Commits(boolean individualCommits)
      throws VersionStoreException {
    final BranchName newBranch = BranchName.of("mergeIntoEmptyBranch3Commits");
    store().create(newBranch, Optional.of(initialHash));

    MetadataRewriter<CommitMeta> metadataRewriter = createMetadataRewriter("");

    doMergeIntoEmpty(individualCommits, newBranch, metadataRewriter);

    if (individualCommits) {
      // not modifying commit meta, will just "fast forward"
      soft.assertThat(store().hashOnReference(newBranch, Optional.empty())).isEqualTo(thirdCommit);

      assertCommitMeta(
          soft, commitsList(newBranch, false).subList(0, 3), commits, metadataRewriter);
    } else {
      // must modify commit meta, it's more than one commit
      assertThat(store().hashOnReference(newBranch, Optional.empty())).isNotEqualTo(thirdCommit);

      soft.assertThat(commitsList(newBranch, false))
          .first()
          .extracting(Commit::getCommitMeta)
          .extracting(CommitMeta::getMessage)
          .asString()
          .contains(
              commits.stream()
                  .map(Commit::getCommitMeta)
                  .map(CommitMeta::getMessage)
                  .toArray(String[]::new));
    }
  }

  private void doMergeIntoEmpty(
      boolean individualCommits,
      BranchName newBranch,
      MetadataRewriter<CommitMeta> metadataRewriter)
      throws ReferenceNotFoundException, ReferenceConflictException {
    store()
        .merge(
            thirdCommit,
            newBranch,
            Optional.of(initialHash),
            metadataRewriter,
            individualCommits,
            Collections.emptyMap(),
            MergeBehavior.NORMAL,
            false,
            false);
    soft.assertThat(
            contentsWithoutId(
                store()
                    .getValues(
                        newBranch,
                        Arrays.asList(
                            ContentKey.of("t1"),
                            ContentKey.of("t2"),
                            ContentKey.of("t3"),
                            ContentKey.of("t4")))))
        .containsExactlyInAnyOrderEntriesOf(
            ImmutableMap.of(
                ContentKey.of("t1"), V_1_2,
                ContentKey.of("t2"), V_2_2,
                ContentKey.of("t4"), V_4_1));
  }

  @SuppressWarnings("deprecation")
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void compareDryAndEffectiveMergeResults(boolean individualCommits) throws VersionStoreException {
    final BranchName newBranch = BranchName.of("compareDryAndEffectiveMergeResults");
    store().create(newBranch, Optional.of(initialHash));
    MetadataRewriter<CommitMeta> metadataRewriter = createMetadataRewriter("");

    Hash origHead = store().getNamedRef(newBranch.getName(), GetNamedRefsParams.DEFAULT).getHash();

    MergeResult<Commit> dryMergeResult =
        store()
            .merge(
                firstCommit,
                newBranch,
                Optional.of(initialHash),
                metadataRewriter,
                individualCommits,
                Collections.emptyMap(),
                MergeBehavior.NORMAL,
                true,
                true);

    soft.assertThat(dryMergeResult)
        .extracting(
            MergeResult::wasApplied,
            MergeResult::wasSuccessful,
            MergeResult::getCommonAncestor,
            MergeResult::getTargetBranch,
            MergeResult::getEffectiveTargetHash,
            MergeResult::getExpectedHash)
        .containsExactly(false, true, initialHash, newBranch, initialHash, initialHash);

    // Dry merges should not advance HEAD
    soft.assertThat(store().getNamedRef(newBranch.getName(), GetNamedRefsParams.DEFAULT).getHash())
        .isEqualTo(origHead);

    MergeResult<Commit> mergeResult =
        store()
            .merge(
                firstCommit,
                newBranch,
                Optional.of(initialHash),
                metadataRewriter,
                individualCommits,
                Collections.emptyMap(),
                MergeBehavior.NORMAL,
                false,
                true);

    Hash head = store().getNamedRef(newBranch.getName(), GetNamedRefsParams.DEFAULT).getHash();
    soft.assertThat(head).isNotEqualTo(origHead);

    soft.assertThat(mergeResult.getSourceCommits())
        .satisfiesAnyOf(
            // Persist storage
            l -> assertThat(l).isEmpty(),
            // Database adapter
            l ->
                assertThat(l)
                    .extracting(
                        Commit::getHash,
                        c -> c.getCommitMeta().getMessage(),
                        c -> operationsWithoutContentId(c.getOperations()))
                    .containsExactly(
                        tuple(
                            firstCommit,
                            "First Commit",
                            Arrays.asList(
                                Put.of(ContentKey.of("t1"), V_1_1),
                                Put.of(ContentKey.of("t2"), V_2_1),
                                Put.of(ContentKey.of("t3"), V_3_1)))));
    soft.assertThat(mergeResult.getTargetCommits()).isNull();
    soft.assertThat(mergeResult.getDetails())
        .containsKeys(ContentKey.of("t1"), ContentKey.of("t2"), ContentKey.of("t3"));
    soft.assertThat(mergeResult)
        // compare "effective" merge-result with re-constructed merge-result
        .isEqualTo(
            MergeResult.<Commit>builder()
                .wasApplied(true)
                .wasSuccessful(true)
                .commonAncestor(initialHash)
                .resultantTargetHash(head)
                .targetBranch(newBranch)
                .effectiveTargetHash(initialHash)
                .expectedHash(initialHash)
                .addAllSourceCommits(mergeResult.getSourceCommits())
                .putAllDetails(mergeResult.getDetails())
                .build())
        // compare "effective" merge-result with dry-run merge-result
        .isEqualTo(
            MergeResult.<Commit>builder()
                .from(dryMergeResult)
                .wasApplied(true)
                .resultantTargetHash(mergeResult.getResultantTargetHash())
                .build());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  protected void mergeIntoEmptyBranch1Commit(boolean individualCommits)
      throws VersionStoreException {
    final BranchName newBranch = BranchName.of("mergeIntoEmptyBranch1Commit");
    store().create(newBranch, Optional.of(initialHash));

    MetadataRewriter<CommitMeta> metadataRewriter = createMetadataRewriter("");

    store()
        .merge(
            firstCommit,
            newBranch,
            Optional.of(initialHash),
            metadataRewriter,
            individualCommits,
            Collections.emptyMap(),
            MergeBehavior.NORMAL,
            false,
            false);
    soft.assertThat(
            contentsWithoutId(
                store()
                    .getValues(
                        newBranch,
                        Arrays.asList(
                            ContentKey.of("t1"),
                            ContentKey.of("t2"),
                            ContentKey.of("t3"),
                            ContentKey.of("t4")))))
        .containsExactlyInAnyOrderEntriesOf(
            ImmutableMap.of(
                ContentKey.of("t1"), V_1_1,
                ContentKey.of("t2"), V_2_1,
                ContentKey.of("t3"), V_3_1));

    // not modifying commit meta, will just "fast forward"
    soft.assertThat(store().hashOnReference(newBranch, Optional.empty())).isEqualTo(firstCommit);

    List<Commit> mergedCommit = commitsList(newBranch, false).subList(0, 1);
    assertCommitMeta(soft, mergedCommit, commits.subList(2, 3), metadataRewriter);

    // fast-forward merge, additional parents not necessary (and not set)
    soft.assertThat(mergedCommit.get(0))
        .extracting(Commit::getCommitMeta)
        .extracting(CommitMeta::getParentCommitHashes)
        .asInstanceOf(list(Hash.class))
        .hasSize(1);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  protected void mergeIntoEmptyBranchModifying(boolean individualCommits)
      throws VersionStoreException {
    final BranchName newBranch = BranchName.of("mergeIntoEmptyBranchModifying");
    store().create(newBranch, Optional.of(initialHash));

    MetadataRewriter<CommitMeta> metadataRewriter = createMetadataRewriter(", merged");

    doMergeIntoEmpty(individualCommits, newBranch, metadataRewriter);

    // modify the commit meta, will generate new commits and therefore new commit hashes
    soft.assertThat(store().hashOnReference(newBranch, Optional.empty())).isNotEqualTo(thirdCommit);

    List<Commit> mergedCommits = commitsList(newBranch, false);
    if (individualCommits) {
      assertCommitMeta(soft, mergedCommits.subList(0, 3), commits, metadataRewriter);
    } else {
      soft.assertThat(mergedCommits)
          .first()
          .extracting(Commit::getCommitMeta)
          .extracting(CommitMeta::getMessage)
          .asString()
          .contains(
              commits.stream()
                  .map(Commit::getCommitMeta)
                  .map(CommitMeta::getMessage)
                  .toArray(String[]::new));

      soft.assertThat(mergedCommits)
          .first()
          .extracting(Commit::getCommitMeta)
          .extracting(CommitMeta::getParentCommitHashes)
          .asInstanceOf(list(String.class))
          .containsExactly(
              mergedCommits.get(1).getHash().asString(), commits.get(0).getHash().asString());
    }
  }

  @SuppressWarnings("deprecation")
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  protected void mergeIntoNonConflictingBranch(boolean individualCommits)
      throws VersionStoreException {
    final BranchName newBranch = BranchName.of("bar_2");
    store().create(newBranch, Optional.of(initialHash));
    final Hash newCommit = commit("Unrelated commit").put("t5", V_5_1).toBranch(newBranch);

    MetadataRewriter<CommitMeta> metadataRewriter = createMetadataRewriter("");

    MergeResult<Commit> result =
        store()
            .merge(
                thirdCommit,
                newBranch,
                Optional.empty(),
                metadataRewriter,
                individualCommits,
                Collections.emptyMap(),
                MergeBehavior.NORMAL,
                false,
                false);

    soft.assertThat(result.getResultantTargetHash()).isNotEqualTo(thirdCommit);
    soft.assertThat(result.getSourceCommits())
        .satisfiesAnyOf(
            // Persist storage
            l -> assertThat(l).isEmpty(),
            // Database adapter
            l -> assertThat(l).hasSize(3));
    soft.assertThat(result)
        .extracting(
            MergeResult::getCommonAncestor, MergeResult::wasSuccessful, MergeResult::wasApplied)
        .containsExactly(initialHash, true, true);

    soft.assertThat(
            contentsWithoutId(
                store()
                    .getValues(
                        newBranch,
                        Arrays.asList(
                            ContentKey.of("t1"),
                            ContentKey.of("t2"),
                            ContentKey.of("t3"),
                            ContentKey.of("t4"),
                            ContentKey.of("t5")))))
        .containsExactlyInAnyOrderEntriesOf(
            ImmutableMap.of(
                ContentKey.of("t1"), V_1_2,
                ContentKey.of("t2"), V_2_2,
                ContentKey.of("t4"), V_4_1,
                ContentKey.of("t5"), V_5_1));

    final List<Commit> commits = commitsList(newBranch, false);
    if (individualCommits) {
      soft.assertThat(commits)
          .satisfiesExactly(
              c0 ->
                  assertThat(c0)
                      .extracting(Commit::getCommitMeta)
                      .extracting(CommitMeta::getMessage)
                      .isEqualTo("Third Commit"),
              c1 ->
                  assertThat(c1)
                      .extracting(Commit::getCommitMeta)
                      .extracting(CommitMeta::getMessage)
                      .isEqualTo("Second Commit"),
              c2 ->
                  assertThat(c2)
                      .extracting(Commit::getCommitMeta)
                      .extracting(CommitMeta::getMessage)
                      .isEqualTo("First Commit"),
              c3 -> assertThat(c3).extracting(Commit::getHash).isEqualTo(newCommit),
              c4 -> assertThat(c4).extracting(Commit::getHash).isEqualTo(initialHash));
    } else {
      soft.assertThat(commits)
          .satisfiesExactly(
              c0 ->
                  assertThat(c0)
                      .extracting(Commit::getCommitMeta)
                      .extracting(CommitMeta::getMessage)
                      .asString()
                      .contains("Third Commit", "Second Commit", "First Commit"),
              c3 -> assertThat(c3).extracting(Commit::getHash).isEqualTo(newCommit),
              c4 -> assertThat(c4).extracting(Commit::getHash).isEqualTo(initialHash));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  protected void nonEmptyFastForwardMerge(boolean individualCommits) throws VersionStoreException {
    final ContentKey key = ContentKey.of("t1");
    final BranchName etl = BranchName.of("etl");
    final BranchName review = BranchName.of("review");
    store().create(etl, Optional.of(initialHash));
    store().create(review, Optional.of(initialHash));

    MetadataRewriter<CommitMeta> metadataRewriter = createMetadataRewriter("");
    Hash etl1 =
        store()
            .commit(
                etl,
                Optional.empty(),
                CommitMeta.fromMessage("commit 1"),
                singletonList(Put.of(key, VALUE_1)));
    Content v = store().getValue(etl, key);
    MergeResult<Commit> mergeResult1 =
        store()
            .merge(
                store().hashOnReference(etl, Optional.empty()),
                review,
                Optional.empty(),
                metadataRewriter,
                individualCommits,
                Collections.emptyMap(),
                MergeBehavior.NORMAL,
                false,
                false);
    soft.assertThat(mergeResult1.getResultantTargetHash()).isEqualTo(etl1);

    Hash etl2 =
        store()
            .commit(
                etl,
                Optional.empty(),
                CommitMeta.fromMessage("commit 2"),
                singletonList(Put.of(key, VALUE_2.withId(v))));
    MergeResult<Commit> mergeResult2 =
        store()
            .merge(
                store().hashOnReference(etl, Optional.empty()),
                review,
                Optional.empty(),
                metadataRewriter,
                individualCommits,
                Collections.emptyMap(),
                MergeBehavior.NORMAL,
                false,
                false);
    soft.assertThat(mergeResult2.getResultantTargetHash()).isEqualTo(etl2);

    soft.assertThat(contentWithoutId(store().getValue(review, key))).isEqualTo(VALUE_2);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  protected void mergeWithCommonAncestor(boolean individualCommits) throws VersionStoreException {
    final BranchName newBranch = BranchName.of("bar_2");
    store().create(newBranch, Optional.of(firstCommit));

    final Hash newCommit = commit("Unrelated commit").put("t5", V_5_1).toBranch(newBranch);

    MetadataRewriter<CommitMeta> metadataRewriter = createMetadataRewriter("");

    store()
        .merge(
            thirdCommit,
            newBranch,
            Optional.empty(),
            metadataRewriter,
            individualCommits,
            Collections.emptyMap(),
            MergeBehavior.NORMAL,
            false,
            false);
    soft.assertThat(
            contentsWithoutId(
                store()
                    .getValues(
                        newBranch,
                        Arrays.asList(
                            ContentKey.of("t1"),
                            ContentKey.of("t2"),
                            ContentKey.of("t3"),
                            ContentKey.of("t4"),
                            ContentKey.of("t5")))))
        .containsExactlyInAnyOrderEntriesOf(
            ImmutableMap.of(
                ContentKey.of("t1"), V_1_2,
                ContentKey.of("t2"), V_2_2,
                ContentKey.of("t4"), V_4_1,
                ContentKey.of("t5"), V_5_1));

    final List<Commit> commits = commitsList(newBranch, false);
    if (individualCommits) {
      soft.assertThat(commits)
          .hasSize(5)
          .satisfiesExactly(
              c -> assertThat(c.getCommitMeta().getMessage()).isEqualTo("Third Commit"),
              c -> assertThat(c.getCommitMeta().getMessage()).isEqualTo("Second Commit"),
              c -> assertThat(c.getHash()).isEqualTo(newCommit),
              c -> assertThat(c.getHash()).isEqualTo(firstCommit),
              c -> assertThat(c.getHash()).isEqualTo(initialHash));
    } else {
      soft.assertThat(commits)
          .hasSize(4)
          .satisfiesExactly(
              c ->
                  assertThat(c.getCommitMeta().getMessage())
                      .contains("Second Commit", "Third Commit"),
              c -> assertThat(c.getHash()).isEqualTo(newCommit),
              c -> assertThat(c.getHash()).isEqualTo(firstCommit),
              c -> assertThat(c.getHash()).isEqualTo(initialHash));

      soft.assertThat(commits)
          .first()
          .extracting(Commit::getCommitMeta)
          .extracting(CommitMeta::getParentCommitHashes)
          .asInstanceOf(list(String.class))
          .containsExactly(newCommit.asString(), thirdCommit.asString());
    }
  }

  @ParameterizedTest
  @CsvSource({
    "false,false",
    "false,true",
    "true,false",
    "true,true",
  })
  protected void mergeWithConflictingKeys(boolean individualCommits, boolean dryRun)
      throws VersionStoreException {
    final BranchName mergeInto = BranchName.of("foofoo");
    final BranchName mergeFrom = BranchName.of("barbar");
    store().create(mergeInto, Optional.of(this.initialHash));
    store().create(mergeFrom, Optional.of(this.initialHash));

    // we're essentially modifying the same key on both branches and then merging one branch into
    // the other and expect a conflict
    ContentKey conflictingKey1 = ContentKey.of("some_key1");
    ContentKey conflictingKey2 = ContentKey.of("some_key2");
    ContentKey key3 = ContentKey.of("some_key3");
    ContentKey key4 = ContentKey.of("some_key4");

    store()
        .commit(
            mergeInto,
            Optional.empty(),
            CommitMeta.fromMessage("commit 1"),
            singletonList(Put.of(conflictingKey1, VALUE_1)));
    store()
        .commit(
            mergeFrom,
            Optional.empty(),
            CommitMeta.fromMessage("commit 2"),
            Arrays.asList(Put.of(conflictingKey1, VALUE_2), Put.of(key3, VALUE_5)));
    Hash mergeIntoHead =
        store()
            .commit(
                mergeInto,
                Optional.empty(),
                CommitMeta.fromMessage("commit 3"),
                Arrays.asList(Put.of(conflictingKey2, VALUE_3), Put.of(key4, VALUE_6)));
    Hash mergeFromHash =
        store()
            .commit(
                mergeFrom,
                Optional.empty(),
                CommitMeta.fromMessage("commit 4"),
                singletonList(Put.of(conflictingKey2, VALUE_4)));

    StorageAssertions checkpoint = storageCheckpoint();
    // "Plain" merge attempt - all keys default to MergeBehavior.NORMAL
    soft.assertThatThrownBy(
            () ->
                store()
                    .merge(
                        mergeFromHash,
                        mergeInto,
                        Optional.empty(),
                        createMetadataRewriter(""),
                        individualCommits,
                        Collections.emptyMap(),
                        MergeBehavior.NORMAL,
                        dryRun,
                        false))
        .isInstanceOf(ReferenceConflictException.class)
        .hasMessageContaining("The following keys have been changed in conflict:")
        .hasMessageContaining(conflictingKey1.toString())
        .hasMessageContaining(conflictingKey2.toString());
    if (dryRun) {
      checkpoint.assertNoWrites();
    }

    // default to MergeBehavior.NORMAL, but ignore conflictingKey1
    soft.assertThatThrownBy(
            () ->
                store()
                    .merge(
                        mergeFromHash,
                        mergeInto,
                        Optional.empty(),
                        createMetadataRewriter(""),
                        individualCommits,
                        Collections.singletonMap(
                            conflictingKey2,
                            MergeKeyBehavior.of(conflictingKey2, MergeBehavior.DROP)),
                        MergeBehavior.NORMAL,
                        dryRun,
                        false))
        .isInstanceOf(ReferenceConflictException.class)
        .hasMessageContaining("The following keys have been changed in conflict:")
        .hasMessageContaining(conflictingKey1.toString())
        .hasMessageNotContaining(conflictingKey2.toString());
    if (dryRun) {
      checkpoint.assertNoWrites();
    }

    // default to MergeBehavior.DROP (don't merge keys by default), but include conflictingKey1
    soft.assertThatThrownBy(
            () ->
                store()
                    .merge(
                        mergeFromHash,
                        mergeInto,
                        Optional.empty(),
                        createMetadataRewriter(""),
                        individualCommits,
                        Collections.singletonMap(
                            conflictingKey1,
                            MergeKeyBehavior.of(conflictingKey1, MergeBehavior.NORMAL)),
                        MergeBehavior.DROP,
                        dryRun,
                        false))
        .isInstanceOf(ReferenceConflictException.class)
        .hasMessageContaining("The following keys have been changed in conflict:")
        .hasMessageContaining(conflictingKey1.toString())
        .hasMessageNotContaining(conflictingKey2.toString());
    if (dryRun) {
      checkpoint.assertNoWrites();
    }

    // default to MergeBehavior.NORMAL, but include conflictingKey1
    soft.assertThatThrownBy(
            () ->
                store()
                    .merge(
                        mergeFromHash,
                        mergeInto,
                        Optional.empty(),
                        createMetadataRewriter(""),
                        individualCommits,
                        Collections.singletonMap(
                            conflictingKey1,
                            MergeKeyBehavior.of(conflictingKey1, MergeBehavior.FORCE)),
                        MergeBehavior.NORMAL,
                        dryRun,
                        false))
        .isInstanceOf(ReferenceConflictException.class)
        .hasMessageContaining("The following keys have been changed in conflict:")
        .hasMessageNotContaining(conflictingKey1.toString())
        .hasMessageContaining(conflictingKey2.toString());
    if (dryRun) {
      checkpoint.assertNoWrites();
    }

    Supplier<Hash> mergeIntoHeadSupplier =
        () -> {
          try {
            return store.getNamedRef(mergeInto.getName(), GetNamedRefsParams.DEFAULT).getHash();
          } catch (ReferenceNotFoundException e) {
            throw new RuntimeException(e);
          }
        };

    soft.assertThat(mergeIntoHeadSupplier.get()).isEqualTo(mergeIntoHead);

    // Merge with force-merge of conflictingKey1 + drop of conflictingKey2
    store()
        .merge(
            mergeFromHash,
            mergeInto,
            Optional.empty(),
            createMetadataRewriter(", merge-force-1"),
            individualCommits,
            ImmutableMap.of(
                conflictingKey1,
                MergeKeyBehavior.of(conflictingKey1, MergeBehavior.FORCE),
                conflictingKey2,
                MergeKeyBehavior.of(conflictingKey2, MergeBehavior.DROP)),
            MergeBehavior.NORMAL,
            false,
            false);
    soft.assertThat(
            contentsWithoutId(
                store.getValues(
                    mergeIntoHeadSupplier.get(),
                    Arrays.asList(conflictingKey1, conflictingKey2, key3, key4))))
        .containsEntry(conflictingKey1, VALUE_2) // value as in "mergeFrom"
        .containsEntry(conflictingKey2, VALUE_3) // value as in "mergeInto"
        .containsEntry(key3, VALUE_5)
        .containsEntry(key4, VALUE_6);

    // reset merge-into branch
    store().assign(mergeInto, Optional.empty(), mergeIntoHead);

    // Merge with force-merge of conflictingKey1 + drop of conflictingKey2
    store()
        .merge(
            mergeFromHash,
            mergeInto,
            Optional.empty(),
            createMetadataRewriter(", merge-all-force"),
            individualCommits,
            Collections.emptyMap(),
            MergeBehavior.FORCE,
            false,
            false);
    soft.assertThat(
            contentsWithoutId(
                store.getValues(
                    mergeIntoHeadSupplier.get(),
                    Arrays.asList(conflictingKey1, conflictingKey2, key3, key4))))
        .containsEntry(conflictingKey1, VALUE_2) // value as in "mergeFrom"
        .containsEntry(conflictingKey2, VALUE_4) // value as in "mergeFrom"
        .containsEntry(key3, VALUE_5)
        .containsEntry(key4, VALUE_6);
  }

  @ParameterizedTest
  @CsvSource({
    "false,false",
    "false,true",
    "true,false",
    "true,true",
  })
  protected void mergeIntoConflictingBranch(boolean individualCommits, boolean dryRun)
      throws VersionStoreException {
    final BranchName newBranch = BranchName.of("bar_3");
    store().create(newBranch, Optional.of(initialHash));
    commit("Another commit").put("t1", V_1_4).toBranch(newBranch);

    StorageAssertions checkpoint = storageCheckpoint();
    soft.assertThatThrownBy(
            () ->
                store()
                    .merge(
                        thirdCommit,
                        newBranch,
                        Optional.of(initialHash),
                        createMetadataRewriter(""),
                        individualCommits,
                        Collections.emptyMap(),
                        MergeBehavior.NORMAL,
                        dryRun,
                        false))
        .isInstanceOf(ReferenceConflictException.class);
    if (dryRun) {
      checkpoint.assertNoWrites();
    }
  }

  @ParameterizedTest
  @CsvSource({
    "false,false",
    "false,true",
    "true,false",
    "true,true",
  })
  protected void mergeIntoNonExistingBranch(boolean individualCommits, boolean dryRun) {
    final BranchName newBranch = BranchName.of("bar_5");
    StorageAssertions checkpoint = storageCheckpoint();
    soft.assertThatThrownBy(
            () ->
                store()
                    .merge(
                        thirdCommit,
                        newBranch,
                        Optional.of(initialHash),
                        createMetadataRewriter(""),
                        individualCommits,
                        Collections.emptyMap(),
                        MergeBehavior.NORMAL,
                        dryRun,
                        false))
        .isInstanceOf(ReferenceNotFoundException.class);
    checkpoint.assertNoWrites();
  }

  @ParameterizedTest
  @CsvSource({
    "false,false",
    "false,true",
    "true,false",
    "true,true",
  })
  protected void mergeIntoNonExistingReference(boolean individualCommits, boolean dryRun)
      throws VersionStoreException {
    final BranchName newBranch = BranchName.of("bar_6");
    store().create(newBranch, Optional.of(initialHash));
    StorageAssertions checkpoint = storageCheckpoint();
    soft.assertThatThrownBy(
            () ->
                store()
                    .merge(
                        Hash.of("1234567890abcdef"),
                        newBranch,
                        Optional.of(initialHash),
                        createMetadataRewriter(""),
                        individualCommits,
                        Collections.emptyMap(),
                        MergeBehavior.NORMAL,
                        dryRun,
                        false))
        .isInstanceOf(ReferenceNotFoundException.class);
    checkpoint.assertNoWrites();
  }

  @ParameterizedTest
  @CsvSource({
    "false,false",
    "false,true",
    "true,false",
    "true,true",
  })
  protected void mergeEmptyCommit(boolean individualCommits, boolean dryRun)
      throws VersionStoreException {
    BranchName source = BranchName.of("source");
    BranchName target = BranchName.of("target");
    store().create(source, Optional.of(this.initialHash));
    store().create(target, Optional.of(this.initialHash));

    ContentKey key1 = ContentKey.of("key1");
    ContentKey key2 = ContentKey.of("key2");

    // Add two commits to the target branch

    Hash targetHead =
        store()
            .commit(
                target,
                Optional.empty(),
                CommitMeta.fromMessage("target 1"),
                singletonList(Put.of(key1, VALUE_1)));

    targetHead =
        store()
            .commit(
                target,
                Optional.of(targetHead),
                CommitMeta.fromMessage("target 2"),
                singletonList(Put.of(key2, VALUE_1)));

    // Add two commits to the source branch, with conflicting changes to key1 and key2

    Hash sourceHead =
        store()
            .commit(
                source,
                Optional.empty(),
                CommitMeta.fromMessage("source 1"),
                singletonList(Put.of(key1, VALUE_2)));

    sourceHead =
        store()
            .commit(
                source,
                Optional.of(sourceHead),
                CommitMeta.fromMessage("source 2"),
                singletonList(Put.of(key2, VALUE_2)));

    // Merge the source branch into the target branch, with a drop of key1 and key2

    store()
        .merge(
            sourceHead,
            target,
            Optional.ofNullable(targetHead),
            createMetadataRewriter(", merge-drop"),
            individualCommits,
            ImmutableMap.of(
                key1,
                MergeKeyBehavior.of(key1, MergeBehavior.DROP),
                key2,
                MergeKeyBehavior.of(key2, MergeBehavior.DROP)),
            MergeBehavior.NORMAL,
            dryRun,
            false);

    // No new commit should have been created in the target branch
    try (PaginationIterator<Commit> iterator = store().getCommits(target, true)) {
      Hash newTargetHead = iterator.next().getHash();
      assertThat(newTargetHead).isEqualTo(targetHead);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void mergeFromAndIntoHead(boolean dryRun) throws Exception {
    BranchName branch = BranchName.of("source");
    store().create(branch, Optional.of(this.initialHash));

    ContentKey key1 = ContentKey.of("key1");
    ContentKey key2 = ContentKey.of("key2");

    Hash commit1 =
        store()
            .commit(
                branch,
                Optional.empty(),
                CommitMeta.fromMessage("commit 1"),
                singletonList(Put.of(key1, VALUE_1)));

    Hash commit2 =
        store()
            .commit(
                branch,
                Optional.empty(),
                CommitMeta.fromMessage("commit 2"),
                singletonList(Put.of(key2, VALUE_2)));

    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                store()
                    .merge(
                        commit2,
                        branch,
                        Optional.of(commit1),
                        createMetadataRewriter(""),
                        false,
                        emptyMap(),
                        MergeBehavior.NORMAL,
                        dryRun,
                        false));

    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                store()
                    .merge(
                        commit1,
                        branch,
                        Optional.of(commit2),
                        createMetadataRewriter(""),
                        false,
                        emptyMap(),
                        MergeBehavior.NORMAL,
                        dryRun,
                        false));
  }
}
