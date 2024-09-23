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

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.list;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.projectnessie.versioned.testworker.OnRefOnly.newOnRef;
import static org.projectnessie.versioned.testworker.OnRefOnly.onRef;

import java.util.Optional;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.error.ReferenceConflicts;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Conflict;
import org.projectnessie.model.Conflict.ConflictType;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.MergeBehavior;
import org.projectnessie.model.MergeKeyBehavior;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Commit;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.MergeConflictException;
import org.projectnessie.versioned.MergeResult;
import org.projectnessie.versioned.MergeTransplantResultBase;
import org.projectnessie.versioned.MergeTransplantResultBase.KeyDetails;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.VersionStore.MergeOp;
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
  private static final OnRefOnly VALUE_1 = newOnRef("value1");
  private static final OnRefOnly VALUE_2 = newOnRef("value2");

  protected AbstractMerge(VersionStore store) {
    super(store);
  }

  private Hash initialHash;
  private Hash firstCommit;
  private Hash thirdCommit;

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

    Content t1 =
        requireNonNull(store().getValue(MAIN_BRANCH, ContentKey.of("t1"), false).content());

    commit("Second Commit")
        .put("t1", V_1_2.withId(t1.getId()))
        .delete("t2")
        .delete("t3")
        .put("t4", V_4_1)
        .toBranch(MAIN_BRANCH);

    thirdCommit = commit("Third Commit").put("t2", V_2_2).unchanged("t4").toBranch(MAIN_BRANCH);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  protected void mergeKeyBehaviorValidation(boolean dryRun) throws Exception {
    BranchName targetBranch = BranchName.of("mergeKeyBehaviorValidation");
    store().create(targetBranch, Optional.of(firstCommit));

    ContentKey keyNotUsed = ContentKey.of("not", "used");
    ContentKey keyUnused = ContentKey.of("un", "used");
    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                store()
                    .merge(
                        MergeOp.builder()
                            .fromRef(MAIN_BRANCH)
                            .fromHash(thirdCommit)
                            .toBranch(targetBranch)
                            .putMergeKeyBehaviors(
                                keyNotUsed, MergeKeyBehavior.of(keyNotUsed, MergeBehavior.DROP))
                            .putMergeKeyBehaviors(
                                keyUnused, MergeKeyBehavior.of(keyUnused, MergeBehavior.DROP))
                            .dryRun(dryRun)
                            .build()))
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
                          MergeOp.builder()
                              .fromRef(MAIN_BRANCH)
                              .fromHash(thirdCommit)
                              .toBranch(targetBranch)
                              .putMergeKeyBehaviors(
                                  keyT3, MergeKeyBehavior.of(keyT3, mergeBehavior, V_3_1, V_1_1))
                              .dryRun(dryRun)
                              .build()))
          .withMessage(
              "MergeKeyBehavior.resolvedContent must be null for MergeBehavior.%s for t3",
              mergeBehavior);
    }

    Content c11 = store().getValue(firstCommit, ContentKey.of("t1"), false).content();

    for (MergeBehavior mergeBehavior :
        new MergeBehavior[] {MergeBehavior.NORMAL, MergeBehavior.FORCE}) {
      StorageAssertions checkpoint = storageCheckpoint();
      soft.assertThatThrownBy(
              () ->
                  store()
                      .merge(
                          MergeOp.builder()
                              .fromRef(MAIN_BRANCH)
                              .fromHash(thirdCommit)
                              .toBranch(targetBranch)
                              .putMergeKeyBehaviors(
                                  keyT3, MergeKeyBehavior.of(keyT3, mergeBehavior, c11, null))
                              .dryRun(dryRun)
                              .build()))
          .describedAs("MergeBehavior.%s", mergeBehavior)
          .hasMessage("The following keys have been changed in conflict: 't3'")
          .asInstanceOf(type(MergeConflictException.class))
          .extracting(MergeConflictException::getMergeResult)
          .extracting(
              MergeTransplantResultBase::wasApplied,
              MergeTransplantResultBase::wasSuccessful,
              r -> r.getDetails().get(keyT3))
          .containsExactly(
              false,
              false,
              KeyDetails.keyDetails(
                  mergeBehavior,
                  Conflict.conflict(
                      ConflictType.VALUE_DIFFERS,
                      keyT3,
                      "values of existing and expected content for key 't3' are different")));
      checkpoint.assertNoWrites();
      soft.assertAll();
    }
  }

  @Test
  protected void mergeResolveConflict() throws VersionStoreException {
    BranchName sourceBranch = BranchName.of("mergeResolveConflict");
    store().create(sourceBranch, Optional.of(thirdCommit));

    ContentKey key2 = ContentKey.of("t2");
    Content contentT2 = requireNonNull(store().getValue(MAIN_BRANCH, key2, false).content());

    Hash targetHead =
        commit("on-target-commit")
            .put("t2", onRef("v2_2-target", contentT2.getId()))
            .toBranch(MAIN_BRANCH);
    Hash sourceHead =
        commit("on-source-commit")
            .put("t2", onRef("v2_2-source", contentT2.getId()))
            .toBranch(sourceBranch);
    contentT2 = requireNonNull(store().getValue(MAIN_BRANCH, key2, false).content());

    soft.assertThatThrownBy(
            () ->
                store()
                    .merge(
                        MergeOp.builder()
                            .fromRef(sourceBranch)
                            .fromHash(sourceHead)
                            .toBranch(MAIN_BRANCH)
                            .build()))
        .isInstanceOf(MergeConflictException.class);

    Content resolvedContent = onRef("resolved", contentT2.getId());
    Content wrongExpectedContent = onRef("wrong", contentT2.getId());

    soft.assertThatThrownBy(
            () ->
                store()
                    .merge(
                        MergeOp.builder()
                            .fromRef(sourceBranch)
                            .fromHash(sourceHead)
                            .toBranch(MAIN_BRANCH)
                            .putMergeKeyBehaviors(
                                key2,
                                MergeKeyBehavior.of(
                                    key2,
                                    MergeBehavior.NORMAL,
                                    wrongExpectedContent,
                                    resolvedContent))
                            .build()))
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
                        MergeOp.builder()
                            .fromRef(sourceBranch)
                            .fromHash(sourceHead)
                            .toBranch(MAIN_BRANCH)
                            .putMergeKeyBehaviors(
                                key2,
                                MergeKeyBehavior.of(
                                    key2, MergeBehavior.NORMAL, null, resolvedContent))
                            .build()))
        .withMessage(
            "MergeKeyBehavior.resolvedContent requires setting MergeKeyBehavior.expectedTarget as well for key t2");

    MergeResult result =
        store()
            .merge(
                MergeOp.builder()
                    .fromRef(sourceBranch)
                    .fromHash(sourceHead)
                    .toBranch(MAIN_BRANCH)
                    .putMergeKeyBehaviors(
                        key2,
                        MergeKeyBehavior.of(key2, MergeBehavior.NORMAL, contentT2, resolvedContent))
                    .build());
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

    Content mergedContent = store().getValue(MAIN_BRANCH, key2, false).content();
    soft.assertThat(mergedContent).isEqualTo(resolvedContent);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  protected void mergeIntoConflictingBranch(boolean dryRun) throws VersionStoreException {
    final BranchName newBranch = BranchName.of("bar_3");
    store().create(newBranch, Optional.of(initialHash));
    commit("Another commit").put("t1", V_1_4).toBranch(newBranch);

    StorageAssertions checkpoint = storageCheckpoint();
    soft.assertThatThrownBy(
            () ->
                store()
                    .merge(
                        MergeOp.builder()
                            .fromRef(MAIN_BRANCH)
                            .fromHash(thirdCommit)
                            .toBranch(newBranch)
                            .expectedHash(Optional.of(initialHash))
                            .dryRun(dryRun)
                            .build()))
        .isInstanceOf(ReferenceConflictException.class);
    if (dryRun) {
      checkpoint.assertNoWrites();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  protected void mergeIntoNonExistingBranch(boolean dryRun) {
    final BranchName newBranch = BranchName.of("bar_5");
    StorageAssertions checkpoint = storageCheckpoint();
    soft.assertThatThrownBy(
            () ->
                store()
                    .merge(
                        MergeOp.builder()
                            .fromRef(MAIN_BRANCH)
                            .fromHash(thirdCommit)
                            .toBranch(newBranch)
                            .expectedHash(Optional.of(initialHash))
                            .dryRun(dryRun)
                            .build()))
        .isInstanceOf(ReferenceNotFoundException.class);
    checkpoint.assertNoWrites();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  protected void mergeIntoNonExistingReference(boolean dryRun) throws VersionStoreException {
    final BranchName newBranch = BranchName.of("bar_6");
    store().create(newBranch, Optional.of(initialHash));
    StorageAssertions checkpoint = storageCheckpoint();
    soft.assertThatThrownBy(
            () ->
                store()
                    .merge(
                        MergeOp.builder()
                            .fromRef(MAIN_BRANCH)
                            .fromHash(Hash.of("1234567890abcdef"))
                            .toBranch(newBranch)
                            .expectedHash(Optional.of(initialHash))
                            .dryRun(dryRun)
                            .build()))
        .isInstanceOf(ReferenceNotFoundException.class);
    checkpoint.assertNoWrites();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  protected void mergeEmptyCommit(boolean dryRun) throws VersionStoreException {
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
                singletonList(Put.of(key1, VALUE_1)))
            .getCommitHash();

    targetHead =
        store()
            .commit(
                target,
                Optional.of(targetHead),
                CommitMeta.fromMessage("target 2"),
                singletonList(Put.of(key2, VALUE_1)))
            .getCommitHash();

    // Add two commits to the source branch, with conflicting changes to key1 and key2

    Hash sourceHead =
        store()
            .commit(
                source,
                Optional.empty(),
                CommitMeta.fromMessage("source 1"),
                singletonList(Put.of(key1, VALUE_2)))
            .getCommitHash();

    sourceHead =
        store()
            .commit(
                source,
                Optional.of(sourceHead),
                CommitMeta.fromMessage("source 2"),
                singletonList(Put.of(key2, VALUE_2)))
            .getCommitHash();

    // Merge the source branch into the target branch, with a drop of key1 and key2

    store()
        .merge(
            MergeOp.builder()
                .fromRef(source)
                .fromHash(sourceHead)
                .toBranch(target)
                .expectedHash(Optional.ofNullable(targetHead))
                .putMergeKeyBehaviors(key1, MergeKeyBehavior.of(key1, MergeBehavior.DROP))
                .putMergeKeyBehaviors(key2, MergeKeyBehavior.of(key2, MergeBehavior.DROP))
                .dryRun(dryRun)
                .build());

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
                singletonList(Put.of(key1, VALUE_1)))
            .getCommitHash();

    Hash commit2 =
        store()
            .commit(
                branch,
                Optional.empty(),
                CommitMeta.fromMessage("commit 2"),
                singletonList(Put.of(key2, VALUE_2)))
            .getCommitHash();

    // New storage model allows "merging the same branch again". If nothing changed, it returns a
    // successful, but not-applied merge-response. This request is a merge without any commits to
    // merge, reported as "successful".
    soft.assertThat(
            store()
                .merge(
                    MergeOp.builder()
                        .fromRef(branch)
                        .fromHash(commit2)
                        .toBranch(branch)
                        .expectedHash(Optional.of(commit1))
                        .dryRun(dryRun)
                        .build()))
        .extracting(
            MergeResult::wasApplied,
            MergeResult::wasSuccessful,
            MergeResult::getResultantTargetHash,
            MergeResult::getCommonAncestor,
            MergeResult::getEffectiveTargetHash)
        .containsExactly(false, true, commit2, commit2, commit2);
  }
}
