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

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.assertj.core.api.InstanceOfAssertFactories.list;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.projectnessie.versioned.testworker.OnRefOnly.newOnRef;
import static org.projectnessie.versioned.testworker.OnRefOnly.onRef;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
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
import org.projectnessie.versioned.CommitResult;
import org.projectnessie.versioned.Delete;
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
import org.projectnessie.versioned.ResultType;
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

    Content t1 = store().getValue(MAIN_BRANCH, ContentKey.of("t1")).content();

    commit("Second Commit")
        .put("t1", V_1_2.withId(t1.getId()))
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
    assumeThat(isNewStorageModel()).isTrue();

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

    Content c11 = store().getValue(firstCommit, ContentKey.of("t1")).content();

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
              MergeResult::wasApplied, MergeResult::wasSuccessful, r -> r.getDetails().get(keyT3))
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

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  protected void mergeResolveConflict(boolean individualCommits) throws VersionStoreException {
    BranchName sourceBranch = BranchName.of("mergeResolveConflict");
    store().create(sourceBranch, Optional.of(thirdCommit));

    ContentKey key2 = ContentKey.of("t2");
    Content contentT2 = store().getValue(MAIN_BRANCH, key2).content();

    Hash targetHead =
        commit("on-target-commit")
            .put("t2", onRef("v2_2-target", contentT2.getId()))
            .toBranch(MAIN_BRANCH);
    Hash sourceHead =
        commit("on-source-commit")
            .put("t2", onRef("v2_2-source", contentT2.getId()))
            .toBranch(sourceBranch);
    contentT2 = store().getValue(MAIN_BRANCH, key2).content();

    soft.assertThatThrownBy(
            () ->
                store()
                    .merge(
                        MergeOp.builder()
                            .fromRef(sourceBranch)
                            .fromHash(sourceHead)
                            .toBranch(MAIN_BRANCH)
                            .keepIndividualCommits(individualCommits)
                            .build()))
        .isInstanceOf(MergeConflictException.class);

    Content resolvedContent = onRef("resolved", contentT2.getId());
    Content wrongExpectedContent = onRef("wrong", contentT2.getId());

    if (!isNewStorageModel()) {
      soft.assertThatIllegalArgumentException()
          .isThrownBy(
              () ->
                  store()
                      .merge(
                          MergeOp.builder()
                              .fromRef(sourceBranch)
                              .fromHash(sourceHead)
                              .toBranch(MAIN_BRANCH)
                              .keepIndividualCommits(individualCommits)
                              .putMergeKeyBehaviors(
                                  key2,
                                  MergeKeyBehavior.of(
                                      key2,
                                      MergeBehavior.NORMAL,
                                      wrongExpectedContent,
                                      resolvedContent))
                              .build()))
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
                          MergeOp.builder()
                              .fromRef(sourceBranch)
                              .fromHash(sourceHead)
                              .toBranch(MAIN_BRANCH)
                              .keepIndividualCommits(individualCommits)
                              .putMergeKeyBehaviors(
                                  key2,
                                  MergeKeyBehavior.of(
                                      key2,
                                      MergeBehavior.NORMAL,
                                      wrongExpectedContent,
                                      resolvedContent))
                              .build()))
          .withMessage(
              "MergeKeyBehavior.expectedTargetContent and MergeKeyBehavior.resolvedContent are only supported for squashing merge/transplant operations.");
      return;
    }

    soft.assertThatThrownBy(
            () ->
                store()
                    .merge(
                        MergeOp.builder()
                            .fromRef(sourceBranch)
                            .fromHash(sourceHead)
                            .toBranch(MAIN_BRANCH)
                            .keepIndividualCommits(individualCommits)
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
                            .keepIndividualCommits(individualCommits)
                            .putMergeKeyBehaviors(
                                key2,
                                MergeKeyBehavior.of(
                                    key2, MergeBehavior.NORMAL, null, resolvedContent))
                            .build()))
        .withMessage(
            "MergeKeyBehavior.resolvedContent requires setting MergeKeyBehavior.expectedTarget as well for key t2");

    MergeResult<Commit> result =
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

    Content mergedContent = store().getValue(MAIN_BRANCH, key2).content();
    soft.assertThat(mergedContent).isEqualTo(resolvedContent);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  protected void mergeIntoEmptyBranch3Commits(boolean individualCommits)
      throws VersionStoreException {
    final BranchName newBranch = BranchName.of("mergeIntoEmptyBranch3Commits");
    store().create(newBranch, Optional.of(initialHash));

    MetadataRewriter<CommitMeta> metadataRewriter = createMetadataRewriter("");

    doMergeIntoEmpty(individualCommits, newBranch, metadataRewriter, true);

    if (individualCommits) {
      // not modifying commit meta, will just "fast forward"
      soft.assertThat(store().hashOnReference(newBranch, Optional.empty(), emptyList()))
          .isEqualTo(thirdCommit);

      assertCommitMeta(
          soft, commitsList(newBranch, false).subList(0, 3), commits, metadataRewriter);
    } else {
      // must modify commit meta, it's more than one commit
      assertThat(store().hashOnReference(newBranch, Optional.empty(), emptyList()))
          .isNotEqualTo(thirdCommit);

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
      MetadataRewriter<CommitMeta> metadataRewriter,
      boolean expectFastForward)
      throws ReferenceNotFoundException, ReferenceConflictException {
    MergeResult<Commit> result =
        store()
            .merge(
                MergeOp.builder()
                    .fromRef(MAIN_BRANCH)
                    .fromHash(thirdCommit)
                    .toBranch(newBranch)
                    .expectedHash(Optional.of(initialHash))
                    .updateCommitMetadata(metadataRewriter)
                    .keepIndividualCommits(individualCommits)
                    .build());

    checkAddedCommits(individualCommits, initialHash, result, expectFastForward);

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

  private void checkAddedCommits(
      boolean individualCommits,
      Hash targetHead,
      MergeResult<Commit> result,
      boolean expectFastForward) {
    if (individualCommits) {
      if (expectFastForward) {
        soft.assertThat(result.getCreatedCommits()).isEmpty();
      } else {
        soft.assertThat(result.getCreatedCommits())
            .hasSize(3)
            .satisfiesExactly(
                c -> {
                  soft.assertThat(c.getParentHash()).isEqualTo(targetHead);
                  soft.assertThat(c.getCommitMeta().getMessage()).contains("First Commit");
                  soft.assertThat(c.getOperations())
                      .hasSize(3)
                      .satisfiesExactlyInAnyOrder(
                          o -> {
                            soft.assertThat(o).isInstanceOf(Put.class);
                            soft.assertThat(o.getKey()).isEqualTo(ContentKey.of("t1"));
                            soft.assertThat(contentWithoutId(((Put) o).getValue()))
                                .isEqualTo(V_1_1);
                          },
                          o -> {
                            soft.assertThat(o).isInstanceOf(Put.class);
                            soft.assertThat(o.getKey()).isEqualTo(ContentKey.of("t2"));
                            soft.assertThat(contentWithoutId(((Put) o).getValue()))
                                .isEqualTo(V_2_1);
                          },
                          o -> {
                            soft.assertThat(o).isInstanceOf(Put.class);
                            soft.assertThat(o.getKey()).isEqualTo(ContentKey.of("t3"));
                            soft.assertThat(contentWithoutId(((Put) o).getValue()))
                                .isEqualTo(V_3_1);
                          });
                },
                c -> {
                  soft.assertThat(c.getParentHash())
                      .isEqualTo(result.getCreatedCommits().get(0).getHash());
                  soft.assertThat(c.getCommitMeta().getMessage()).contains("Second Commit");
                  soft.assertThat(c.getOperations())
                      .hasSize(4)
                      .satisfiesExactlyInAnyOrder(
                          o -> {
                            soft.assertThat(o).isInstanceOf(Put.class);
                            soft.assertThat(o.getKey()).isEqualTo(ContentKey.of("t1"));
                            soft.assertThat(contentWithoutId(((Put) o).getValue()))
                                .isEqualTo(V_1_2);
                          },
                          o ->
                              soft.assertThat(o)
                                  .asInstanceOf(type(Delete.class))
                                  .extracting(Delete::getKey)
                                  .isEqualTo(ContentKey.of("t2")),
                          o ->
                              soft.assertThat(o)
                                  .asInstanceOf(type(Delete.class))
                                  .extracting(Delete::getKey)
                                  .isEqualTo(ContentKey.of("t3")),
                          o -> {
                            soft.assertThat(o).isInstanceOf(Put.class);
                            soft.assertThat(o.getKey()).isEqualTo(ContentKey.of("t4"));
                            soft.assertThat(contentWithoutId(((Put) o).getValue()))
                                .isEqualTo(V_4_1);
                          });
                },
                c -> {
                  soft.assertThat(c.getParentHash())
                      .isEqualTo(result.getCreatedCommits().get(1).getHash());
                  soft.assertThat(c.getCommitMeta().getMessage()).contains("Third Commit");
                  soft.assertThat(c.getOperations())
                      .hasSize(1)
                      .satisfiesExactlyInAnyOrder(
                          o -> {
                            soft.assertThat(o).isInstanceOf(Put.class);
                            soft.assertThat(o.getKey()).isEqualTo(ContentKey.of("t2"));
                            soft.assertThat(contentWithoutId(((Put) o).getValue()))
                                .isEqualTo(V_2_2);
                          }
                          // Unchanged operation not retained
                          );
                });
      }
    } else {
      soft.assertThat(result.getCreatedCommits())
          .singleElement()
          .satisfies(
              c -> {
                soft.assertThat(c.getParentHash()).isEqualTo(targetHead);
                soft.assertThat(c.getCommitMeta().getMessage())
                    .contains("First Commit")
                    .contains("Second Commit")
                    .contains("Third Commit");
                soft.assertThat(c.getOperations())
                    // old storage model keeps the Delete operation when a key is Put then Deleted,
                    // see https://github.com/projectnessie/nessie/pull/6472
                    .hasSizeBetween(3, 4)
                    .anySatisfy(
                        o -> {
                          if (c.getOperations() != null && c.getOperations().size() == 4) {
                            soft.assertThat(o).isInstanceOf(Delete.class);
                            soft.assertThat(o.getKey()).isEqualTo(ContentKey.of("t3"));
                          }
                        })
                    .anySatisfy(
                        o -> {
                          soft.assertThat(o).isInstanceOf(Put.class);
                          soft.assertThat(o.getKey()).isEqualTo(ContentKey.of("t1"));
                          soft.assertThat(contentWithoutId(((Put) o).getValue())).isEqualTo(V_1_2);
                        })
                    .anySatisfy(
                        o -> {
                          soft.assertThat(o).isInstanceOf(Put.class);
                          soft.assertThat(o.getKey()).isEqualTo(ContentKey.of("t2"));
                          soft.assertThat(contentWithoutId(((Put) o).getValue())).isEqualTo(V_2_2);
                        })
                    .anySatisfy(
                        o -> {
                          soft.assertThat(o).isInstanceOf(Put.class);
                          soft.assertThat(o.getKey()).isEqualTo(ContentKey.of("t4"));
                          soft.assertThat(contentWithoutId(((Put) o).getValue())).isEqualTo(V_4_1);
                        });
              });
    }
  }

  @SuppressWarnings("deprecation")
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void compareDryAndEffectiveMergeResults(boolean individualCommits) throws VersionStoreException {
    final BranchName newBranch = BranchName.of("compareDryAndEffectiveMergeResults");
    store().create(newBranch, Optional.of(initialHash));

    Hash origHead = store().getNamedRef(newBranch.getName(), GetNamedRefsParams.DEFAULT).getHash();

    MergeResult<Commit> dryMergeResult =
        store()
            .merge(
                MergeOp.builder()
                    .fromRef(MAIN_BRANCH)
                    .fromHash(firstCommit)
                    .toBranch(newBranch)
                    .expectedHash(Optional.of(initialHash))
                    .keepIndividualCommits(individualCommits)
                    .dryRun(true)
                    .fetchAdditionalInfo(true)
                    .build());

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
                MergeOp.builder()
                    .fromRef(MAIN_BRANCH)
                    .fromHash(firstCommit)
                    .toBranch(newBranch)
                    .expectedHash(Optional.of(initialHash))
                    .keepIndividualCommits(individualCommits)
                    .fetchAdditionalInfo(true)
                    .build());

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
    // The branch was fast-forwarded to firstCommit, so no commits added
    soft.assertThat(mergeResult.getCreatedCommits()).isEmpty();
    soft.assertThat(mergeResult.getDetails())
        .containsKeys(ContentKey.of("t1"), ContentKey.of("t2"), ContentKey.of("t3"));
    soft.assertThat(mergeResult)
        // compare "effective" merge-result with re-constructed merge-result
        .isEqualTo(
            MergeResult.<Commit>builder()
                .resultType(ResultType.MERGE)
                .sourceRef(MAIN_BRANCH)
                .wasApplied(true)
                .wasSuccessful(true)
                .commonAncestor(initialHash)
                .resultantTargetHash(head)
                .targetBranch(newBranch)
                .effectiveTargetHash(initialHash)
                .expectedHash(initialHash)
                .addAllSourceCommits(mergeResult.getSourceCommits())
                .addAllCreatedCommits(mergeResult.getCreatedCommits())
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

    MergeResult<Commit> result =
        store()
            .merge(
                MergeOp.builder()
                    .fromRef(MAIN_BRANCH)
                    .fromHash(firstCommit)
                    .toBranch(newBranch)
                    .updateCommitMetadata(metadataRewriter)
                    .expectedHash(Optional.of(initialHash))
                    .keepIndividualCommits(individualCommits)
                    .build());
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
    soft.assertThat(store().hashOnReference(newBranch, Optional.empty(), emptyList()))
        .isEqualTo(firstCommit);
    soft.assertThat(result.getCreatedCommits()).isEmpty(); // no new commits

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

    doMergeIntoEmpty(
        individualCommits, newBranch, metadataRewriter, false /* because of metadata rewrite */);

    // modify the commit meta, will generate new commits and therefore new commit hashes
    soft.assertThat(store().hashOnReference(newBranch, Optional.empty(), emptyList()))
        .isNotEqualTo(thirdCommit);

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
                MergeOp.builder()
                    .fromRef(MAIN_BRANCH)
                    .fromHash(thirdCommit)
                    .toBranch(newBranch)
                    .updateCommitMetadata(metadataRewriter)
                    .keepIndividualCommits(individualCommits)
                    .build());

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
    if (individualCommits) {
      soft.assertThat(result.getCreatedCommits())
          .hasSize(3)
          .satisfiesExactly(
              commit -> {
                soft.assertThat(commit.getParentHash()).isEqualTo(newCommit);
                soft.assertThat(commit.getCommitMeta().getMessage()).isEqualTo("First Commit");
              },
              commit -> {
                soft.assertThat(commit.getParentHash())
                    .isEqualTo(result.getCreatedCommits().get(0).getHash());
                soft.assertThat(commit.getCommitMeta().getMessage()).isEqualTo("Second Commit");
              },
              commit -> {
                soft.assertThat(commit.getParentHash())
                    .isEqualTo(result.getCreatedCommits().get(1).getHash());
                soft.assertThat(commit.getCommitMeta().getMessage()).isEqualTo("Third Commit");
              });
    } else {
      soft.assertThat(result.getCreatedCommits())
          .singleElement()
          .extracting(Commit::getParentHash)
          .isEqualTo(newCommit);
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

    CommitResult<Commit> etl1 =
        store()
            .commit(
                etl,
                Optional.empty(),
                CommitMeta.fromMessage("commit 1"),
                singletonList(Put.of(key, VALUE_1)));
    Content v = store().getValue(etl, key).content();
    MergeResult<Commit> mergeResult1 =
        store()
            .merge(
                MergeOp.builder()
                    .fromRef(etl)
                    .fromHash(store().hashOnReference(etl, Optional.empty(), emptyList()))
                    .toBranch(review)
                    .keepIndividualCommits(individualCommits)
                    .build());
    soft.assertThat(mergeResult1.getResultantTargetHash()).isEqualTo(etl1.getCommitHash());
    soft.assertThat(mergeResult1.getCreatedCommits()).isEmpty(); // fast-forward, so nothing added

    CommitResult<Commit> etl2 =
        store()
            .commit(
                etl,
                Optional.empty(),
                CommitMeta.fromMessage("commit 2"),
                singletonList(Put.of(key, VALUE_2.withId(v.getId()))));
    MergeResult<Commit> mergeResult2 =
        store()
            .merge(
                MergeOp.builder()
                    .fromRef(etl)
                    .fromHash(store().hashOnReference(etl, Optional.empty(), emptyList()))
                    .toBranch(review)
                    .keepIndividualCommits(individualCommits)
                    .build());
    soft.assertThat(mergeResult2.getResultantTargetHash()).isEqualTo(etl2.getCommitHash());
    soft.assertThat(mergeResult2.getCreatedCommits()).isEmpty(); // fast-forward, so nothing added

    soft.assertThat(contentWithoutId(store().getValue(review, key))).isEqualTo(VALUE_2);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  protected void mergeWithCommonAncestor(boolean individualCommits) throws VersionStoreException {
    final BranchName newBranch = BranchName.of("bar_2");
    store().create(newBranch, Optional.of(firstCommit));

    final Hash newCommit = commit("Unrelated commit").put("t5", V_5_1).toBranch(newBranch);

    MetadataRewriter<CommitMeta> metadataRewriter = createMetadataRewriter("");

    MergeResult<Commit> result =
        store()
            .merge(
                MergeOp.builder()
                    .fromRef(MAIN_BRANCH)
                    .fromHash(thirdCommit)
                    .toBranch(newBranch)
                    .updateCommitMetadata(metadataRewriter)
                    .keepIndividualCommits(individualCommits)
                    .build());
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

    final List<Commit> commits = commitsList(newBranch, true);
    if (individualCommits) {
      soft.assertThat(commits)
          .hasSize(5)
          .satisfiesExactly(
              c -> assertThat(c.getCommitMeta().getMessage()).isEqualTo("Third Commit"),
              c -> assertThat(c.getCommitMeta().getMessage()).isEqualTo("Second Commit"),
              c -> assertThat(c.getHash()).isEqualTo(newCommit),
              c -> assertThat(c.getHash()).isEqualTo(firstCommit),
              c -> assertThat(c.getHash()).isEqualTo(initialHash));
      soft.assertThat(result.getCreatedCommits())
          .hasSize(2)
          .satisfiesExactly(
              c -> assertThat(c).isEqualTo(commits.get(1)),
              c -> assertThat(c).isEqualTo(commits.get(0)));

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

      soft.assertThat(result.getCreatedCommits()).singleElement().isEqualTo(commits.get(0));

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
                Arrays.asList(Put.of(conflictingKey2, VALUE_3), Put.of(key4, VALUE_6)))
            .getCommitHash();
    Hash mergeFromHash =
        store()
            .commit(
                mergeFrom,
                Optional.empty(),
                CommitMeta.fromMessage("commit 4"),
                singletonList(Put.of(conflictingKey2, VALUE_4)))
            .getCommitHash();

    StorageAssertions checkpoint = storageCheckpoint();
    // "Plain" merge attempt - all keys default to MergeBehavior.NORMAL
    soft.assertThatThrownBy(
            () ->
                store()
                    .merge(
                        MergeOp.builder()
                            .fromRef(mergeFrom)
                            .fromHash(mergeFromHash)
                            .toBranch(mergeInto)
                            .keepIndividualCommits(individualCommits)
                            .dryRun(dryRun)
                            .build()))
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
                        MergeOp.builder()
                            .fromRef(mergeFrom)
                            .fromHash(mergeFromHash)
                            .toBranch(mergeInto)
                            .keepIndividualCommits(individualCommits)
                            .putMergeKeyBehaviors(
                                conflictingKey2,
                                MergeKeyBehavior.of(conflictingKey2, MergeBehavior.DROP))
                            .dryRun(dryRun)
                            .build()))
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
                        MergeOp.builder()
                            .fromRef(mergeFrom)
                            .fromHash(mergeFromHash)
                            .toBranch(mergeInto)
                            .keepIndividualCommits(individualCommits)
                            .putMergeKeyBehaviors(
                                conflictingKey1,
                                MergeKeyBehavior.of(conflictingKey1, MergeBehavior.NORMAL))
                            .defaultMergeBehavior(MergeBehavior.DROP)
                            .dryRun(dryRun)
                            .build()))
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
                        MergeOp.builder()
                            .fromRef(mergeFrom)
                            .fromHash(mergeFromHash)
                            .toBranch(mergeInto)
                            .keepIndividualCommits(individualCommits)
                            .putMergeKeyBehaviors(
                                conflictingKey1,
                                MergeKeyBehavior.of(conflictingKey1, MergeBehavior.FORCE))
                            .dryRun(dryRun)
                            .build()))
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
    MergeResult<Commit> result =
        store()
            .merge(
                MergeOp.builder()
                    .fromRef(mergeFrom)
                    .fromHash(mergeFromHash)
                    .toBranch(mergeInto)
                    .updateCommitMetadata(createMetadataRewriter(", merge-force-1"))
                    .keepIndividualCommits(individualCommits)
                    .putMergeKeyBehaviors(
                        conflictingKey1, MergeKeyBehavior.of(conflictingKey1, MergeBehavior.FORCE))
                    .putMergeKeyBehaviors(
                        conflictingKey2, MergeKeyBehavior.of(conflictingKey2, MergeBehavior.DROP))
                    .build());
    soft.assertThat(
            contentsWithoutId(
                store.getValues(
                    mergeIntoHeadSupplier.get(),
                    Arrays.asList(conflictingKey1, conflictingKey2, key3, key4))))
        .containsEntry(conflictingKey1, VALUE_2) // value as in "mergeFrom"
        .containsEntry(conflictingKey2, VALUE_3) // value as in "mergeInto"
        .containsEntry(key3, VALUE_5)
        .containsEntry(key4, VALUE_6);

    soft.assertThat(result.getCreatedCommits())
        .singleElement()
        .satisfies(
            commit -> {
              if (individualCommits) {
                soft.assertThat(commit.getCommitMeta().getMessage())
                    .isEqualTo("commit 2, merge-force-1");
                // commit 4 was skipped because empty after drop of conflictingKey2
              } else {
                soft.assertThat(commit.getCommitMeta().getMessage())
                    .contains("commit 2, merge-force-1")
                    .contains("commit 4, merge-force-1");
              }
              soft.assertThat(commit.getParentHash()).isEqualTo(mergeIntoHead);
              soft.assertThat(commit.getOperations())
                  .satisfiesExactlyInAnyOrder(
                      op -> {
                        soft.assertThat(op.getKey()).isEqualTo(conflictingKey1);
                        soft.assertThat(contentWithoutId(((Put) op).getValue())).isEqualTo(VALUE_2);
                      },
                      op -> {
                        soft.assertThat(op.getKey()).isEqualTo(key3);
                        soft.assertThat(contentWithoutId(((Put) op).getValue())).isEqualTo(VALUE_5);
                      });
            });

    // reset merge-into branch
    store().assign(mergeInto, Optional.empty(), mergeIntoHead);

    // Merge with force-merge of conflictingKey1 + drop of conflictingKey2
    MergeResult<Commit> result2 =
        store()
            .merge(
                MergeOp.builder()
                    .fromRef(mergeFrom)
                    .fromHash(mergeFromHash)
                    .toBranch(mergeInto)
                    .updateCommitMetadata(createMetadataRewriter(", merge-all-force"))
                    .keepIndividualCommits(individualCommits)
                    .defaultMergeBehavior(MergeBehavior.FORCE)
                    .build());
    soft.assertThat(
            contentsWithoutId(
                store.getValues(
                    mergeIntoHeadSupplier.get(),
                    Arrays.asList(conflictingKey1, conflictingKey2, key3, key4))))
        .containsEntry(conflictingKey1, VALUE_2) // value as in "mergeFrom"
        .containsEntry(conflictingKey2, VALUE_4) // value as in "mergeFrom"
        .containsEntry(key3, VALUE_5)
        .containsEntry(key4, VALUE_6);

    if (individualCommits) {
      soft.assertThat(result2.getCreatedCommits())
          .hasSize(2)
          .satisfiesExactly(
              commit -> {
                soft.assertThat(commit.getCommitMeta().getMessage())
                    .isEqualTo("commit 2, merge-all-force");
                soft.assertThat(commit.getParentHash()).isEqualTo(mergeIntoHead);
                soft.assertThat(commit.getOperations())
                    .satisfiesExactlyInAnyOrder(
                        op -> {
                          soft.assertThat(op.getKey()).isEqualTo(conflictingKey1);
                          soft.assertThat(contentWithoutId(((Put) op).getValue()))
                              .isEqualTo(VALUE_2);
                        },
                        op -> {
                          soft.assertThat(op.getKey()).isEqualTo(key3);
                          soft.assertThat(contentWithoutId(((Put) op).getValue()))
                              .isEqualTo(VALUE_5);
                        });
              },
              commit -> {
                soft.assertThat(commit.getCommitMeta().getMessage())
                    .isEqualTo("commit 4, merge-all-force");
                soft.assertThat(commit.getParentHash())
                    .isEqualTo(result2.getCreatedCommits().get(0).getHash());
                soft.assertThat(commit.getOperations())
                    .singleElement()
                    .satisfies(
                        op -> {
                          soft.assertThat(op.getKey()).isEqualTo(conflictingKey2);
                          soft.assertThat(contentWithoutId(((Put) op).getValue()))
                              .isEqualTo(VALUE_4);
                        });
              });
    } else {
      soft.assertThat(result2.getCreatedCommits())
          .singleElement()
          .satisfies(
              commit -> {
                soft.assertThat(commit.getCommitMeta().getMessage())
                    .contains("commit 2, merge-all-force")
                    .contains("commit 4, merge-all-force");
                soft.assertThat(commit.getParentHash()).isEqualTo(mergeIntoHead);
                soft.assertThat(commit.getOperations())
                    .satisfiesExactlyInAnyOrder(
                        op -> {
                          soft.assertThat(op.getKey()).isEqualTo(conflictingKey1);
                          soft.assertThat(contentWithoutId(((Put) op).getValue()))
                              .isEqualTo(VALUE_2);
                        },
                        op -> {
                          soft.assertThat(op.getKey()).isEqualTo(conflictingKey2);
                          soft.assertThat(contentWithoutId(((Put) op).getValue()))
                              .isEqualTo(VALUE_4);
                        },
                        op -> {
                          soft.assertThat(op.getKey()).isEqualTo(key3);
                          soft.assertThat(contentWithoutId(((Put) op).getValue()))
                              .isEqualTo(VALUE_5);
                        });
              });
    }
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
                        MergeOp.builder()
                            .fromRef(MAIN_BRANCH)
                            .fromHash(thirdCommit)
                            .toBranch(newBranch)
                            .expectedHash(Optional.of(initialHash))
                            .keepIndividualCommits(individualCommits)
                            .dryRun(dryRun)
                            .build()))
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
                        MergeOp.builder()
                            .fromRef(MAIN_BRANCH)
                            .fromHash(thirdCommit)
                            .toBranch(newBranch)
                            .expectedHash(Optional.of(initialHash))
                            .keepIndividualCommits(individualCommits)
                            .dryRun(dryRun)
                            .build()))
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
                        MergeOp.builder()
                            .fromRef(MAIN_BRANCH)
                            .fromHash(Hash.of("1234567890abcdef"))
                            .toBranch(newBranch)
                            .expectedHash(Optional.of(initialHash))
                            .keepIndividualCommits(individualCommits)
                            .dryRun(dryRun)
                            .build()))
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
                .updateCommitMetadata(createMetadataRewriter(", merge-drop"))
                .keepIndividualCommits(individualCommits)
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

    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                store()
                    .merge(
                        MergeOp.builder()
                            .fromRef(branch)
                            .fromHash(commit2)
                            .toBranch(branch)
                            .expectedHash(Optional.of(commit1))
                            .dryRun(dryRun)
                            .build()));

    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                store()
                    .merge(
                        MergeOp.builder()
                            .fromRef(branch)
                            .fromHash(commit1)
                            .toBranch(branch)
                            .expectedHash(Optional.of(commit2))
                            .dryRun(dryRun)
                            .build()));
  }
}
