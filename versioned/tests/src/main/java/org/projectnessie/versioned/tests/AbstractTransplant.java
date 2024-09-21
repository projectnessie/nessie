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
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.projectnessie.versioned.testworker.OnRefOnly.newOnRef;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.MergeBehavior;
import org.projectnessie.model.MergeKeyBehavior;
import org.projectnessie.model.Operation.Delete;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Commit;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.TransplantResult;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.VersionStore.TransplantOp;
import org.projectnessie.versioned.VersionStoreException;
import org.projectnessie.versioned.paging.PaginationIterator;
import org.projectnessie.versioned.testworker.OnRefOnly;

@ExtendWith(SoftAssertionsExtension.class)
public abstract class AbstractTransplant extends AbstractNestedVersionStore {

  @InjectSoftAssertions protected SoftAssertions soft;

  private static final String T_1 = "t1";
  private static final String T_2 = "t2";
  private static final String T_3 = "t3";
  private static final String T_4 = "t4";
  private static final String T_5 = "t5";

  private static final OnRefOnly V_1_1 = newOnRef("v1_1");
  private static final OnRefOnly V_1_2 = newOnRef("v1_2");
  private static final OnRefOnly V_1_4 = newOnRef("v1_4");
  private static final OnRefOnly V_2_2 = newOnRef("v2_2");
  private static final OnRefOnly V_2_1 = newOnRef("v2_1");
  private static final OnRefOnly V_3_1 = newOnRef("v3_1");
  private static final OnRefOnly V_4_1 = newOnRef("v4_1");
  private static final OnRefOnly V_5_1 = newOnRef("v5_1");

  private static final BranchName sourceBranch = BranchName.of("foo");

  protected AbstractTransplant(VersionStore store) {
    super(store);
  }

  private Hash initialHash;
  private Hash firstCommit;
  private Hash secondCommit;
  private Hash thirdCommit;
  private List<Commit> commits;

  @BeforeEach
  protected void setupCommits() throws VersionStoreException {
    store().create(sourceBranch, Optional.empty());

    initialHash = store().hashOnReference(sourceBranch, Optional.empty(), emptyList());

    firstCommit =
        commit("Initial Commit")
            .put(T_1, V_1_1)
            .put(T_2, V_2_1)
            .put(T_3, V_3_1)
            .toBranch(sourceBranch);

    Content t1 =
        requireNonNull(store().getValue(sourceBranch, ContentKey.of("t1"), false).content());

    secondCommit =
        commit("Second Commit")
            .put("t1", V_1_2.withId(t1.getId()))
            .delete(T_2)
            .delete(T_3)
            .put(T_4, V_4_1)
            .toBranch(sourceBranch);

    thirdCommit = commit("Third Commit").put(T_2, V_2_2).unchanged(T_4).toBranch(sourceBranch);

    commits = commitsList(sourceBranch, false);
  }

  @Test
  protected void checkTransplantOnEmptyBranch() throws VersionStoreException {
    final BranchName newBranch = BranchName.of("bar_1");
    store().create(newBranch, Optional.empty()).getHash();

    TransplantResult result =
        store()
            .transplant(
                TransplantOp.builder()
                    .fromRef(sourceBranch)
                    .toBranch(newBranch)
                    .expectedHash(Optional.of(initialHash))
                    .addSequenceToTransplant(firstCommit, secondCommit, thirdCommit)
                    .build());

    soft.assertThat(result.getCreatedCommits()).isEmpty(); // fast-forward

    soft.assertThat(
            contentsWithoutId(
                store()
                    .getValues(
                        newBranch,
                        Arrays.asList(
                            ContentKey.of(T_1),
                            ContentKey.of(T_2),
                            ContentKey.of(T_3),
                            ContentKey.of(T_4)),
                        false)))
        .containsExactlyInAnyOrderEntriesOf(
            ImmutableMap.of(
                ContentKey.of(T_1), V_1_2,
                ContentKey.of(T_2), V_2_2,
                ContentKey.of(T_4), V_4_1));

    assertCommitMeta(soft, commitsList(newBranch, false).subList(0, 3), commits);
  }

  @Test
  protected void checkTransplantWithPreviousCommit() throws VersionStoreException {
    final BranchName newBranch = BranchName.of("bar_2");
    store().create(newBranch, Optional.empty());
    Hash targetHead = commit("Unrelated commit").put(T_5, V_5_1).toBranch(newBranch);

    TransplantResult result =
        store()
            .transplant(
                TransplantOp.builder()
                    .fromRef(sourceBranch)
                    .toBranch(newBranch)
                    .expectedHash(Optional.of(initialHash))
                    .addSequenceToTransplant(firstCommit, secondCommit, thirdCommit)
                    .build());

    soft.assertThat(result.getCreatedCommits())
        .hasSize(3)
        .satisfiesExactly(
            c -> {
              soft.assertThat(c.getParentHash()).isEqualTo(targetHead);
              soft.assertThat(c.getCommitMeta().getMessage()).isEqualTo("Initial Commit");
              soft.assertThat(c.getOperations())
                  .hasSize(3)
                  .satisfiesExactlyInAnyOrder(
                      o -> {
                        soft.assertThat(o).isInstanceOf(Put.class);
                        soft.assertThat(o.getKey()).isEqualTo(ContentKey.of(T_1));
                        soft.assertThat(contentWithoutId(((Put) o).getContent())).isEqualTo(V_1_1);
                      },
                      o -> {
                        soft.assertThat(o).isInstanceOf(Put.class);
                        soft.assertThat(o.getKey()).isEqualTo(ContentKey.of(T_2));
                        soft.assertThat(contentWithoutId(((Put) o).getContent())).isEqualTo(V_2_1);
                      },
                      o -> {
                        soft.assertThat(o).isInstanceOf(Put.class);
                        soft.assertThat(o.getKey()).isEqualTo(ContentKey.of(T_3));
                        soft.assertThat(contentWithoutId(((Put) o).getContent())).isEqualTo(V_3_1);
                      });
            },
            c -> {
              soft.assertThat(c.getParentHash())
                  .isEqualTo(result.getCreatedCommits().get(0).getHash());
              soft.assertThat(c.getCommitMeta().getMessage()).isEqualTo("Second Commit");
              soft.assertThat(c.getOperations())
                  .hasSize(4)
                  .satisfiesExactlyInAnyOrder(
                      o -> {
                        soft.assertThat(o).isInstanceOf(Put.class);
                        soft.assertThat(o.getKey()).isEqualTo(ContentKey.of(T_1));
                        soft.assertThat(contentWithoutId(((Put) o).getContent())).isEqualTo(V_1_2);
                      },
                      o ->
                          soft.assertThat(o)
                              .asInstanceOf(type(Delete.class))
                              .extracting(Delete::getKey)
                              .isEqualTo(ContentKey.of(T_2)),
                      o ->
                          soft.assertThat(o)
                              .asInstanceOf(type(Delete.class))
                              .extracting(Delete::getKey)
                              .isEqualTo(ContentKey.of(T_3)),
                      o -> {
                        soft.assertThat(o).isInstanceOf(Put.class);
                        soft.assertThat(o.getKey()).isEqualTo(ContentKey.of(T_4));
                        soft.assertThat(contentWithoutId(((Put) o).getContent())).isEqualTo(V_4_1);
                      });
            },
            c -> {
              soft.assertThat(c.getParentHash())
                  .isEqualTo(result.getCreatedCommits().get(1).getHash());
              soft.assertThat(c.getCommitMeta().getMessage()).isEqualTo("Third Commit");
              soft.assertThat(c.getOperations())
                  .hasSize(1)
                  .satisfiesExactlyInAnyOrder(
                      o -> {
                        soft.assertThat(o).isInstanceOf(Put.class);
                        soft.assertThat(o.getKey()).isEqualTo(ContentKey.of(T_2));
                        soft.assertThat(contentWithoutId(((Put) o).getContent())).isEqualTo(V_2_2);
                      }
                      // Unchanged operation not retained
                      );
            });

    assertThat(
            contentsWithoutId(
                store()
                    .getValues(
                        newBranch,
                        Arrays.asList(
                            ContentKey.of(T_1),
                            ContentKey.of(T_2),
                            ContentKey.of(T_3),
                            ContentKey.of(T_4),
                            ContentKey.of(T_5)),
                        false)))
        .containsExactlyInAnyOrderEntriesOf(
            ImmutableMap.of(
                ContentKey.of(T_1), V_1_2,
                ContentKey.of(T_2), V_2_2,
                ContentKey.of(T_4), V_4_1,
                ContentKey.of(T_5), V_5_1));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  protected void checkTransplantWithConflictingCommit(boolean dryRun) throws VersionStoreException {
    final BranchName newBranch = BranchName.of("bar_3");
    store().create(newBranch, Optional.empty());
    commit("Another commit").put(T_1, V_1_4).toBranch(newBranch);

    StorageAssertions checkpoint = storageCheckpoint();
    soft.assertThatThrownBy(
            () ->
                store()
                    .transplant(
                        TransplantOp.builder()
                            .fromRef(sourceBranch)
                            .toBranch(newBranch)
                            .expectedHash(Optional.of(initialHash))
                            .addSequenceToTransplant(firstCommit, secondCommit, thirdCommit)
                            .dryRun(dryRun)
                            .build()))
        .isInstanceOf(ReferenceConflictException.class);
    if (dryRun) {
      checkpoint.assertNoWrites();
    }
  }

  @Test
  protected void checkTransplantWithDelete() throws VersionStoreException {
    final BranchName newBranch = BranchName.of("bar_4");
    store().create(newBranch, Optional.empty());
    commit("Another commit").put(T_1, V_1_4).toBranch(newBranch);
    commit("Another commit").delete(T_1).toBranch(newBranch);

    store()
        .transplant(
            TransplantOp.builder()
                .fromRef(sourceBranch)
                .toBranch(newBranch)
                .expectedHash(Optional.of(initialHash))
                .addSequenceToTransplant(firstCommit, secondCommit, thirdCommit)
                .build());
    soft.assertThat(
            contentsWithoutId(
                store()
                    .getValues(
                        newBranch,
                        Arrays.asList(
                            ContentKey.of(T_1),
                            ContentKey.of(T_2),
                            ContentKey.of(T_3),
                            ContentKey.of(T_4)),
                        false)))
        .containsExactlyInAnyOrderEntriesOf(
            ImmutableMap.of(
                ContentKey.of(T_1), V_1_2,
                ContentKey.of(T_2), V_2_2,
                ContentKey.of(T_4), V_4_1));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  protected void checkTransplantOnNonExistingBranch(boolean dryRun) {
    final BranchName newBranch = BranchName.of("bar_5");
    StorageAssertions checkpoint = storageCheckpoint();
    soft.assertThatThrownBy(
            () ->
                store()
                    .transplant(
                        TransplantOp.builder()
                            .fromRef(sourceBranch)
                            .toBranch(newBranch)
                            .expectedHash(Optional.of(initialHash))
                            .addSequenceToTransplant(firstCommit, secondCommit, thirdCommit)
                            .dryRun(dryRun)
                            .build()))
        .isInstanceOf(ReferenceNotFoundException.class);
    checkpoint.assertNoWrites();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  protected void checkTransplantWithNonExistingCommit(boolean dryRun) throws VersionStoreException {
    final BranchName newBranch = BranchName.of("bar_6");
    store().create(newBranch, Optional.empty());
    StorageAssertions checkpoint = storageCheckpoint();
    soft.assertThatThrownBy(
            () ->
                store()
                    .transplant(
                        TransplantOp.builder()
                            .fromRef(sourceBranch)
                            .toBranch(newBranch)
                            .expectedHash(Optional.of(initialHash))
                            .addSequenceToTransplant(Hash.of("1234567890abcdef"))
                            .dryRun(dryRun)
                            .build()))
        .isInstanceOf(ReferenceNotFoundException.class);
    checkpoint.assertNoWrites();
  }

  @Test
  protected void checkTransplantWithNoExpectedHash() throws VersionStoreException {
    final BranchName newBranch = BranchName.of("bar_7");
    store().create(newBranch, Optional.empty());
    commit("Another commit").put(T_5, V_5_1).toBranch(newBranch);
    Content t5 = requireNonNull(store().getValue(newBranch, ContentKey.of(T_5), false).content());
    commit("Another commit").put(T_5, V_1_4.withId(t5.getId())).toBranch(newBranch);

    store()
        .transplant(
            TransplantOp.builder()
                .fromRef(sourceBranch)
                .toBranch(newBranch)
                .addSequenceToTransplant(firstCommit, secondCommit, thirdCommit)
                .build());
    soft.assertThat(
            contentsWithoutId(
                store()
                    .getValues(
                        newBranch,
                        Arrays.asList(
                            ContentKey.of(T_1),
                            ContentKey.of(T_2),
                            ContentKey.of(T_3),
                            ContentKey.of(T_4),
                            ContentKey.of(T_5)),
                        false)))
        .containsExactlyInAnyOrderEntriesOf(
            ImmutableMap.of(
                ContentKey.of(T_1), V_1_2,
                ContentKey.of(T_2), V_2_2,
                ContentKey.of(T_4), V_4_1,
                ContentKey.of(T_5), V_1_4));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  protected void checkTransplantWithCommitsInWrongOrder(boolean dryRun)
      throws VersionStoreException {
    final BranchName newBranch = BranchName.of("bar_8");
    store().create(newBranch, Optional.empty());

    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                store()
                    .transplant(
                        TransplantOp.builder()
                            .fromRef(sourceBranch)
                            .toBranch(newBranch)
                            .addSequenceToTransplant(secondCommit, firstCommit, thirdCommit)
                            .dryRun(dryRun)
                            .build()));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  protected void checkInvalidBranchHash(boolean dryRun) throws VersionStoreException {
    final BranchName anotherBranch = BranchName.of("bar");
    store().create(anotherBranch, Optional.empty());
    final Hash unrelatedCommit =
        commit("Another Commit")
            .put(T_1, V_1_1)
            .put(T_2, V_2_1)
            .put(T_3, V_3_1)
            .toBranch(anotherBranch);

    final BranchName newBranch = BranchName.of("bar_1");
    store().create(newBranch, Optional.empty());

    StorageAssertions checkpoint = storageCheckpoint();
    soft.assertThatThrownBy(
            () ->
                store()
                    .transplant(
                        TransplantOp.builder()
                            .fromRef(sourceBranch)
                            .toBranch(newBranch)
                            .expectedHash(Optional.of(unrelatedCommit))
                            .addSequenceToTransplant(firstCommit, secondCommit, thirdCommit)
                            .dryRun(dryRun)
                            .build()))
        .isInstanceOf(ReferenceNotFoundException.class);
    checkpoint.assertNoWrites();
  }

  @Test
  protected void transplantBasic() throws VersionStoreException {
    final BranchName newBranch = BranchName.of("bar_2");
    store().create(newBranch, Optional.empty());
    Hash targetHead = commit("Unrelated commit").put(T_5, V_5_1).toBranch(newBranch);

    TransplantResult result =
        store()
            .transplant(
                TransplantOp.builder()
                    .fromRef(sourceBranch)
                    .toBranch(newBranch)
                    .expectedHash(Optional.of(initialHash))
                    .addSequenceToTransplant(firstCommit, secondCommit)
                    .build());

    soft.assertThat(result.getCreatedCommits())
        .hasSize(2)
        .satisfiesExactly(
            c -> {
              soft.assertThat(c.getParentHash()).isEqualTo(targetHead);
              soft.assertThat(c.getCommitMeta().getMessage()).isEqualTo("Initial Commit");
            },
            c -> {
              soft.assertThat(c.getParentHash())
                  .isEqualTo(result.getCreatedCommits().get(0).getHash());
              soft.assertThat(c.getCommitMeta().getMessage()).isEqualTo("Second Commit");
            });
    soft.assertThat(
            contentsWithoutId(
                store()
                    .getValues(
                        newBranch,
                        Arrays.asList(ContentKey.of(T_1), ContentKey.of(T_4), ContentKey.of(T_5)),
                        false)))
        .containsExactlyInAnyOrderEntriesOf(
            ImmutableMap.of(
                ContentKey.of(T_1), V_1_2,
                ContentKey.of(T_4), V_4_1,
                ContentKey.of(T_5), V_5_1));
  }

  @Test
  protected void transplantEmptyCommit() throws VersionStoreException {
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
                singletonList(Put.of(key1, V_1_1)))
            .getCommitHash();

    targetHead =
        store()
            .commit(
                target,
                Optional.of(targetHead),
                CommitMeta.fromMessage("target 2"),
                singletonList(Put.of(key2, V_2_1)))
            .getCommitHash();

    // Add two commits to the source branch, with conflicting changes to key1 and key2

    Hash source1 =
        store()
            .commit(
                source,
                Optional.empty(),
                CommitMeta.fromMessage("source 1"),
                singletonList(Put.of(key1, V_1_2)))
            .getCommitHash();

    Hash source2 =
        store()
            .commit(
                source,
                Optional.of(source1),
                CommitMeta.fromMessage("source 2"),
                singletonList(Put.of(key2, V_2_2)))
            .getCommitHash();

    // Transplant the source branch into the target branch, with a drop of key1 and key2

    store()
        .transplant(
            TransplantOp.builder()
                .fromRef(source)
                .toBranch(target)
                .expectedHash(Optional.of(targetHead))
                .addSequenceToTransplant(source1, source2)
                .putMergeKeyBehaviors(key1, MergeKeyBehavior.of(key1, MergeBehavior.DROP))
                .putMergeKeyBehaviors(key2, MergeKeyBehavior.of(key2, MergeBehavior.DROP))
                .build());

    // No new commit should have been created in the target branch
    try (PaginationIterator<Commit> iterator = store().getCommits(target, true)) {
      Hash newTargetHead = iterator.next().getHash();
      assertThat(newTargetHead).isEqualTo(targetHead);
    }
  }
}
