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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.projectnessie.versioned.testworker.CommitMessage.commitMessage;
import static org.projectnessie.versioned.testworker.OnRefOnly.newOnRef;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Commit;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.MetadataRewriter;
import org.projectnessie.versioned.Put;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.VersionStoreException;
import org.projectnessie.versioned.testworker.BaseContent;
import org.projectnessie.versioned.testworker.CommitMessage;
import org.projectnessie.versioned.testworker.OnRefOnly;

public abstract class AbstractMerge extends AbstractNestedVersionStore {

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

  protected AbstractMerge(VersionStore<BaseContent, CommitMessage, BaseContent.Type> store) {
    super(store);
  }

  private Hash initialHash;
  private Hash firstCommit;
  private Hash secondCommit;
  private Hash thirdCommit;
  private List<Commit<CommitMessage, BaseContent>> commits;

  @BeforeEach
  protected void setupCommits() throws VersionStoreException {
    final BranchName branch = BranchName.of("foo");
    store().create(branch, Optional.empty());

    // The default common ancestor for all merge-tests.
    // The spec for 'VersionStore.merge' mentions "(...) until we arrive at a common ancestor",
    // but old implementations allowed a merge even if the "merge-from" and "merge-to" have no
    // common ancestor and did merge "everything" from the "merge-from" into "merge-to".
    // Note: "beginning-of-time" (aka creating a branch without specifying a "create-from")
    // creates a new commit-tree that is decoupled from other commit-trees.
    initialHash = commit("Default common ancestor").toBranch(branch);

    firstCommit =
        commit("First Commit").put("t1", V_1_1).put("t2", V_2_1).put("t3", V_3_1).toBranch(branch);
    secondCommit =
        commit("Second Commit")
            .put("t1", V_1_2)
            .delete("t2")
            .delete("t3")
            .put("t4", V_4_1)
            .toBranch(branch);
    thirdCommit = commit("Third Commit").put("t2", V_2_2).unchanged("t4").toBranch(branch);

    commits = commitsList(branch, false).subList(0, 3);
  }

  private MetadataRewriter<CommitMessage> createMetadataRewriter(String suffix) {
    return new MetadataRewriter<CommitMessage>() {
      @Override
      public CommitMessage rewriteSingle(CommitMessage metadata) {
        return CommitMessage.commitMessage(metadata.getMessage() + suffix);
      }

      @Override
      public CommitMessage squash(List<CommitMessage> metadata) {
        return CommitMessage.commitMessage(
            metadata.stream()
                .map(cm -> cm.getMessage() + suffix)
                .collect(Collectors.joining("\n-----------------------------------\n")));
      }
    };
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  protected void mergeIntoEmptyBranch3Commits(boolean individualCommits)
      throws VersionStoreException {
    final BranchName newBranch = BranchName.of("mergeIntoEmptyBranch3Commits");
    store().create(newBranch, Optional.of(initialHash));

    MetadataRewriter<CommitMessage> metadataRewriter = createMetadataRewriter("");

    store()
        .merge(
            thirdCommit, newBranch, Optional.of(initialHash), metadataRewriter, individualCommits);
    assertThat(
            store()
                .getValues(
                    newBranch,
                    Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"), Key.of("t4"))))
        .containsExactlyInAnyOrderEntriesOf(
            ImmutableMap.of(
                Key.of("t1"), V_1_2,
                Key.of("t2"), V_2_2,
                Key.of("t4"), V_4_1));

    if (individualCommits) {
      // not modifying commit meta, will just "fast forward"
      assertThat(store().hashOnReference(newBranch, Optional.empty())).isEqualTo(thirdCommit);

      assertCommitMeta(commitsList(newBranch, false).subList(0, 3), commits, metadataRewriter);
    } else {
      // must modify commit meta, it's more than one commit
      assertThat(store().hashOnReference(newBranch, Optional.empty())).isNotEqualTo(thirdCommit);

      assertThat(commitsList(newBranch, false))
          .first()
          .extracting(Commit::getCommitMeta)
          .extracting(CommitMessage::getMessage)
          .asString()
          .contains(
              commits.stream()
                  .map(Commit::getCommitMeta)
                  .map(CommitMessage::getMessage)
                  .toArray(String[]::new));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  protected void mergeIntoEmptyBranch1Commit(boolean individualCommits)
      throws VersionStoreException {
    final BranchName newBranch = BranchName.of("mergeIntoEmptyBranch1Commit");
    store().create(newBranch, Optional.of(initialHash));

    MetadataRewriter<CommitMessage> metadataRewriter = createMetadataRewriter("");

    store()
        .merge(
            firstCommit, newBranch, Optional.of(initialHash), metadataRewriter, individualCommits);
    assertThat(
            store()
                .getValues(
                    newBranch,
                    Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"), Key.of("t4"))))
        .containsExactlyInAnyOrderEntriesOf(
            ImmutableMap.of(
                Key.of("t1"), V_1_1,
                Key.of("t2"), V_2_1,
                Key.of("t3"), V_3_1));

    // not modifying commit meta, will just "fast forward"
    assertThat(store().hashOnReference(newBranch, Optional.empty())).isEqualTo(firstCommit);

    assertCommitMeta(
        commitsList(newBranch, false).subList(0, 1), commits.subList(2, 3), metadataRewriter);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  protected void mergeIntoEmptyBranchModifying(boolean individualCommits)
      throws VersionStoreException {
    final BranchName newBranch = BranchName.of("mergeIntoEmptyBranchModifying");
    store().create(newBranch, Optional.of(initialHash));

    MetadataRewriter<CommitMessage> metadataRewriter = createMetadataRewriter(", merged");

    store()
        .merge(
            thirdCommit, newBranch, Optional.of(initialHash), metadataRewriter, individualCommits);
    assertThat(
            store()
                .getValues(
                    newBranch,
                    Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"), Key.of("t4"))))
        .containsExactlyInAnyOrderEntriesOf(
            ImmutableMap.of(
                Key.of("t1"), V_1_2,
                Key.of("t2"), V_2_2,
                Key.of("t4"), V_4_1));

    // modify the commit meta, will generate new commits and therefore new commit hashes
    assertThat(store().hashOnReference(newBranch, Optional.empty())).isNotEqualTo(thirdCommit);

    if (individualCommits) {
      assertCommitMeta(commitsList(newBranch, false).subList(0, 3), commits, metadataRewriter);
    } else {
      assertThat(commitsList(newBranch, false))
          .first()
          .extracting(Commit::getCommitMeta)
          .extracting(CommitMessage::getMessage)
          .asString()
          .contains(
              commits.stream()
                  .map(Commit::getCommitMeta)
                  .map(CommitMessage::getMessage)
                  .toArray(String[]::new));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  protected void mergeIntoNonConflictingBranch(boolean individualCommits)
      throws VersionStoreException {
    final BranchName newBranch = BranchName.of("bar_2");
    store().create(newBranch, Optional.of(initialHash));
    final Hash newCommit = commit("Unrelated commit").put("t5", V_5_1).toBranch(newBranch);

    MetadataRewriter<CommitMessage> metadataRewriter = createMetadataRewriter("");

    store().merge(thirdCommit, newBranch, Optional.empty(), metadataRewriter, individualCommits);
    assertThat(
            store()
                .getValues(
                    newBranch,
                    Arrays.asList(
                        Key.of("t1"), Key.of("t2"), Key.of("t3"), Key.of("t4"), Key.of("t5"))))
        .containsExactlyInAnyOrderEntriesOf(
            ImmutableMap.of(
                Key.of("t1"), V_1_2,
                Key.of("t2"), V_2_2,
                Key.of("t4"), V_4_1,
                Key.of("t5"), V_5_1));

    final List<Commit<CommitMessage, BaseContent>> commits = commitsList(newBranch, false);
    if (individualCommits) {
      assertThat(commits)
          .satisfiesExactly(
              c0 ->
                  assertThat(c0)
                      .extracting(Commit::getCommitMeta)
                      .isEqualTo(commitMessage("Third Commit")),
              c1 ->
                  assertThat(c1)
                      .extracting(Commit::getCommitMeta)
                      .isEqualTo(commitMessage("Second Commit")),
              c2 ->
                  assertThat(c2)
                      .extracting(Commit::getCommitMeta)
                      .isEqualTo(commitMessage("First Commit")),
              c3 -> assertThat(c3).extracting(Commit::getHash).isEqualTo(newCommit),
              c4 -> assertThat(c4).extracting(Commit::getHash).isEqualTo(initialHash));
    } else {
      assertThat(commits)
          .satisfiesExactly(
              c0 ->
                  assertThat(c0)
                      .extracting(Commit::getCommitMeta)
                      .extracting(CommitMessage::getMessage)
                      .asString()
                      .contains("Third Commit", "Second Commit", "First Commit"),
              c3 -> assertThat(c3).extracting(Commit::getHash).isEqualTo(newCommit),
              c4 -> assertThat(c4).extracting(Commit::getHash).isEqualTo(initialHash));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  protected void nonEmptyFastForwardMerge(boolean individualCommits) throws VersionStoreException {
    final Key key = Key.of("t1");
    final BranchName etl = BranchName.of("etl");
    final BranchName review = BranchName.of("review");
    store().create(etl, Optional.of(initialHash));
    store().create(review, Optional.of(initialHash));

    MetadataRewriter<CommitMessage> metadataRewriter = createMetadataRewriter("");
    store()
        .commit(
            etl,
            Optional.empty(),
            commitMessage("commit 1"),
            Collections.singletonList(Put.of(key, VALUE_1)));
    store()
        .merge(
            store().hashOnReference(etl, Optional.empty()),
            review,
            Optional.empty(),
            metadataRewriter,
            individualCommits);
    store()
        .commit(
            etl,
            Optional.empty(),
            commitMessage("commit 2"),
            Collections.singletonList(Put.of(key, VALUE_2)));
    store()
        .merge(
            store().hashOnReference(etl, Optional.empty()),
            review,
            Optional.empty(),
            metadataRewriter,
            individualCommits);
    assertEquals(store().getValue(review, key), VALUE_2);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  protected void mergeWithCommonAncestor(boolean individualCommits) throws VersionStoreException {
    final BranchName newBranch = BranchName.of("bar_2");
    store().create(newBranch, Optional.of(firstCommit));

    final Hash newCommit = commit("Unrelated commit").put("t5", V_5_1).toBranch(newBranch);

    MetadataRewriter<CommitMessage> metadataRewriter = createMetadataRewriter("");

    store().merge(thirdCommit, newBranch, Optional.empty(), metadataRewriter, individualCommits);
    assertThat(
            store()
                .getValues(
                    newBranch,
                    Arrays.asList(
                        Key.of("t1"), Key.of("t2"), Key.of("t3"), Key.of("t4"), Key.of("t5"))))
        .containsExactlyInAnyOrderEntriesOf(
            ImmutableMap.of(
                Key.of("t1"), V_1_2,
                Key.of("t2"), V_2_2,
                Key.of("t4"), V_4_1,
                Key.of("t5"), V_5_1));

    final List<Commit<CommitMessage, BaseContent>> commits = commitsList(newBranch, false);
    if (individualCommits) {
      assertThat(commits)
          .hasSize(5)
          .satisfiesExactly(
              c -> assertThat(c.getCommitMeta().getMessage()).isEqualTo("Third Commit"),
              c -> assertThat(c.getCommitMeta().getMessage()).isEqualTo("Second Commit"),
              c -> assertThat(c.getHash()).isEqualTo(newCommit),
              c -> assertThat(c.getHash()).isEqualTo(firstCommit),
              c -> assertThat(c.getHash()).isEqualTo(initialHash));
    } else {
      assertThat(commits)
          .hasSize(4)
          .satisfiesExactly(
              c ->
                  assertThat(c.getCommitMeta().getMessage())
                      .contains("Second Commit", "Third Commit"),
              c -> assertThat(c.getHash()).isEqualTo(newCommit),
              c -> assertThat(c.getHash()).isEqualTo(firstCommit),
              c -> assertThat(c.getHash()).isEqualTo(initialHash));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  protected void mergeWithConflictingKeys(boolean individualCommits) throws VersionStoreException {
    final BranchName foo = BranchName.of("foofoo");
    final BranchName bar = BranchName.of("barbar");
    store().create(foo, Optional.of(this.initialHash));
    store().create(bar, Optional.of(this.initialHash));

    // we're essentially modifying the same key on both branches and then merging one branch into
    // the other and expect a conflict
    Key key1 = Key.of("some_key1");
    Key key2 = Key.of("some_key2");

    store()
        .commit(
            foo,
            Optional.empty(),
            commitMessage("commit 1"),
            Collections.singletonList(Put.of(key1, VALUE_1)));
    store()
        .commit(
            bar,
            Optional.empty(),
            commitMessage("commit 2"),
            Collections.singletonList(Put.of(key1, VALUE_2)));
    store()
        .commit(
            foo,
            Optional.empty(),
            commitMessage("commit 3"),
            Collections.singletonList(Put.of(key2, VALUE_3)));
    Hash barHash =
        store()
            .commit(
                bar,
                Optional.empty(),
                commitMessage("commit 4"),
                Collections.singletonList(Put.of(key2, VALUE_4)));

    assertThatThrownBy(
            () ->
                store()
                    .merge(
                        barHash,
                        foo,
                        Optional.empty(),
                        createMetadataRewriter(""),
                        individualCommits))
        .isInstanceOf(ReferenceConflictException.class)
        .hasMessageContaining("The following keys have been changed in conflict:")
        .hasMessageContaining(key1.toString())
        .hasMessageContaining(key2.toString());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  protected void mergeIntoConflictingBranch(boolean individualCommits)
      throws VersionStoreException {
    final BranchName newBranch = BranchName.of("bar_3");
    store().create(newBranch, Optional.of(initialHash));
    commit("Another commit").put("t1", V_1_4).toBranch(newBranch);

    assertThrows(
        ReferenceConflictException.class,
        () ->
            store()
                .merge(
                    thirdCommit,
                    newBranch,
                    Optional.of(initialHash),
                    createMetadataRewriter(""),
                    individualCommits));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  protected void mergeIntoNonExistingBranch(boolean individualCommits) {
    final BranchName newBranch = BranchName.of("bar_5");
    assertThrows(
        ReferenceNotFoundException.class,
        () ->
            store()
                .merge(
                    thirdCommit,
                    newBranch,
                    Optional.of(initialHash),
                    createMetadataRewriter(""),
                    individualCommits));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  protected void mergeIntoNonExistingReference(boolean individualCommits)
      throws VersionStoreException {
    final BranchName newBranch = BranchName.of("bar_6");
    store().create(newBranch, Optional.of(initialHash));
    assertThrows(
        ReferenceNotFoundException.class,
        () ->
            store()
                .merge(
                    Hash.of("1234567890abcdef"),
                    newBranch,
                    Optional.of(initialHash),
                    createMetadataRewriter(""),
                    individualCommits));
  }
}
