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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.projectnessie.versioned.testworker.CommitMessage.commitMessage;
import static org.projectnessie.versioned.testworker.OnRefOnly.newOnRef;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.WithType;
import org.projectnessie.versioned.testworker.BaseContent;
import org.projectnessie.versioned.testworker.CommitMessage;
import org.projectnessie.versioned.testworker.OnRefOnly;

public abstract class AbstractCommits extends AbstractNestedVersionStore {

  private static final OnRefOnly V_1_1 = newOnRef("v1_1");
  private static final OnRefOnly V_1_2 = newOnRef("v1_2");
  private static final OnRefOnly V_1_3 = newOnRef("v1_3");
  private static final OnRefOnly V_2_1 = newOnRef("v2_1");
  private static final OnRefOnly V_2_2 = newOnRef("v2_2");
  private static final OnRefOnly V_3_1 = newOnRef("v3_1");
  private static final OnRefOnly V_3_2 = newOnRef("v3_2");
  private static final OnRefOnly V_4_1 = newOnRef("v4_1");
  private static final OnRefOnly NEW_v2_1 = newOnRef("new_v2_1");

  protected AbstractCommits(VersionStore<BaseContent, CommitMessage, BaseContent.Type> store) {
    super(store);
  }

  /*
   * Test:
   * - Create a new branch
   * - Add a commit to it
   * - Check that another commit with no operations can be added with the initial hash
   * - Check the commit can be listed
   * - Check that the commit can be deleted
   */
  @Test
  public void commitToBranch() throws Exception {
    final BranchName branch = BranchName.of("foo");

    final Hash createHash = store().create(branch, Optional.empty());
    final Hash initialHash = store().hashOnReference(branch, Optional.empty());
    assertEquals(createHash, initialHash);

    final Hash commitHash0 =
        store()
            .commit(
                branch,
                Optional.of(initialHash),
                commitMessage("Some commit"),
                Collections.emptyList());
    final Hash commitHash = store().hashOnReference(branch, Optional.empty());
    assertEquals(commitHash, commitHash0);

    assertThat(commitHash).isNotEqualTo(initialHash);
    store()
        .commit(
            branch,
            Optional.of(initialHash),
            commitMessage("Another commit"),
            Collections.emptyList());
    final Hash anotherCommitHash = store().hashOnReference(branch, Optional.empty());

    assertThat(commitsList(branch, false))
        .contains(commit(anotherCommitHash, "Another commit"), commit(commitHash, "Some commit"));
    assertThat(commitsList(commitHash, false)).contains(commit(commitHash, "Some commit"));

    assertThrows(
        ReferenceConflictException.class, () -> store().delete(branch, Optional.of(initialHash)));
    store().delete(branch, Optional.of(anotherCommitHash));
    assertThrows(
        ReferenceNotFoundException.class, () -> store().hashOnReference(branch, Optional.empty()));
    try (Stream<ReferenceInfo<CommitMessage>> str =
        store().getNamedRefs(GetNamedRefsParams.DEFAULT).filter(this::filterMainBranch)) {
      assertThat(str).isEmpty();
    }
    assertThrows(
        ReferenceNotFoundException.class, () -> store().delete(branch, Optional.of(commitHash)));
  }

  /*
   * Test:
   * - Create a new branch
   * - Add 3 commits in succession with no conflicts to it with put and delete operations
   * - Check commit metadata
   * - Check keys for each commit hash
   * - Check values for each commit hash
   */
  @Test
  public void commitSomeOperations() throws Exception {
    final BranchName branch = BranchName.of("foo");

    store().create(branch, Optional.empty());

    final Hash initialCommit =
        commit("Initial Commit")
            .put("t1", V_1_1)
            .put("t2", V_2_1)
            .put("t3", V_3_1)
            .toBranch(branch);

    final Hash secondCommit =
        commit("Second Commit")
            .put("t1", V_1_2)
            .delete("t2")
            .delete("t3")
            .put("t4", V_4_1)
            .toBranch(branch);

    final Hash thirdCommit =
        commit("Third Commit").put("t2", V_2_2).unchanged("t4").toBranch(branch);

    assertThat(commitsList(branch, false))
        .contains(
            commit(thirdCommit, "Third Commit"),
            commit(secondCommit, "Second Commit"),
            commit(initialCommit, "Initial Commit"));

    try (Stream<Key> keys = store().getKeys(branch).map(WithType::getValue)) {
      assertThat(keys).containsExactlyInAnyOrder(Key.of("t1"), Key.of("t2"), Key.of("t4"));
    }

    try (Stream<Key> keys = store().getKeys(secondCommit).map(WithType::getValue)) {
      assertThat(keys).containsExactlyInAnyOrder(Key.of("t1"), Key.of("t4"));
    }

    try (Stream<Key> keys = store().getKeys(initialCommit).map(WithType::getValue)) {
      assertThat(keys).containsExactlyInAnyOrder(Key.of("t1"), Key.of("t2"), Key.of("t3"));
    }

    assertThat(
            store()
                .getValues(
                    secondCommit,
                    Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"), Key.of("t4"))))
        .containsExactlyInAnyOrderEntriesOf(
            ImmutableMap.of(Key.of("t1"), V_1_2, Key.of("t4"), V_4_1));

    assertThat(
            store()
                .getValues(
                    initialCommit,
                    Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"), Key.of("t4"))))
        .containsExactlyInAnyOrderEntriesOf(
            ImmutableMap.of(
                Key.of("t1"), V_1_1,
                Key.of("t2"), V_2_1,
                Key.of("t3"), V_3_1));

    assertThat(store().getValue(branch, Key.of("t1"))).isEqualTo(V_1_2);
    assertThat(store().getValue(branch, Key.of("t2"))).isEqualTo(V_2_2);
    assertThat(store().getValue(branch, Key.of("t3"))).isNull();
    assertThat(store().getValue(branch, Key.of("t4"))).isEqualTo(V_4_1);

    assertThat(store().getValue(secondCommit, Key.of("t1"))).isEqualTo(V_1_2);
    assertThat(store().getValue(secondCommit, Key.of("t2"))).isNull();
    assertThat(store().getValue(secondCommit, Key.of("t3"))).isNull();
    assertThat(store().getValue(secondCommit, Key.of("t4"))).isEqualTo(V_4_1);

    assertThat(store().getValue(initialCommit, Key.of("t1"))).isEqualTo(V_1_1);
    assertThat(store().getValue(initialCommit, Key.of("t2"))).isEqualTo(V_2_1);
    assertThat(store().getValue(initialCommit, Key.of("t3"))).isEqualTo(V_3_1);
    assertThat(store().getValue(initialCommit, Key.of("t4"))).isNull();
  }

  /*
   * Test:
   * - Create a new branch
   * - Add a commit for 3 keys
   * - Add a commit based on initial commit for first key
   * - Add a commit based on initial commit for second key
   * - Add a commit based on initial commit for third  key
   * - Check commit metadata
   * - Check keys for each commit hash
   * - Check values for each commit hash
   */
  @Test
  public void commitNonConflictingOperations() throws Exception {
    final BranchName branch = BranchName.of("foo");

    store().create(branch, Optional.empty());

    final Hash initialCommit =
        commit("Initial Commit")
            .put("t1", V_1_1)
            .put("t2", V_2_1)
            .put("t3", V_3_1)
            .toBranch(branch);

    final Hash t1Commit =
        commit("T1 Commit").fromReference(initialCommit).put("t1", V_1_2).toBranch(branch);
    final Hash t2Commit =
        commit("T2 Commit").fromReference(initialCommit).delete("t2").toBranch(branch);
    final Hash t3Commit =
        commit("T3 Commit").fromReference(initialCommit).unchanged("t3").toBranch(branch);
    final Hash extraCommit =
        commit("Extra Commit")
            .fromReference(t1Commit)
            .put("t1", V_1_3)
            .put("t3", V_3_2)
            .toBranch(branch);
    final Hash newT2Commit =
        commit("New T2 Commit").fromReference(t2Commit).put("t2", NEW_v2_1).toBranch(branch);

    assertThat(commitsList(branch, false))
        .contains(
            commit(newT2Commit, "New T2 Commit"),
            commit(extraCommit, "Extra Commit"),
            commit(t3Commit, "T3 Commit"),
            commit(t2Commit, "T2 Commit"),
            commit(t1Commit, "T1 Commit"),
            commit(initialCommit, "Initial Commit"));

    try (Stream<Key> keys = store().getKeys(branch).map(WithType::getValue)) {
      assertThat(keys).containsExactlyInAnyOrder(Key.of("t1"), Key.of("t2"), Key.of("t3"));
    }

    assertThat(store().getValues(branch, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"))))
        .containsExactlyInAnyOrderEntriesOf(
            ImmutableMap.of(
                Key.of("t1"), V_1_3,
                Key.of("t2"), NEW_v2_1,
                Key.of("t3"), V_3_2));

    assertThat(
            store().getValues(newT2Commit, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"))))
        .containsExactlyInAnyOrderEntriesOf(
            ImmutableMap.of(
                Key.of("t1"), V_1_3,
                Key.of("t2"), NEW_v2_1,
                Key.of("t3"), V_3_2));

    assertThat(
            store().getValues(extraCommit, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"))))
        .containsExactlyInAnyOrderEntriesOf(
            ImmutableMap.of(
                Key.of("t1"), V_1_3,
                Key.of("t3"), V_3_2));

    assertThat(store().getValues(t3Commit, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"))))
        .containsExactlyInAnyOrderEntriesOf(
            ImmutableMap.of(
                Key.of("t1"), V_1_2,
                Key.of("t3"), V_3_1));

    assertThat(store().getValues(t2Commit, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"))))
        .containsExactlyInAnyOrderEntriesOf(
            ImmutableMap.of(
                Key.of("t1"), V_1_2,
                Key.of("t3"), V_3_1));

    assertThat(store().getValues(t1Commit, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"))))
        .containsExactlyInAnyOrderEntriesOf(
            ImmutableMap.of(
                Key.of("t1"), V_1_2,
                Key.of("t2"), V_2_1,
                Key.of("t3"), V_3_1));
  }

  /*
   * Test:
   * - Create a new branch
   * - Add a commit to create 2 keys
   * - Add a second commit to delete one key and add a new one
   * - Check that put operations against 1st commit for the 3 keys fail
   * - Check that delete operations against 1st commit for the 3 keys fail
   * - Check that unchanged operations against 1st commit for the 3 keys fail
   * - Check that branch state hasn't changed
   */
  @Test
  public void commitConflictingOperations() throws Exception {
    final BranchName branch = BranchName.of("foo");

    store().create(branch, Optional.empty());

    final Hash initialCommit =
        commit("Initial Commit").put("t1", V_1_1).put("t2", V_2_1).toBranch(branch);

    final Hash secondCommit =
        commit("Second Commit").put("t1", V_1_2).delete("t2").put("t3", V_3_1).toBranch(branch);

    assertThrows(
        ReferenceConflictException.class,
        () ->
            commit("Conflicting Commit")
                .fromReference(initialCommit)
                .put("t1", V_1_3)
                .toBranch(branch));
    assertThrows(
        ReferenceConflictException.class,
        () ->
            commit("Conflicting Commit")
                .fromReference(initialCommit)
                .put("t2", V_2_2)
                .toBranch(branch));
    assertThrows(
        ReferenceConflictException.class,
        () ->
            commit("Conflicting Commit")
                .fromReference(initialCommit)
                .put("t3", V_3_2)
                .toBranch(branch));

    assertThrows(
        ReferenceConflictException.class,
        () ->
            commit("Conflicting Commit")
                .fromReference(initialCommit)
                .delete("t1")
                .toBranch(branch));
    assertThrows(
        ReferenceConflictException.class,
        () ->
            commit("Conflicting Commit")
                .fromReference(initialCommit)
                .delete("t2")
                .toBranch(branch));
    assertThrows(
        ReferenceConflictException.class,
        () ->
            commit("Conflicting Commit")
                .fromReference(initialCommit)
                .delete("t3")
                .toBranch(branch));

    // Checking the state hasn't changed
    assertThat(store().hashOnReference(branch, Optional.empty())).isEqualTo(secondCommit);
  }

  /*
   * Test:
   * - Create a new branch
   * - Add a commit to create 2 keys
   * - Add a second commit to delete one key and add a new one
   * - force commit put operations
   * - Check that put operations against 1st commit for the 3 keys fail
   * - Check that delete operations against 1st commit for the 3 keys fail
   * - Check that unchanged operations against 1st commit for the 3 keys fail
   * - Check that branch state hasn't changed
   */
  @Test
  public void forceCommitConflictingOperations() throws Exception {
    final BranchName branch = BranchName.of("foo");

    store().create(branch, Optional.empty());

    commit("Initial Commit").put("t1", V_1_1).put("t2", V_2_1).toBranch(branch);

    commit("Second Commit").put("t1", V_1_2).delete("t2").put("t3", V_3_1).toBranch(branch);

    final Hash putCommit =
        forceCommit("Conflicting Commit")
            .put("t1", V_1_3)
            .put("t2", V_2_2)
            .put("t3", V_3_2)
            .toBranch(branch);

    assertThat(store().hashOnReference(branch, Optional.empty())).isEqualTo(putCommit);
    assertThat(store().getValues(branch, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"))))
        .containsExactlyInAnyOrderEntriesOf(
            ImmutableMap.of(
                Key.of("t1"), V_1_3,
                Key.of("t2"), V_2_2,
                Key.of("t3"), V_3_2));

    final Hash unchangedCommit =
        commit("Conflicting Commit")
            .unchanged("t1")
            .unchanged("t2")
            .unchanged("t3")
            .toBranch(branch);
    assertThat(store().hashOnReference(branch, Optional.empty())).isEqualTo(unchangedCommit);
    assertThat(store().getValues(branch, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"))))
        .containsExactlyInAnyOrderEntriesOf(
            ImmutableMap.of(
                Key.of("t1"), V_1_3,
                Key.of("t2"), V_2_2,
                Key.of("t3"), V_3_2));

    final Hash deleteCommit =
        commit("Conflicting Commit").delete("t1").delete("t2").delete("t3").toBranch(branch);
    assertThat(store().hashOnReference(branch, Optional.empty())).isEqualTo(deleteCommit);
    assertThat(store().getValues(branch, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"))))
        .isEmpty();
  }

  /*
   * Test:
   *  - Check that store allows storing the same value under different keys
   */
  @Test
  public void commitDuplicateValues() throws Exception {
    BranchName branch = BranchName.of("dupe-values");
    store().create(branch, Optional.empty());
    OnRefOnly foo1 = newOnRef("foo");
    OnRefOnly foo2 = newOnRef("foo");
    store()
        .commit(
            branch,
            Optional.empty(),
            commitMessage("metadata"),
            ImmutableList.of(put("keyA", foo1), put("keyB", foo2)));

    assertThat(store().getValue(branch, Key.of("keyA"))).isEqualTo(foo1);
    assertThat(store().getValue(branch, Key.of("keyB"))).isEqualTo(foo2);
  }

  /*
   * Test:
   * - Check that store throws RNFE if branch doesn't exist
   */
  @Test
  public void commitWithInvalidBranch() {
    final BranchName branch = BranchName.of("unknown");

    assertThrows(
        ReferenceNotFoundException.class,
        () ->
            store()
                .commit(
                    branch,
                    Optional.empty(),
                    commitMessage("New commit"),
                    Collections.emptyList()));
  }

  /*
   * Test:
   * - Check that store throws RNFE if reference hash doesn't exist
   */
  @Test
  public void commitWithUnknownReference()
      throws ReferenceNotFoundException, ReferenceAlreadyExistsException {
    final BranchName branch = BranchName.of("foo");
    store().create(branch, Optional.empty());

    assertThrows(
        ReferenceNotFoundException.class,
        () ->
            store()
                .commit(
                    branch,
                    Optional.of(Hash.of("1234567890abcdef")),
                    commitMessage("New commit"),
                    Collections.emptyList()));
  }

  /*
   * Test:
   * - Check that store throws IllegalArgumentException if reference hash is not in branch ancestry
   */
  @Test
  public void commitWithInvalidReference()
      throws ReferenceNotFoundException, ReferenceConflictException,
          ReferenceAlreadyExistsException {
    final BranchName branch = BranchName.of("foo");
    store().create(branch, Optional.empty());

    final Hash initialHash = store().hashOnReference(branch, Optional.empty());
    store()
        .commit(
            branch,
            Optional.of(initialHash),
            commitMessage("Some commit"),
            Collections.emptyList());

    final Hash commitHash = store().hashOnReference(branch, Optional.empty());

    final BranchName branch2 = BranchName.of("bar");
    store().create(branch2, Optional.empty());

    assertThrows(
        ReferenceNotFoundException.class,
        () ->
            store()
                .commit(
                    branch2,
                    Optional.of(commitHash),
                    commitMessage("Another commit"),
                    Collections.emptyList()));
  }
}
