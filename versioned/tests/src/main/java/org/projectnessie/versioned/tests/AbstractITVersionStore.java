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
package org.projectnessie.versioned.tests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Delete;
import org.projectnessie.versioned.Diff;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.Put;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.StringSerializer;
import org.projectnessie.versioned.TagName;
import org.projectnessie.versioned.Unchanged;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.VersionStoreException;
import org.projectnessie.versioned.WithHash;
import org.projectnessie.versioned.WithType;

/** Base class used for integration tests against version store implementations. */
public abstract class AbstractITVersionStore {

  protected abstract VersionStore<String, String, String, StringSerializer.TestEnum> store();

  /** Use case simulation: single branch, multiple users, each user updating a separate table. */
  @Test
  void singleBranchManyUsersDistinctTables() throws Exception {
    singleBranchTest(
        "singleBranchManyUsersDistinctTables", user -> String.format("user-table-%d", user), false);
  }

  /** Use case simulation: single branch, multiple users, all users updating a single table. */
  @Test
  void singleBranchManyUsersSingleTable() throws Exception {
    singleBranchTest("singleBranchManyUsersSingleTable", user -> "single-table", true);
  }

  private void singleBranchTest(
      String branchName, IntFunction<String> tableNameGen, boolean allowInconsistentValueException)
      throws Exception {
    BranchName branch = BranchName.of(branchName);

    int numUsers = 5;
    int numCommits = 50;

    Hash[] hashesKnownByUser = new Hash[numUsers];
    Hash createHash = store().create(branch, Optional.empty(), Optional.empty());
    Arrays.fill(hashesKnownByUser, createHash);

    List<String> expectedValues = new ArrayList<>();
    for (int commitNum = 0; commitNum < numCommits; commitNum++) {
      for (int user = 0; user < numUsers; user++) {
        Hash hashKnownByUser = hashesKnownByUser[user];

        String msg = String.format("user %03d/commit %03d", user, commitNum);
        expectedValues.add(msg);
        String value = String.format("data_file_%03d_%03d", user, commitNum);
        Put<String, String> put = Put.of(Key.of(tableNameGen.apply(user)), value);

        Hash commitHash;
        try {
          commitHash =
              store().commit(branch, Optional.of(hashKnownByUser), msg, ImmutableList.of(put));
        } catch (ReferenceConflictException inconsistentValueException) {
          if (allowInconsistentValueException) {
            hashKnownByUser = store().toHash(branch);
            commitHash =
                store().commit(branch, Optional.of(hashKnownByUser), msg, ImmutableList.of(put));
          } else {
            throw inconsistentValueException;
          }
        }

        assertNotEquals(hashKnownByUser, commitHash);

        hashesKnownByUser[user] = commitHash;
      }
    }

    // Verify that all commits are there and that the order of the commits is correct
    List<String> committedValues =
        commitsList(branch, Optional.empty(), Optional.empty(), s -> s.map(WithHash::getValue));
    Collections.reverse(expectedValues);
    assertEquals(expectedValues, committedValues);
  }

  /*
   * Test:
   * - Create a branch with no hash assigned to it
   * - check that a hash is returned by toHash
   * - check the branch is returned by getNamedRefs
   * - check that no commits are returned using getCommits
   * - check the branch cannot be created
   * - check the branch can be deleted
   */
  @Test
  public void createAndDeleteBranch() throws Exception {
    final BranchName branch = BranchName.of("foo");
    store().create(branch, Optional.empty(), Optional.empty());
    final Hash hash = store().toHash(branch);
    assertThat(hash).isNotNull();

    final BranchName anotherBranch = BranchName.of("bar");
    final Hash createHash = store().create(anotherBranch, Optional.of(branch), Optional.of(hash));
    final Hash commitHash = commit("Some Commit").toBranch(anotherBranch);
    assertNotEquals(createHash, commitHash);

    final BranchName anotherAnotherBranch = BranchName.of("baz");
    final Hash otherCreateHash =
        store().create(anotherAnotherBranch, Optional.of(anotherBranch), Optional.of(commitHash));
    assertEquals(commitHash, otherCreateHash);

    List<WithHash<NamedRef>> namedRefs;
    try (Stream<WithHash<NamedRef>> str =
        store().getNamedRefs().filter(r -> !r.getValue().getName().equals("main"))) {
      namedRefs = str.collect(Collectors.toList());
    }
    assertThat(namedRefs)
        .containsExactlyInAnyOrder(
            WithHash.of(hash, branch),
            WithHash.of(commitHash, anotherBranch),
            WithHash.of(commitHash, anotherAnotherBranch));

    assertThat(commitsList(branch, Optional.empty(), Optional.empty())).isEmpty();
    assertThat(commitsList(anotherBranch, Optional.empty(), Optional.empty())).hasSize(1);
    assertThat(commitsList(anotherAnotherBranch, Optional.empty(), Optional.empty())).hasSize(1);
    assertThat(commitsList(branch, Optional.of(hash), Optional.empty()))
        .isEmpty(); // empty commit should not be listed
    assertThat(commitsList(anotherBranch, Optional.of(commitHash), Optional.empty()))
        .hasSize(1); // empty commit should not be listed

    assertThrows(
        ReferenceAlreadyExistsException.class,
        () -> store().create(branch, Optional.empty(), Optional.empty()));
    assertThrows(
        ReferenceAlreadyExistsException.class,
        () -> store().create(branch, Optional.of(branch), Optional.of(hash)));

    store().delete(branch, Optional.of(hash));
    assertThrows(ReferenceNotFoundException.class, () -> store().toHash(branch));
    try (Stream<WithHash<NamedRef>> str =
        store().getNamedRefs().filter(r -> !r.getValue().getName().equals("main"))) {
      assertThat(str).hasSize(2); // bar + baz
    }
    assertThrows(ReferenceNotFoundException.class, () -> store().delete(branch, Optional.of(hash)));
  }

  @Test
  public void commitLogPaging() throws Exception {
    BranchName branch = BranchName.of("commitLogPaging");
    Hash createHash = store().create(branch, Optional.empty(), Optional.empty());

    int commits = 95; // this should be enough
    Hash[] commitHashes = new Hash[commits];
    List<String> messages = new ArrayList<>(commits);
    for (int i = 0; i < commits; i++) {
      String msg = String.format("commit#%05d", i);
      messages.add(msg);
      commitHashes[i] =
          store()
              .commit(
                  branch,
                  Optional.of(i == 0 ? createHash : commitHashes[i - 1]),
                  msg,
                  ImmutableList.of(Put.of(Key.of("table"), String.format("value#%05d", i))));
    }
    Collections.reverse(messages);

    List<String> justTwo =
        commitsList(
            branch, Optional.empty(), Optional.empty(), s -> s.limit(2).map(WithHash::getValue));
    assertEquals(messages.subList(0, 2), justTwo);
    List<String> justTen =
        commitsList(
            branch, Optional.empty(), Optional.empty(), s -> s.limit(10).map(WithHash::getValue));
    assertEquals(messages.subList(0, 10), justTen);

    int pageSize = 10;

    // Test parameter sanity check. Want the last page to be smaller than the page-size.
    assertNotEquals(0, commits % (pageSize - 1));

    Hash lastHash = null;
    for (int offset = 0; ; ) {
      List<WithHash<String>> logPage =
          commitsList(
              branch, Optional.ofNullable(lastHash), Optional.empty(), s -> s.limit(pageSize));

      assertEquals(
          messages.subList(offset, Math.min(offset + pageSize, commits)),
          logPage.stream().map(WithHash::getValue).collect(Collectors.toList()));

      lastHash = logPage.get(logPage.size() - 1).getHash();

      offset += pageSize - 1;
      if (offset >= commits) {
        // The "next after last page" should always return just a single commit, that's basically
        // the "end of commit-log"-condition.
        logPage =
            commitsList(branch, Optional.of(lastHash), Optional.empty(), s -> s.limit(pageSize));
        assertEquals(
            Collections.singletonList(messages.get(commits - 1)),
            logPage.stream().map(WithHash::getValue).collect(Collectors.toList()));
        break;
      }
    }
  }

  /*
   * Test:
   * - Create a branch with no hash assigned to it
   * - add a commit to the branch
   * - create a tag for the initial hash
   * - create another tag for the hash after the commit
   * - check that cannot create existing tags, or tag with no assigned hash
   * - check that a hash is returned by toHash
   * - check the tags are returned by getNamedRefs
   * - check that expected commits are returned by getCommits
   * - check the branch can be deleted
   */
  @Test
  public void createAndDeleteTag() throws Exception {
    final BranchName branch = BranchName.of("foo");
    store().create(branch, Optional.empty(), Optional.empty());

    final Hash initialHash = store().toHash(branch);
    final Hash commitHash = commit("Some commit").toBranch(branch);

    final TagName tag = TagName.of("tag");
    store().create(tag, Optional.of(branch), Optional.of(initialHash));

    final TagName anotherTag = TagName.of("another-tag");
    store().create(anotherTag, Optional.of(branch), Optional.of(commitHash));

    assertThrows(
        ReferenceAlreadyExistsException.class,
        () -> store().create(tag, Optional.of(branch), Optional.of(initialHash)));
    assertThrows(
        IllegalArgumentException.class,
        () -> store().create(tag, Optional.empty(), Optional.empty()));

    assertThat(store().toHash(tag)).isEqualTo(initialHash);
    assertThat(store().toHash(anotherTag)).isEqualTo(commitHash);

    List<WithHash<NamedRef>> namedRefs;
    try (Stream<WithHash<NamedRef>> str =
        store().getNamedRefs().filter(r -> !r.getValue().getName().equals("main"))) {
      namedRefs = str.collect(Collectors.toList());
    }
    assertThat(namedRefs)
        .containsExactlyInAnyOrder(
            WithHash.of(commitHash, branch),
            WithHash.of(initialHash, tag),
            WithHash.of(commitHash, anotherTag));

    assertThat(commitsList(tag, Optional.empty(), Optional.empty())).isEmpty();
    assertThat(commitsList(branch, Optional.of(initialHash), Optional.empty()))
        .isEmpty(); // empty commit should not be listed

    assertThat(commitsList(anotherTag, Optional.empty(), Optional.empty())).hasSize(1);
    assertThat(commitsList(anotherTag, Optional.of(commitHash), Optional.empty()))
        .hasSize(1); // empty commit should not be listed

    store().delete(tag, Optional.of(initialHash));
    assertThrows(ReferenceNotFoundException.class, () -> store().toHash(tag));
    try (Stream<WithHash<NamedRef>> str =
        store().getNamedRefs().filter(r -> !r.getValue().getName().equals("main"))) {
      assertThat(str).hasSize(2); // foo + another-tag
    }
    assertThrows(
        ReferenceNotFoundException.class, () -> store().delete(tag, Optional.of(initialHash)));
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

    final Hash createHash = store().create(branch, Optional.empty(), Optional.empty());
    final Hash initialHash = store().toHash(branch);
    assertEquals(createHash, initialHash);

    final Hash commitHash0 =
        store().commit(branch, Optional.of(initialHash), "Some commit", Collections.emptyList());
    final Hash commitHash = store().toHash(branch);
    assertEquals(commitHash, commitHash0);

    assertThat(commitHash).isNotEqualTo(initialHash);
    store().commit(branch, Optional.of(initialHash), "Another commit", Collections.emptyList());
    final Hash anotherCommitHash = store().toHash(branch);

    assertThat(commitsList(branch, Optional.empty(), Optional.empty()))
        .contains(
            WithHash.of(anotherCommitHash, "Another commit"),
            WithHash.of(commitHash, "Some commit"));
    assertThat(commitsList(branch, Optional.of(commitHash), Optional.empty()))
        .contains(WithHash.of(commitHash, "Some commit"));

    assertThrows(
        ReferenceConflictException.class, () -> store().delete(branch, Optional.of(initialHash)));
    store().delete(branch, Optional.of(anotherCommitHash));
    assertThrows(ReferenceNotFoundException.class, () -> store().toHash(branch));
    try (Stream<WithHash<NamedRef>> str =
        store().getNamedRefs().filter(r -> !r.getValue().getName().equals("main"))) {
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

    store().create(branch, Optional.empty(), Optional.empty());

    final Hash initialCommit =
        commit("Initial Commit")
            .put("t1", "v1_1")
            .put("t2", "v2_1")
            .put("t3", "v3_1")
            .toBranch(branch);

    final Hash secondCommit =
        commit("Second Commit")
            .put("t1", "v1_2")
            .delete("t2")
            .delete("t3")
            .put("t4", "v4_1")
            .toBranch(branch);

    final Hash thirdCommit =
        commit("Third Commit").put("t2", "v2_2").unchanged("t4").toBranch(branch);

    assertThat(commitsList(branch, Optional.empty(), Optional.empty()))
        .contains(
            WithHash.of(thirdCommit, "Third Commit"),
            WithHash.of(secondCommit, "Second Commit"),
            WithHash.of(initialCommit, "Initial Commit"));

    try (Stream<Key> keys = store().getKeys(branch, Optional.empty()).map(WithType::getValue)) {
      assertThat(keys).containsExactlyInAnyOrder(Key.of("t1"), Key.of("t2"), Key.of("t4"));
    }

    try (Stream<Key> keys =
        store().getKeys(branch, Optional.of(secondCommit)).map(WithType::getValue)) {
      assertThat(keys).containsExactlyInAnyOrder(Key.of("t1"), Key.of("t4"));
    }

    try (Stream<Key> keys =
        store().getKeys(branch, Optional.of(initialCommit)).map(WithType::getValue)) {
      assertThat(keys).containsExactlyInAnyOrder(Key.of("t1"), Key.of("t2"), Key.of("t3"));
    }

    assertThat(
            store()
                .getValues(
                    branch,
                    Optional.empty(),
                    Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"), Key.of("t4"))))
        .contains(Optional.of("v1_2"), Optional.of("v2_2"), Optional.empty(), Optional.of("v4_1"));

    assertThat(
            store()
                .getValues(
                    branch,
                    Optional.of(secondCommit),
                    Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"), Key.of("t4"))))
        .contains(Optional.of("v1_2"), Optional.empty(), Optional.empty(), Optional.of("v4_1"));

    assertThat(
            store()
                .getValues(
                    branch,
                    Optional.of(initialCommit),
                    Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"), Key.of("t4"))))
        .contains(Optional.of("v1_1"), Optional.of("v2_1"), Optional.of("v3_1"), Optional.empty());

    assertThat(store().getValue(branch, Optional.empty(), Key.of("t1"))).isEqualTo("v1_2");
    assertThat(store().getValue(branch, Optional.empty(), Key.of("t2"))).isEqualTo("v2_2");
    assertThat(store().getValue(branch, Optional.empty(), Key.of("t3"))).isNull();
    assertThat(store().getValue(branch, Optional.empty(), Key.of("t4"))).isEqualTo("v4_1");

    assertThat(store().getValue(branch, Optional.of(secondCommit), Key.of("t1"))).isEqualTo("v1_2");
    assertThat(store().getValue(branch, Optional.of(secondCommit), Key.of("t2"))).isNull();
    assertThat(store().getValue(branch, Optional.of(secondCommit), Key.of("t3"))).isNull();
    assertThat(store().getValue(branch, Optional.of(secondCommit), Key.of("t4"))).isEqualTo("v4_1");

    assertThat(store().getValue(branch, Optional.of(initialCommit), Key.of("t1")))
        .isEqualTo("v1_1");
    assertThat(store().getValue(branch, Optional.of(initialCommit), Key.of("t2")))
        .isEqualTo("v2_1");
    assertThat(store().getValue(branch, Optional.of(initialCommit), Key.of("t3")))
        .isEqualTo("v3_1");
    assertThat(store().getValue(branch, Optional.of(initialCommit), Key.of("t4"))).isNull();
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

    store().create(branch, Optional.empty(), Optional.empty());

    final Hash initialCommit =
        commit("Initial Commit")
            .put("t1", "v1_1")
            .put("t2", "v2_1")
            .put("t3", "v3_1")
            .toBranch(branch);

    final Hash t1Commit =
        commit("T1 Commit").fromReference(initialCommit).put("t1", "v1_2").toBranch(branch);
    final Hash t2Commit =
        commit("T2 Commit").fromReference(initialCommit).delete("t2").toBranch(branch);
    final Hash t3Commit =
        commit("T3 Commit").fromReference(initialCommit).unchanged("t3").toBranch(branch);
    final Hash extraCommit =
        commit("Extra Commit")
            .fromReference(t1Commit)
            .put("t1", "v1_3")
            .put("t3", "v3_2")
            .toBranch(branch);
    final Hash newT2Commit =
        commit("New T2 Commit").fromReference(t2Commit).put("t2", "new_v2_1").toBranch(branch);

    assertThat(commitsList(branch, Optional.empty(), Optional.empty()))
        .contains(
            WithHash.of(newT2Commit, "New T2 Commit"),
            WithHash.of(extraCommit, "Extra Commit"),
            WithHash.of(t3Commit, "T3 Commit"),
            WithHash.of(t2Commit, "T2 Commit"),
            WithHash.of(t1Commit, "T1 Commit"),
            WithHash.of(initialCommit, "Initial Commit"));

    try (Stream<Key> keys = store().getKeys(branch, Optional.empty()).map(WithType::getValue)) {
      assertThat(keys).containsExactlyInAnyOrder(Key.of("t1"), Key.of("t2"), Key.of("t3"));
    }

    assertThat(
            store()
                .getValues(
                    branch,
                    Optional.empty(),
                    Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"))))
        .contains(Optional.of("v1_3"), Optional.of("new_v2_1"), Optional.of("v3_2"));

    assertThat(
            store()
                .getValues(
                    branch,
                    Optional.of(newT2Commit),
                    Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"))))
        .contains(Optional.of("v1_3"), Optional.of("new_v2_1"), Optional.of("v3_2"));

    assertThat(
            store()
                .getValues(
                    branch,
                    Optional.of(extraCommit),
                    Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"))))
        .contains(Optional.of("v1_3"), Optional.empty(), Optional.of("v3_2"));

    assertThat(
            store()
                .getValues(
                    branch,
                    Optional.of(t3Commit),
                    Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"))))
        .contains(Optional.of("v1_2"), Optional.empty(), Optional.of("v3_1"));

    assertThat(
            store()
                .getValues(
                    branch,
                    Optional.of(t2Commit),
                    Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"))))
        .contains(Optional.of("v1_2"), Optional.empty(), Optional.of("v3_1"));

    assertThat(
            store()
                .getValues(
                    branch,
                    Optional.of(t1Commit),
                    Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"))))
        .contains(Optional.of("v1_2"), Optional.of("v2_1"), Optional.of("v3_1"));
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

    store().create(branch, Optional.empty(), Optional.empty());

    final Hash initialCommit =
        commit("Initial Commit").put("t1", "v1_1").put("t2", "v2_1").toBranch(branch);

    final Hash secondCommit =
        commit("Second Commit").put("t1", "v1_2").delete("t2").put("t3", "v3_1").toBranch(branch);

    assertThrows(
        ReferenceConflictException.class,
        () ->
            commit("Conflicting Commit")
                .fromReference(initialCommit)
                .put("t1", "v1_3")
                .toBranch(branch));
    assertThrows(
        ReferenceConflictException.class,
        () ->
            commit("Conflicting Commit")
                .fromReference(initialCommit)
                .put("t2", "v2_2")
                .toBranch(branch));
    assertThrows(
        ReferenceConflictException.class,
        () ->
            commit("Conflicting Commit")
                .fromReference(initialCommit)
                .put("t3", "v3_2")
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
    assertThat(store().toHash(branch)).isEqualTo(secondCommit);
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

    store().create(branch, Optional.empty(), Optional.empty());

    commit("Initial Commit").put("t1", "v1_1").put("t2", "v2_1").toBranch(branch);

    commit("Second Commit").put("t1", "v1_2").delete("t2").put("t3", "v3_1").toBranch(branch);

    final Hash putCommit =
        forceCommit("Conflicting Commit")
            .put("t1", "v1_3")
            .put("t2", "v2_2")
            .put("t3", "v3_2")
            .toBranch(branch);

    assertThat(store().toHash(branch)).isEqualTo(putCommit);
    assertThat(
            store()
                .getValues(
                    branch,
                    Optional.empty(),
                    Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"))))
        .contains(Optional.of("v1_3"), Optional.of("v2_2"), Optional.of("v3_2"));

    final Hash unchangedCommit =
        commit("Conflicting Commit")
            .unchanged("t1")
            .unchanged("t2")
            .unchanged("t3")
            .toBranch(branch);
    assertThat(store().toHash(branch)).isEqualTo(unchangedCommit);
    assertThat(
            store()
                .getValues(
                    branch,
                    Optional.empty(),
                    Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"))))
        .contains(Optional.of("v1_3"), Optional.of("v2_2"), Optional.of("v3_2"));

    final Hash deleteCommit =
        commit("Conflicting Commit").delete("t1").delete("t2").delete("t3").toBranch(branch);
    assertThat(store().toHash(branch)).isEqualTo(deleteCommit);
    assertThat(
            store()
                .getValues(
                    branch,
                    Optional.empty(),
                    Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"))))
        .contains(Optional.empty(), Optional.empty(), Optional.empty());
  }

  /*
   * Test:
   *  - Check that store allows storing the same value under different keys
   */
  @Test
  public void commitDuplicateValues() throws Exception {
    BranchName branch = BranchName.of("dupe-values");
    store().create(branch, Optional.empty(), Optional.empty());
    store()
        .commit(
            branch,
            Optional.empty(),
            "metadata",
            ImmutableList.of(put("keyA", "foo"), put("keyB", "foo")));

    assertThat(store().getValue(branch, Optional.empty(), Key.of("keyA"))).isEqualTo("foo");
    assertThat(store().getValue(branch, Optional.empty(), Key.of("keyB"))).isEqualTo("foo");
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
        () -> store().commit(branch, Optional.empty(), "New commit", Collections.emptyList()));
  }

  /*
   * Test:
   * - Check that store throws RNFE if reference hash doesn't exist
   */
  @Test
  public void commitWithUnknownReference()
      throws ReferenceNotFoundException, ReferenceAlreadyExistsException {
    final BranchName branch = BranchName.of("foo");
    store().create(branch, Optional.empty(), Optional.empty());

    assertThrows(
        ReferenceNotFoundException.class,
        () ->
            store()
                .commit(
                    branch,
                    Optional.of(Hash.of("1234567890abcdef")),
                    "New commit",
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
    store().create(branch, Optional.empty(), Optional.empty());

    final Hash initialHash = store().toHash(branch);
    store().commit(branch, Optional.of(initialHash), "Some commit", Collections.emptyList());

    final Hash commitHash = store().toHash(branch);

    final BranchName branch2 = BranchName.of("bar");
    store().create(branch2, Optional.empty(), Optional.empty());

    assertThrows(
        ReferenceNotFoundException.class,
        () ->
            store()
                .commit(
                    branch2, Optional.of(commitHash), "Another commit", Collections.emptyList()));
  }

  @Test
  public void getValueForEmptyBranch()
      throws ReferenceNotFoundException, ReferenceAlreadyExistsException {
    BranchName branch = BranchName.of("empty-branch");
    store().create(branch, Optional.empty(), Optional.empty());
    final Hash hash = store().toHash(branch);

    assertThat(store().getValue(branch, Optional.of(hash), Key.of("arbitrary"))).isNull();
  }

  @Test
  public void assign() throws VersionStoreException {
    final BranchName branch = BranchName.of("foo");
    store().create(branch, Optional.empty(), Optional.empty());
    final Hash initialHash = store().toHash(branch);

    final Hash commit = commit("Some commit").toBranch(branch);
    store().create(BranchName.of("bar"), Optional.of(branch), Optional.of(commit));
    store().create(TagName.of("tag1"), Optional.of(branch), Optional.of(commit));
    store().create(TagName.of("tag2"), Optional.of(branch), Optional.of(commit));
    store().create(TagName.of("tag3"), Optional.of(branch), Optional.of(commit));

    final Hash anotherCommit = commit("Another commit").toBranch(branch);
    store().assign(TagName.of("tag2"), Optional.of(commit), branch, Optional.of(anotherCommit));
    store().assign(TagName.of("tag3"), Optional.empty(), branch, Optional.of(anotherCommit));

    assertThrows(
        ReferenceNotFoundException.class,
        () ->
            store()
                .assign(
                    BranchName.of("baz"), Optional.empty(), branch, Optional.of(anotherCommit)));
    assertThrows(
        ReferenceNotFoundException.class,
        () ->
            store()
                .assign(
                    TagName.of("unknowon-tag"),
                    Optional.empty(),
                    branch,
                    Optional.of(anotherCommit)));

    assertThrows(
        ReferenceConflictException.class,
        () ->
            store()
                .assign(TagName.of("tag1"), Optional.of(initialHash), branch, Optional.of(commit)));
    assertThrows(
        ReferenceConflictException.class,
        () ->
            store()
                .assign(
                    TagName.of("tag1"),
                    Optional.of(initialHash),
                    branch,
                    Optional.of(anotherCommit)));
    assertThrows(
        ReferenceNotFoundException.class,
        () ->
            store()
                .assign(
                    TagName.of("tag1"),
                    Optional.of(commit),
                    branch,
                    Optional.of(Hash.of("1234567890abcdef"))));

    assertThat(commitsList(branch, Optional.empty(), Optional.empty()))
        .contains(WithHash.of(anotherCommit, "Another commit"), WithHash.of(commit, "Some commit"));

    assertThat(commitsList(BranchName.of("bar"), Optional.empty(), Optional.empty()))
        .contains(WithHash.of(commit, "Some commit"));

    assertThat(commitsList(TagName.of("tag1"), Optional.empty(), Optional.empty()))
        .contains(WithHash.of(commit, "Some commit"));

    assertThat(commitsList(TagName.of("tag2"), Optional.empty(), Optional.empty()))
        .contains(WithHash.of(anotherCommit, "Another commit"), WithHash.of(commit, "Some commit"));
  }

  @Nested
  @DisplayName("when transplanting")
  protected class WhenTransplanting {

    private Hash initialHash;
    private Hash firstCommit;
    private Hash secondCommit;
    private Hash thirdCommit;
    private BranchName branch;

    @BeforeEach
    protected void setupCommits() throws VersionStoreException {
      branch = BranchName.of("foo");
      store().create(branch, Optional.empty(), Optional.empty());

      initialHash = store().toHash(branch);

      firstCommit =
          commit("Initial Commit")
              .put("t1", "v1_1")
              .put("t2", "v2_1")
              .put("t3", "v3_1")
              .toBranch(branch);

      secondCommit =
          commit("Second Commit")
              .put("t1", "v1_2")
              .delete("t2")
              .delete("t3")
              .put("t4", "v4_1")
              .toBranch(branch);

      thirdCommit = commit("Third Commit").put("t2", "v2_2").unchanged("t4").toBranch(branch);
    }

    @Test
    protected void checkTransplantOnEmptyBranch() throws VersionStoreException {
      final BranchName newBranch = BranchName.of("bar_1");
      store().create(newBranch, Optional.empty(), Optional.empty());

      store()
          .transplant(
              newBranch,
              Optional.of(initialHash),
              branch,
              Arrays.asList(firstCommit, secondCommit, thirdCommit));
      assertThat(
              store()
                  .getValues(
                      newBranch,
                      Optional.empty(),
                      Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"), Key.of("t4"))))
          .contains(
              Optional.of("v1_2"), Optional.of("v2_2"), Optional.empty(), Optional.of("v4_1"));
    }

    @Test
    protected void checkTransplantWithPreviousCommit() throws VersionStoreException {
      final BranchName newBranch = BranchName.of("bar_2");
      store().create(newBranch, Optional.empty(), Optional.empty());
      commit("Unrelated commit").put("t5", "v5_1").toBranch(newBranch);

      store()
          .transplant(
              newBranch,
              Optional.of(initialHash),
              branch,
              Arrays.asList(firstCommit, secondCommit, thirdCommit));
      assertThat(
              store()
                  .getValues(
                      newBranch,
                      Optional.empty(),
                      Arrays.asList(
                          Key.of("t1"), Key.of("t2"), Key.of("t3"), Key.of("t4"), Key.of("t5"))))
          .contains(
              Optional.of("v1_2"),
              Optional.of("v2_2"),
              Optional.empty(),
              Optional.of("v4_1"),
              Optional.of("v5_1"));
    }

    @Test
    protected void checkTransplantWitConflictingCommit() throws VersionStoreException {
      final BranchName newBranch = BranchName.of("bar_3");
      store().create(newBranch, Optional.empty(), Optional.empty());
      commit("Another commit").put("t1", "v1_4").toBranch(newBranch);

      assertThrows(
          ReferenceConflictException.class,
          () ->
              store()
                  .transplant(
                      newBranch,
                      Optional.of(initialHash),
                      branch,
                      Arrays.asList(firstCommit, secondCommit, thirdCommit)));
    }

    @Test
    protected void checkTransplantWithDelete() throws VersionStoreException {
      final BranchName newBranch = BranchName.of("bar_4");
      store().create(newBranch, Optional.empty(), Optional.empty());
      commit("Another commit").put("t1", "v1_4").toBranch(newBranch);
      commit("Another commit").delete("t1").toBranch(newBranch);

      store()
          .transplant(
              newBranch,
              Optional.of(initialHash),
              branch,
              Arrays.asList(firstCommit, secondCommit, thirdCommit));
      assertThat(
              store()
                  .getValues(
                      newBranch,
                      Optional.empty(),
                      Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"), Key.of("t4"))))
          .contains(
              Optional.of("v1_2"), Optional.of("v2_2"), Optional.empty(), Optional.of("v4_1"));
    }

    @Test
    protected void checkTransplantOnNonExistingBranch() {
      final BranchName newBranch = BranchName.of("bar_5");
      assertThrows(
          ReferenceNotFoundException.class,
          () ->
              store()
                  .transplant(
                      newBranch,
                      Optional.of(initialHash),
                      branch,
                      Arrays.asList(firstCommit, secondCommit, thirdCommit)));
    }

    @Test
    protected void checkTransplantWithNonExistingCommit() throws VersionStoreException {
      final BranchName newBranch = BranchName.of("bar_6");
      store().create(newBranch, Optional.empty(), Optional.empty());
      assertThrows(
          ReferenceNotFoundException.class,
          () ->
              store()
                  .transplant(
                      newBranch,
                      Optional.of(initialHash),
                      branch,
                      Collections.singletonList(Hash.of("1234567890abcdef"))));
    }

    @Test
    protected void checkTransplantWithNoExpectedHash() throws VersionStoreException {
      final BranchName newBranch = BranchName.of("bar_7");
      store().create(newBranch, Optional.empty(), Optional.empty());
      commit("Another commit").put("t5", "v5_1").toBranch(newBranch);
      commit("Another commit").put("t1", "v1_4").toBranch(newBranch);

      store()
          .transplant(
              newBranch,
              Optional.empty(),
              branch,
              Arrays.asList(firstCommit, secondCommit, thirdCommit));
      assertThat(
              store()
                  .getValues(
                      newBranch,
                      Optional.empty(),
                      Arrays.asList(
                          Key.of("t1"), Key.of("t2"), Key.of("t3"), Key.of("t4"), Key.of("t5"))))
          .contains(
              Optional.of("v1_2"),
              Optional.of("v2_2"),
              Optional.empty(),
              Optional.of("v4_1"),
              Optional.of("v5_1"));
    }

    @Test
    protected void checkTransplantWithCommitsInWrongOrder() throws VersionStoreException {
      final BranchName newBranch = BranchName.of("bar_8");
      store().create(newBranch, Optional.empty(), Optional.empty());

      assertThrows(
          IllegalArgumentException.class,
          () ->
              store()
                  .transplant(
                      newBranch,
                      Optional.empty(),
                      branch,
                      Arrays.asList(secondCommit, firstCommit, thirdCommit)));
    }

    @Test
    protected void checkInvalidBranchHash() throws VersionStoreException {
      final BranchName anotherBranch = BranchName.of("bar");
      store().create(anotherBranch, Optional.empty(), Optional.empty());
      final Hash unrelatedCommit =
          commit("Another Commit")
              .put("t1", "v1_1")
              .put("t2", "v2_1")
              .put("t3", "v3_1")
              .toBranch(anotherBranch);

      final BranchName newBranch = BranchName.of("bar_1");
      store().create(newBranch, Optional.empty(), Optional.empty());

      assertThrows(
          ReferenceNotFoundException.class,
          () ->
              store()
                  .transplant(
                      newBranch,
                      Optional.of(unrelatedCommit),
                      branch,
                      Arrays.asList(firstCommit, secondCommit, thirdCommit)));
    }

    @Test
    protected void transplantBasic() throws VersionStoreException {
      final BranchName newBranch = BranchName.of("bar_2");
      store().create(newBranch, Optional.empty(), Optional.empty());
      commit("Unrelated commit").put("t5", "v5_1").toBranch(newBranch);

      store()
          .transplant(
              newBranch,
              Optional.of(initialHash),
              branch,
              Arrays.asList(firstCommit, secondCommit));
      assertThat(
              store()
                  .getValues(
                      newBranch,
                      Optional.empty(),
                      Arrays.asList(Key.of("t1"), Key.of("t4"), Key.of("t5"))))
          .contains(Optional.of("v1_2"), Optional.of("v4_1"), Optional.of("v5_1"));
    }
  }

  @Nested
  protected class WhenMergingCommonAncestor extends AbstractWhenMerging {
    protected WhenMergingCommonAncestor() {
      super(true);
    }
  }

  @Nested
  protected class WhenMerging extends AbstractWhenMerging {
    protected WhenMerging() {
      super(false);
    }
  }

  protected abstract class AbstractWhenMerging {

    private Hash initialHash;
    private Hash firstCommit;
    private Hash secondCommit;
    private Hash thirdCommit;
    private BranchName branch;
    private final boolean commonAncestor;

    protected AbstractWhenMerging(boolean commonAncestorRequired) {
      this.commonAncestor = commonAncestorRequired;
    }

    @BeforeEach
    protected void setupCommits() throws VersionStoreException {
      branch = BranchName.of("foo");
      store().create(branch, Optional.empty(), Optional.empty());

      // The default common ancestor for all merge-tests.
      // The spec for 'VersionStore.merge' mentions "(...) until we arrive at a common ancestor",
      // but old implementations allowed a merge even if the "merge-from" and "merge-to" have no
      // common ancestor and did merge "everything" from the "merge-from" into "merge-to".
      // Note: "beginning-of-time" (aka creating a branch without specifying a "create-from")
      // creates a new commit-tree that is decoupled from other commit-trees.
      initialHash =
          commonAncestor
              ? commit("Default common ancestor").toBranch(branch)
              : store().toHash(branch);

      firstCommit =
          commit("First Commit")
              .put("t1", "v1_1")
              .put("t2", "v2_1")
              .put("t3", "v3_1")
              .toBranch(branch);
      secondCommit =
          commit("Second Commit")
              .put("t1", "v1_2")
              .delete("t2")
              .delete("t3")
              .put("t4", "v4_1")
              .toBranch(branch);
      thirdCommit = commit("Third Commit").put("t2", "v2_2").unchanged("t4").toBranch(branch);
    }

    @Test
    protected void mergeIntoEmptyBranch() throws VersionStoreException {
      final BranchName newBranch = BranchName.of("bar_1");
      store().create(newBranch, Optional.of(branch), Optional.of(initialHash));

      store()
          .merge(
              branch,
              Optional.of(thirdCommit),
              newBranch,
              Optional.of(initialHash),
              commonAncestor);
      assertThat(
              store()
                  .getValues(
                      newBranch,
                      Optional.empty(),
                      Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"), Key.of("t4"))))
          .contains(
              Optional.of("v1_2"), Optional.of("v2_2"), Optional.empty(), Optional.of("v4_1"));

      assertThat(store().toHash(newBranch)).isEqualTo(thirdCommit);
    }

    @Test
    protected void mergeIntoNonConflictingBranch() throws VersionStoreException {
      final BranchName newBranch = BranchName.of("bar_2");
      store().create(newBranch, Optional.of(branch), Optional.of(initialHash));
      final Hash newCommit = commit("Unrelated commit").put("t5", "v5_1").toBranch(newBranch);

      store().merge(branch, Optional.of(thirdCommit), newBranch, Optional.empty(), commonAncestor);
      assertThat(
              store()
                  .getValues(
                      newBranch,
                      Optional.empty(),
                      Arrays.asList(
                          Key.of("t1"), Key.of("t2"), Key.of("t3"), Key.of("t4"), Key.of("t5"))))
          .contains(
              Optional.of("v1_2"),
              Optional.of("v2_2"),
              Optional.empty(),
              Optional.of("v4_1"),
              Optional.of("v5_1"));

      final List<WithHash<String>> commits =
          commitsList(newBranch, Optional.empty(), Optional.empty());
      assertThat(commits).hasSize(commonAncestor ? 5 : 4);
      if (commonAncestor) {
        assertThat(commits.get(4).getHash()).isEqualTo(initialHash);
      }
      assertThat(commits.get(3).getHash()).isEqualTo(newCommit);
      assertThat(commits.get(2).getValue()).isEqualTo("First Commit");
      assertThat(commits.get(1).getValue()).isEqualTo("Second Commit");
      assertThat(commits.get(0).getValue()).isEqualTo("Third Commit");
    }

    @Test
    protected void nonEmptyFastForwardMerge() throws VersionStoreException {
      final Key key = Key.of("t1");
      final BranchName etl = BranchName.of("etl");
      final BranchName review = BranchName.of("review");
      store().create(etl, Optional.of(branch), Optional.of(initialHash));
      store().create(review, Optional.of(branch), Optional.of(initialHash));
      store()
          .commit(
              etl, Optional.empty(), "commit 1", Collections.singletonList(Put.of(key, "value1")));
      store()
          .merge(etl, Optional.of(store().toHash(etl)), review, Optional.empty(), commonAncestor);
      store()
          .commit(
              etl, Optional.empty(), "commit 2", Collections.singletonList(Put.of(key, "value2")));
      store()
          .merge(etl, Optional.of(store().toHash(etl)), review, Optional.empty(), commonAncestor);
      assertEquals(store().getValue(review, Optional.empty(), key), "value2");
    }

    @Test
    protected void mergeWithCommonAncestor() throws VersionStoreException {
      final BranchName newBranch = BranchName.of("bar_2");
      store().create(newBranch, Optional.of(branch), Optional.of(firstCommit));

      final Hash newCommit = commit("Unrelated commit").put("t5", "v5_1").toBranch(newBranch);

      store().merge(branch, Optional.of(thirdCommit), newBranch, Optional.empty(), commonAncestor);
      assertThat(
              store()
                  .getValues(
                      newBranch,
                      Optional.empty(),
                      Arrays.asList(
                          Key.of("t1"), Key.of("t2"), Key.of("t3"), Key.of("t4"), Key.of("t5"))))
          .contains(
              Optional.of("v1_2"),
              Optional.of("v2_2"),
              Optional.empty(),
              Optional.of("v4_1"),
              Optional.of("v5_1"));

      final List<WithHash<String>> commits =
          commitsList(newBranch, Optional.empty(), Optional.empty());
      assertThat(commits).hasSize(commonAncestor ? 5 : 4);
      if (commonAncestor) {
        assertThat(commits.get(4).getHash()).isEqualTo(initialHash);
      }
      assertThat(commits.get(3).getHash()).isEqualTo(firstCommit);
      assertThat(commits.get(2).getHash()).isEqualTo(newCommit);
      assertThat(commits.get(1).getValue()).isEqualTo("Second Commit");
      assertThat(commits.get(0).getValue()).isEqualTo("Third Commit");
    }

    @Test
    protected void mergeWithConflictingKeys() throws VersionStoreException {
      final BranchName foo = BranchName.of("foofoo");
      final BranchName bar = BranchName.of("barbar");
      store().create(foo, Optional.of(branch), Optional.of(initialHash));
      store().create(bar, Optional.of(branch), Optional.of(initialHash));

      // we're essentially modifying the same key on both branches and then merging one branch into
      // the other and expect a conflict
      Key key1 = Key.of("some_key1");
      Key key2 = Key.of("some_key2");

      store()
          .commit(
              foo, Optional.empty(), "commit 1", Collections.singletonList(Put.of(key1, "value1")));
      store()
          .commit(
              bar, Optional.empty(), "commit 2", Collections.singletonList(Put.of(key1, "value2")));
      store()
          .commit(
              foo, Optional.empty(), "commit 3", Collections.singletonList(Put.of(key2, "value3")));
      Hash barHash =
          store()
              .commit(
                  bar,
                  Optional.empty(),
                  "commit 4",
                  Collections.singletonList(Put.of(key2, "value4")));

      assertThatThrownBy(
              () -> store().merge(bar, Optional.of(barHash), foo, Optional.empty(), commonAncestor))
          .isInstanceOf(ReferenceConflictException.class)
          .hasMessageContaining("The following keys have been changed in conflict:")
          .hasMessageContaining(key1.toString())
          .hasMessageContaining(key2.toString());
    }

    @Test
    protected void mergeIntoConflictingBranch() throws VersionStoreException {
      final BranchName newBranch = BranchName.of("bar_3");
      store().create(newBranch, Optional.of(branch), Optional.of(initialHash));
      commit("Another commit").put("t1", "v1_4").toBranch(newBranch);

      assertThrows(
          ReferenceConflictException.class,
          () ->
              store()
                  .merge(
                      branch,
                      Optional.of(thirdCommit),
                      newBranch,
                      Optional.of(initialHash),
                      commonAncestor));
    }

    @Test
    protected void mergeIntoNonExistingBranch() {
      final BranchName newBranch = BranchName.of("bar_5");
      assertThrows(
          ReferenceNotFoundException.class,
          () ->
              store()
                  .merge(
                      branch,
                      Optional.of(thirdCommit),
                      newBranch,
                      Optional.of(initialHash),
                      commonAncestor));
    }

    @Test
    protected void mergeIntoNonExistingReference() throws VersionStoreException {
      final BranchName newBranch = BranchName.of("bar_6");
      store().create(newBranch, Optional.of(branch), Optional.of(initialHash));
      assertThrows(
          ReferenceNotFoundException.class,
          () ->
              store()
                  .merge(
                      branch,
                      Optional.of(Hash.of("1234567890abcdef")),
                      newBranch,
                      Optional.of(initialHash),
                      commonAncestor));
    }
  }

  @Test
  void toRef() throws VersionStoreException {
    final BranchName branch = BranchName.of("toRef");
    store().create(branch, Optional.empty(), Optional.empty());
    store().toHash(branch);

    final Hash firstCommit = commit("First Commit").toBranch(branch);

    assertThat(store().toRef(branch.getName())).isEqualTo(WithHash.of(firstCommit, branch));

    final Hash secondCommit = commit("Second Commit").toBranch(branch);
    final Hash thirdCommit = commit("Third Commit").toBranch(branch);

    store()
        .create(
            BranchName.of(thirdCommit.asString()), Optional.of(branch), Optional.of(firstCommit));
    store()
        .create(TagName.of(secondCommit.asString()), Optional.of(branch), Optional.of(firstCommit));

    assertThat(store().toRef(secondCommit.asString()))
        .isEqualTo(WithHash.of(firstCommit, TagName.of(secondCommit.asString())));
    assertThat(store().toRef(thirdCommit.asString()))
        .isEqualTo(WithHash.of(firstCommit, BranchName.of(thirdCommit.asString())));
    // Is it correct to allow a reference with the sentinel reference?
    // assertThat(store().toRef(initialCommit.asString()), is(WithHash.of(initialCommit,
    // initialCommit)));
    assertThrows(ReferenceNotFoundException.class, () -> store().toRef("unknown-ref"));
    assertThrows(ReferenceNotFoundException.class, () -> store().toRef("1234567890abcdef"));
  }

  @Test
  protected void checkDiff() throws VersionStoreException {
    final BranchName branch = BranchName.of("checkDiff");
    store().create(branch, Optional.empty(), Optional.empty());
    final Hash initial = store().toHash(branch);

    final Hash firstCommit =
        commit("First Commit").put("k1", "v1").put("k2", "v2").toBranch(branch);
    final Hash secondCommit =
        commit("Second Commit").put("k2", "v2a").put("k3", "v3").toBranch(branch);

    List<Diff<String>> startToSecond =
        store()
            .getDiffs(branch, Optional.of(initial), branch, Optional.of(secondCommit))
            .collect(Collectors.toList());
    assertThat(startToSecond)
        .containsExactlyInAnyOrder(
            Diff.of(Key.of("k1"), Optional.empty(), Optional.of("v1")),
            Diff.of(Key.of("k2"), Optional.empty(), Optional.of("v2a")),
            Diff.of(Key.of("k3"), Optional.empty(), Optional.of("v3")));

    List<Diff<String>> secondToStart =
        store()
            .getDiffs(branch, Optional.of(secondCommit), branch, Optional.of(initial))
            .collect(Collectors.toList());
    assertThat(secondToStart)
        .containsExactlyInAnyOrder(
            Diff.of(Key.of("k1"), Optional.of("v1"), Optional.empty()),
            Diff.of(Key.of("k2"), Optional.of("v2a"), Optional.empty()),
            Diff.of(Key.of("k3"), Optional.of("v3"), Optional.empty()));

    List<Diff<String>> firstToSecond =
        store()
            .getDiffs(branch, Optional.of(firstCommit), branch, Optional.of(secondCommit))
            .collect(Collectors.toList());
    assertThat(firstToSecond)
        .containsExactlyInAnyOrder(
            Diff.of(Key.of("k2"), Optional.of("v2"), Optional.of("v2a")),
            Diff.of(Key.of("k3"), Optional.empty(), Optional.of("v3")));

    List<Diff<String>> firstToFirst =
        store()
            .getDiffs(branch, Optional.of(firstCommit), branch, Optional.of(firstCommit))
            .collect(Collectors.toList());
    assertTrue(firstToFirst.isEmpty());
  }

  @Test
  void checkValueEntityType() throws Exception {
    BranchName branch = BranchName.of("entity-types");
    store().create(branch, Optional.empty(), Optional.empty());

    // have to do this here as tiered store stores payload at commit time
    Mockito.doReturn(StringSerializer.TestEnum.NO)
        .when(StringSerializer.getInstance())
        .getType("world");
    store()
        .commit(
            branch, Optional.empty(), "metadata", ImmutableList.of(Put.of(Key.of("hi"), "world")));

    assertEquals("world", store().getValue(branch, Optional.empty(), Key.of("hi")));
    List<Optional<String>> values =
        store().getValues(branch, Optional.empty(), Lists.newArrayList(Key.of("hi")));
    assertEquals(1, values.size());
    assertTrue(values.get(0).isPresent());

    // have to do this here as non-tiered store reads payload when getKeys is called
    Mockito.doReturn(StringSerializer.TestEnum.NO)
        .when(StringSerializer.getInstance())
        .getType("world");
    List<WithType<Key, StringSerializer.TestEnum>> keys;
    try (Stream<WithType<Key, StringSerializer.TestEnum>> k =
        store().getKeys(branch, Optional.empty())) {
      keys = k.collect(Collectors.toList());
    }

    assertEquals(1, keys.size());
    assertEquals(Key.of("hi"), keys.get(0).getValue());
    assertEquals(StringSerializer.TestEnum.NO, keys.get(0).getType());
  }

  private static class ReferenceNotFoundFunction {

    final String name;
    String msg;
    ThrowingFunction setup;
    ThrowingFunction function;

    ReferenceNotFoundFunction(String name) {
      this.name = name;
    }

    ReferenceNotFoundFunction setup(ThrowingFunction setup) {
      this.setup = setup;
      return this;
    }

    ReferenceNotFoundFunction function(ThrowingFunction function) {
      this.function = function;
      return this;
    }

    ReferenceNotFoundFunction msg(String msg) {
      this.msg = msg;
      return this;
    }

    @Override
    public String toString() {
      return name;
    }

    @FunctionalInterface
    interface ThrowingFunction {
      void run(VersionStore<String, String, String, StringSerializer.TestEnum> store)
          throws VersionStoreException;
    }
  }

  static List<ReferenceNotFoundFunction> referenceNotFoundFunctions() {
    return Arrays.asList(
        // getCommits()
        new ReferenceNotFoundFunction("getCommits/branch")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(
                s ->
                    s.getCommits(
                        BranchName.of("this-one-should-not-exist"),
                        Optional.empty(),
                        Optional.empty())),
        new ReferenceNotFoundFunction("getCommits/tag")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(
                s ->
                    s.getCommits(
                        TagName.of("this-one-should-not-exist"),
                        Optional.empty(),
                        Optional.empty())),
        new ReferenceNotFoundFunction("getCommits/hash")
            .msg(
                "Could not find commit '12341234123412341234123412341234123412341234' in reference 'main'.")
            .function(
                s ->
                    s.getCommits(
                        BranchName.of("main"),
                        Optional.of(Hash.of("12341234123412341234123412341234123412341234")),
                        Optional.empty())),
        // getValue()
        new ReferenceNotFoundFunction("getValue/branch")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(
                s ->
                    s.getValue(
                        BranchName.of("this-one-should-not-exist"),
                        Optional.empty(),
                        Key.of("foo"))),
        new ReferenceNotFoundFunction("getValue/tag")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(
                s ->
                    s.getValue(
                        TagName.of("this-one-should-not-exist"), Optional.empty(), Key.of("foo"))),
        new ReferenceNotFoundFunction("getValue/hash")
            .msg(
                "Could not find commit '12341234123412341234123412341234123412341234' in reference 'main'.")
            .function(
                s ->
                    s.getValue(
                        BranchName.of("main"),
                        Optional.of(Hash.of("12341234123412341234123412341234123412341234")),
                        Key.of("foo"))),
        // getValues()
        new ReferenceNotFoundFunction("getValues/branch")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(
                s ->
                    s.getValues(
                        BranchName.of("this-one-should-not-exist"),
                        Optional.empty(),
                        Collections.singletonList(Key.of("foo")))),
        new ReferenceNotFoundFunction("getValues/tag")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(
                s ->
                    s.getValues(
                        TagName.of("this-one-should-not-exist"),
                        Optional.empty(),
                        Collections.singletonList(Key.of("foo")))),
        new ReferenceNotFoundFunction("getValues/hash")
            .msg(
                "Could not find commit '12341234123412341234123412341234123412341234' in reference 'main'.")
            .function(
                s ->
                    s.getValues(
                        BranchName.of("main"),
                        Optional.of(Hash.of("12341234123412341234123412341234123412341234")),
                        Collections.singletonList(Key.of("foo")))),
        // getKeys()
        new ReferenceNotFoundFunction("getKeys/branch")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(s -> s.getKeys(BranchName.of("this-one-should-not-exist"), Optional.empty())),
        new ReferenceNotFoundFunction("getKeys/tag")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(s -> s.getKeys(TagName.of("this-one-should-not-exist"), Optional.empty())),
        new ReferenceNotFoundFunction("getKeys/hash")
            .msg(
                "Could not find commit '12341234123412341234123412341234123412341234' in reference 'main'.")
            .function(
                s ->
                    s.getKeys(
                        BranchName.of("main"),
                        Optional.of(Hash.of("12341234123412341234123412341234123412341234")))),
        // assign()
        new ReferenceNotFoundFunction("assign/branch/ok")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(
                s ->
                    s.assign(
                        BranchName.of("this-one-should-not-exist"),
                        Optional.empty(),
                        BranchName.of("main"),
                        Optional.empty())),
        new ReferenceNotFoundFunction("assign/ok/branch")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(
                s ->
                    s.assign(
                        BranchName.of("main"),
                        Optional.empty(),
                        BranchName.of("this-one-should-not-exist"),
                        Optional.empty())),
        new ReferenceNotFoundFunction("assign/hash")
            .msg(
                "Could not find commit '12341234123412341234123412341234123412341234' in reference 'main'.")
            .function(
                s ->
                    s.assign(
                        BranchName.of("main"),
                        Optional.empty(),
                        BranchName.of("main"),
                        Optional.of(Hash.of("12341234123412341234123412341234123412341234")))),
        // delete()
        new ReferenceNotFoundFunction("delete/branch")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(s -> s.delete(BranchName.of("this-one-should-not-exist"), Optional.empty())),
        new ReferenceNotFoundFunction("delete/tag")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(s -> s.delete(BranchName.of("this-one-should-not-exist"), Optional.empty())),
        // create()
        new ReferenceNotFoundFunction("create/branch")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(
                s ->
                    s.create(
                        BranchName.of("foo"),
                        Optional.of(BranchName.of("this-one-should-not-exist")),
                        Optional.empty())),
        new ReferenceNotFoundFunction("create/tag")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(
                s ->
                    s.create(
                        BranchName.of("foo"),
                        Optional.of(TagName.of("this-one-should-not-exist")),
                        Optional.empty())),
        new ReferenceNotFoundFunction("create/hash")
            .msg(
                "Could not find commit '12341234123412341234123412341234123412341234' in reference 'main'.")
            .function(
                s ->
                    s.create(
                        BranchName.of("foo"),
                        Optional.of(BranchName.of("main")),
                        Optional.of(Hash.of("12341234123412341234123412341234123412341234")))),
        // commit()
        new ReferenceNotFoundFunction("commit/branch")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(
                s ->
                    s.commit(
                        BranchName.of("this-one-should-not-exist"),
                        Optional.empty(),
                        "meta",
                        Collections.singletonList(Delete.of(Key.of("meep"))))),
        new ReferenceNotFoundFunction("commit/hash")
            .msg(
                "Could not find commit '12341234123412341234123412341234123412341234' in reference 'main'.")
            .function(
                s ->
                    s.commit(
                        BranchName.of("main"),
                        Optional.of(Hash.of("12341234123412341234123412341234123412341234")),
                        "meta",
                        Collections.singletonList(Delete.of(Key.of("meep"))))),
        // getDiffs()
        new ReferenceNotFoundFunction("getDiffs/branch/ok")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(
                s ->
                    s.getDiffs(
                        BranchName.of("this-one-should-not-exist"),
                        Optional.empty(),
                        BranchName.of("main"),
                        Optional.empty())),
        new ReferenceNotFoundFunction("getDiffs/ok/branch")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(
                s ->
                    s.getDiffs(
                        BranchName.of("main"),
                        Optional.empty(),
                        BranchName.of("this-one-should-not-exist"),
                        Optional.empty())),
        new ReferenceNotFoundFunction("getDiffs/tag/ok")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(
                s ->
                    s.getDiffs(
                        TagName.of("this-one-should-not-exist"),
                        Optional.empty(),
                        BranchName.of("main"),
                        Optional.empty())),
        new ReferenceNotFoundFunction("getDiffs/ok/tag")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(
                s ->
                    s.getDiffs(
                        BranchName.of("main"),
                        Optional.empty(),
                        TagName.of("this-one-should-not-exist"),
                        Optional.empty())),
        new ReferenceNotFoundFunction("getDiffs/hash/empty")
            .msg(
                "Could not find commit '12341234123412341234123412341234123412341234' in reference 'main'.")
            .function(
                s ->
                    s.getDiffs(
                        BranchName.of("main"),
                        Optional.of(Hash.of("12341234123412341234123412341234123412341234")),
                        BranchName.of("main"),
                        Optional.empty())),
        new ReferenceNotFoundFunction("getDiffs/empty/hash")
            .msg(
                "Could not find commit '12341234123412341234123412341234123412341234' in reference 'main'.")
            .function(
                s ->
                    s.getDiffs(
                        BranchName.of("main"),
                        Optional.empty(),
                        BranchName.of("main"),
                        Optional.of(Hash.of("12341234123412341234123412341234123412341234")))),
        // transplant()
        new ReferenceNotFoundFunction("transplant/branch/ok")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(
                s ->
                    s.transplant(
                        BranchName.of("this-one-should-not-exist"),
                        Optional.empty(),
                        BranchName.of("main"),
                        Collections.singletonList(
                            Hash.of("12341234123412341234123412341234123412341234")))),
        new ReferenceNotFoundFunction("transplant/ok/branch")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(
                s ->
                    s.transplant(
                        BranchName.of("main"),
                        Optional.empty(),
                        BranchName.of("this-one-should-not-exist"),
                        Collections.singletonList(
                            Hash.of("12341234123412341234123412341234123412341234")))),
        new ReferenceNotFoundFunction("transplant/ok/tag")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(
                s ->
                    s.transplant(
                        BranchName.of("main"),
                        Optional.empty(),
                        TagName.of("this-one-should-not-exist"),
                        Collections.singletonList(
                            Hash.of("12341234123412341234123412341234123412341234")))),
        new ReferenceNotFoundFunction("transplant/hash/empty")
            .msg(
                "Could not find commit '12341234123412341234123412341234123412341234' in reference 'main'.")
            .function(
                s ->
                    s.transplant(
                        BranchName.of("main"),
                        Optional.of(Hash.of("12341234123412341234123412341234123412341234")),
                        BranchName.of("main"),
                        Collections.singletonList(
                            Hash.of("12341234123412341234123412341234123412341234")))),
        new ReferenceNotFoundFunction("transplant/empty/hash")
            .msg(
                "Could not find commit '12341234123412341234123412341234123412341234' in reference 'main'.")
            .function(
                s ->
                    s.transplant(
                        BranchName.of("main"),
                        Optional.empty(),
                        BranchName.of("main"),
                        Collections.singletonList(
                            Hash.of("12341234123412341234123412341234123412341234")))),
        // merge()
        new ReferenceNotFoundFunction("merge/branch/ok")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(
                s ->
                    s.merge(
                        BranchName.of("this-one-should-not-exist"),
                        Optional.empty(),
                        BranchName.of("main"),
                        Optional.empty(),
                        false)),
        new ReferenceNotFoundFunction("merge/ok/branch")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(
                s ->
                    s.merge(
                        BranchName.of("main"),
                        Optional.empty(),
                        BranchName.of("this-one-should-not-exist"),
                        Optional.of(Hash.of("12341234123412341234123412341234123412341234")),
                        false)),
        new ReferenceNotFoundFunction("merge/hash/empty")
            .msg(
                "Could not find commit '12341234123412341234123412341234123412341234' in reference 'main'.")
            .function(
                s ->
                    s.merge(
                        BranchName.of("main"),
                        Optional.of(Hash.of("12341234123412341234123412341234123412341234")),
                        BranchName.of("main"),
                        Optional.empty(),
                        false)),
        new ReferenceNotFoundFunction("merge/empty/hash")
            .msg(
                "Could not find commit '12341234123412341234123412341234123412341234' in reference 'main'.")
            .function(
                s ->
                    s.merge(
                        BranchName.of("main"),
                        Optional.empty(),
                        BranchName.of("main"),
                        Optional.of(Hash.of("12341234123412341234123412341234123412341234")),
                        false))
        //
        );
  }

  @ParameterizedTest
  @MethodSource("referenceNotFoundFunctions")
  void referenceNotFound(ReferenceNotFoundFunction f) throws Exception {
    if (f.setup != null) {
      f.setup.run(store());
    }
    assertThatThrownBy(() -> f.function.run(store()))
        .isInstanceOf(ReferenceNotFoundException.class)
        .hasMessage(f.msg);
  }

  protected CommitBuilder<String, String, String, StringSerializer.TestEnum> forceCommit(
      String message) {
    return new CommitBuilder<>(store()).withMetadata(message);
  }

  protected CommitBuilder<String, String, String, StringSerializer.TestEnum> commit(
      String message) {
    return new CommitBuilder<>(store()).withMetadata(message).fromLatest();
  }

  protected Put<String, String> put(String key, String value) {
    return Put.of(Key.of(key), value);
  }

  protected Delete<String, String> delete(String key) {
    return Delete.of(Key.of(key));
  }

  protected Unchanged<String, String> unchanged(String key) {
    return Unchanged.of(Key.of(key));
  }

  protected List<WithHash<String>> commitsList(
      NamedRef ref, Optional<Hash> offset, Optional<Hash> untilExcluding)
      throws ReferenceNotFoundException {
    return commitsList(ref, offset, untilExcluding, Function.identity());
  }

  protected <T> List<T> commitsList(
      NamedRef ref,
      Optional<Hash> offset,
      Optional<Hash> untilExcluding,
      Function<Stream<WithHash<String>>, Stream<T>> streamFunction)
      throws ReferenceNotFoundException {
    try (Stream<WithHash<String>> s = store().getCommits(ref, offset, untilExcluding)) {
      return streamFunction.apply(s).collect(Collectors.toList());
    }
  }
}
