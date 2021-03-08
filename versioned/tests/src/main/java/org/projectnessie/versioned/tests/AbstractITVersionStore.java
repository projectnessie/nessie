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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Delete;
import org.projectnessie.versioned.Diff;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.Operation;
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
import org.projectnessie.versioned.WithPayload;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Base class used for integration tests against version store implementations.
 */
public abstract class AbstractITVersionStore {

  protected abstract VersionStore<String, String> store();

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
    store().create(branch, Optional.empty());
    final Hash hash = store().toHash(branch);
    assertThat(hash, is(notNullValue()));

    final BranchName anotherBranch = BranchName.of("bar");
    store().create(anotherBranch, Optional.of(hash));
    final Hash commitHash = commit("Some Commit").toBranch(anotherBranch);

    final BranchName anotherAnotherBranch = BranchName.of("baz");
    store().create(anotherAnotherBranch, Optional.of(commitHash));

    List<WithHash<NamedRef>> namedRefs;
    try (Stream<WithHash<NamedRef>> str = store().getNamedRefs()) {
      namedRefs = str.collect(Collectors.toList());
    }
    assertThat(namedRefs, containsInAnyOrder(
        WithHash.of(hash, branch),
        WithHash.of(commitHash, anotherBranch),
        WithHash.of(commitHash, anotherAnotherBranch)
    ));

    assertThat(store().getCommits(branch).count(), is(0L));
    assertThat(store().getCommits(anotherBranch).count(), is(1L));
    assertThat(store().getCommits(anotherAnotherBranch).count(), is(1L));
    assertThat(store().getCommits(hash).count(), is(0L)); // empty commit should not be listed
    assertThat(store().getCommits(commitHash).count(), is(1L)); // empty commit should not be listed

    assertThrows(ReferenceAlreadyExistsException.class, () -> store().create(branch, Optional.empty()));
    assertThrows(ReferenceAlreadyExistsException.class, () -> store().create(branch, Optional.of(hash)));

    store().delete(branch, Optional.of(hash));
    assertThrows(ReferenceNotFoundException.class, () -> store().toHash(branch));
    try (Stream<WithHash<NamedRef>> str = store().getNamedRefs()) {
      assertThat(str.count(), is(2L)); // bar + baz
    }
    assertThrows(ReferenceNotFoundException.class, () -> store().delete(branch, Optional.of(hash)));
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
    store().create(branch, Optional.empty());

    final Hash initialHash = store().toHash(branch);
    final Hash commitHash = commit("Some commit").toBranch(branch);

    final TagName tag = TagName.of("tag");
    store().create(tag, Optional.of(initialHash));

    final TagName anotherTag = TagName.of("another-tag");
    store().create(anotherTag, Optional.of(commitHash));

    assertThrows(ReferenceAlreadyExistsException.class, () -> store().create(tag, Optional.of(initialHash)));
    assertThrows(IllegalArgumentException.class, () -> store().create(tag,  Optional.empty()));

    assertThat(store().toHash(tag), is(initialHash));
    assertThat(store().toHash(anotherTag), is(commitHash));

    List<WithHash<NamedRef>> namedRefs;
    try (Stream<WithHash<NamedRef>> str = store().getNamedRefs()) {
      namedRefs = str.collect(Collectors.toList());
    }
    assertThat(namedRefs, containsInAnyOrder(
        WithHash.of(commitHash, branch),
        WithHash.of(initialHash, tag),
        WithHash.of(commitHash, anotherTag)));

    assertThat(store().getCommits(tag).count(), is(0L));
    assertThat(store().getCommits(initialHash).count(), is(0L)); // empty commit should not be listed

    assertThat(store().getCommits(anotherTag).count(), is(1L));
    assertThat(store().getCommits(commitHash).count(), is(1L)); // empty commit should not be listed

    store().delete(tag, Optional.of(initialHash));
    assertThrows(ReferenceNotFoundException.class, () -> store().toHash(tag));
    try (Stream<WithHash<NamedRef>> str = store().getNamedRefs()) {
      assertThat(str.count(), is(2L)); // foo + another-tag
    }
    assertThrows(ReferenceNotFoundException.class, () -> store().delete(tag, Optional.of(initialHash)));
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

    store().create(branch, Optional.empty());
    final Hash initialHash = store().toHash(branch);

    store().commit(branch, Optional.of(initialHash), "Some commit", Collections.emptyList());
    final Hash commitHash = store().toHash(branch);

    assertThat(commitHash, is(Matchers.not(initialHash)));
    store().commit(branch, Optional.of(initialHash), "Another commit", Collections.emptyList());
    final Hash anotherCommitHash = store().toHash(branch);

    assertThat(store().getCommits(branch).collect(Collectors.toList()), contains(
        WithHash.of(anotherCommitHash, "Another commit"),
        WithHash.of(commitHash, "Some commit")
    ));
    assertThat(store().getCommits(commitHash).collect(Collectors.toList()), contains(WithHash.of(commitHash, "Some commit")));

    assertThrows(ReferenceConflictException.class, () -> store().delete(branch, Optional.of(initialHash)));
    store().delete(branch, Optional.of(anotherCommitHash));
    assertThrows(ReferenceNotFoundException.class, () -> store().toHash(branch));
    try (Stream<WithHash<NamedRef>> str = store().getNamedRefs()) {
      assertThat(str.count(), is(0L));
    }
    assertThrows(ReferenceNotFoundException.class, () -> store().delete(branch, Optional.of(commitHash)));
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

    final Hash initialCommit = commit("Initial Commit")
        .put("t1", "v1_1")
        .put("t2", "v2_1")
        .put("t3", "v3_1")
        .toBranch(branch);

    final Hash secondCommit = commit("Second Commit")
        .put("t1", "v1_2")
        .delete("t2")
        .delete("t3")
        .put("t4", "v4_1")
        .toBranch(branch);

    final Hash thirdCommit = commit("Third Commit")
        .put("t2", "v2_2")
        .unchanged("t4")
        .toBranch(branch);

    assertThat(store().getCommits(branch).collect(Collectors.toList()), contains(
        WithHash.of(thirdCommit, "Third Commit"),
        WithHash.of(secondCommit, "Second Commit"),
        WithHash.of(initialCommit, "Initial Commit")
        ));

    assertThat(store().getKeys(branch).map(WithPayload::getValue).collect(Collectors.toList()), containsInAnyOrder(
        Key.of("t1"),
        Key.of("t2"),
        Key.of("t4")
        ));

    assertThat(store().getKeys(secondCommit).map(WithPayload::getValue).collect(Collectors.toList()), containsInAnyOrder(
        Key.of("t1"),
        Key.of("t4")
        ));

    assertThat(store().getKeys(initialCommit).map(WithPayload::getValue).collect(Collectors.toList()), containsInAnyOrder(
        Key.of("t1"),
        Key.of("t2"),
        Key.of("t3")
        ));

    assertThat(store().getValues(branch, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"), Key.of("t4"))),
        contains(
            Optional.of("v1_2"),
            Optional.of("v2_2"),
            Optional.empty(),
            Optional.of("v4_1")
        ));

    assertThat(store().getValues(secondCommit, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"), Key.of("t4"))),
        contains(
            Optional.of("v1_2"),
            Optional.empty(),
            Optional.empty(),
            Optional.of("v4_1")
        ));

    assertThat(store().getValues(initialCommit, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"), Key.of("t4"))),
        contains(
            Optional.of("v1_1"),
            Optional.of("v2_1"),
            Optional.of("v3_1"),
            Optional.empty()
        ));

    assertThat(store().getValue(branch, Key.of("t1")), is("v1_2"));
    assertThat(store().getValue(branch, Key.of("t2")), is("v2_2"));
    assertThat(store().getValue(branch, Key.of("t3")), is(nullValue()));
    assertThat(store().getValue(branch, Key.of("t4")), is("v4_1"));

    assertThat(store().getValue(secondCommit, Key.of("t1")), is("v1_2"));
    assertThat(store().getValue(secondCommit, Key.of("t2")), is(nullValue()));
    assertThat(store().getValue(secondCommit, Key.of("t3")), is(nullValue()));
    assertThat(store().getValue(secondCommit, Key.of("t4")), is("v4_1"));

    assertThat(store().getValue(initialCommit, Key.of("t1")), is("v1_1"));
    assertThat(store().getValue(initialCommit, Key.of("t2")), is("v2_1"));
    assertThat(store().getValue(initialCommit, Key.of("t3")), is("v3_1"));
    assertThat(store().getValue(initialCommit, Key.of("t4")), is(nullValue()));
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

    final Hash initialCommit = commit("Initial Commit")
        .put("t1", "v1_1")
        .put("t2", "v2_1")
        .put("t3", "v3_1")
        .toBranch(branch);

    final Hash t1Commit = commit("T1 Commit").fromReference(initialCommit).put("t1", "v1_2").toBranch(branch);
    final Hash t2Commit = commit("T2 Commit").fromReference(initialCommit).delete("t2").toBranch(branch);
    final Hash t3Commit = commit("T3 Commit").fromReference(initialCommit).unchanged("t3").toBranch(branch);
    final Hash extraCommit = commit("Extra Commit").fromReference(t1Commit).put("t1", "v1_3").put("t3", "v3_2").toBranch(branch);
    final Hash newT2Commit = commit("New T2 Commit").fromReference(t2Commit).put("t2", "new_v2_1").toBranch(branch);

    assertThat(store().getCommits(branch).collect(Collectors.toList()), contains(
        WithHash.of(newT2Commit, "New T2 Commit"),
        WithHash.of(extraCommit, "Extra Commit"),
        WithHash.of(t3Commit, "T3 Commit"),
        WithHash.of(t2Commit, "T2 Commit"),
        WithHash.of(t1Commit, "T1 Commit"),
        WithHash.of(initialCommit, "Initial Commit")
        ));

    assertThat(store().getKeys(branch).map(WithPayload::getValue).collect(Collectors.toList()), containsInAnyOrder(
        Key.of("t1"),
        Key.of("t2"),
        Key.of("t3")
        ));

    assertThat(store().getValues(branch, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"))),
        contains(
            Optional.of("v1_3"),
            Optional.of("new_v2_1"),
            Optional.of("v3_2")
        ));

    assertThat(store().getValues(newT2Commit, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"))),
        contains(
            Optional.of("v1_3"),
            Optional.of("new_v2_1"),
            Optional.of("v3_2")
        ));

    assertThat(store().getValues(extraCommit, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"))),
        contains(
            Optional.of("v1_3"),
            Optional.empty(),
            Optional.of("v3_2")
        ));

    assertThat(store().getValues(t3Commit, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"))),
        contains(
            Optional.of("v1_2"),
            Optional.empty(),
            Optional.of("v3_1")
        ));

    assertThat(store().getValues(t2Commit, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"))),
        contains(
            Optional.of("v1_2"),
            Optional.empty(),
            Optional.of("v3_1")
        ));

    assertThat(store().getValues(t1Commit, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"))),
        contains(
            Optional.of("v1_2"),
            Optional.of("v2_1"),
            Optional.of("v3_1")
        ));
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

    final Hash initialCommit = commit("Initial Commit")
        .put("t1", "v1_1")
        .put("t2", "v2_1")
        .toBranch(branch);

    final Hash secondCommit = commit("Second Commit")
        .put("t1", "v1_2")
        .delete("t2")
        .put("t3", "v3_1")
        .toBranch(branch);

    assertThrows(ReferenceConflictException.class,
        () -> commit("Conflicting Commit").fromReference(initialCommit).put("t1", "v1_3").toBranch(branch));
    assertThrows(ReferenceConflictException.class,
        () -> commit("Conflicting Commit").fromReference(initialCommit).put("t2", "v2_2").toBranch(branch));
    assertThrows(ReferenceConflictException.class,
        () -> commit("Conflicting Commit").fromReference(initialCommit).put("t3", "v3_2").toBranch(branch));

    assertThrows(ReferenceConflictException.class,
        () -> commit("Conflicting Commit").fromReference(initialCommit).delete("t1").toBranch(branch));
    assertThrows(ReferenceConflictException.class,
        () -> commit("Conflicting Commit").fromReference(initialCommit).delete("t2").toBranch(branch));
    assertThrows(ReferenceConflictException.class,
        () -> commit("Conflicting Commit").fromReference(initialCommit).delete("t3").toBranch(branch));

    // Checking the state hasn't changed
    assertThat(store().toHash(branch), is(secondCommit));
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

    commit("Initial Commit")
        .put("t1", "v1_1")
        .put("t2", "v2_1")
        .toBranch(branch);

    commit("Second Commit")
        .put("t1", "v1_2")
        .delete("t2")
        .put("t3", "v3_1")
        .toBranch(branch);

    final Hash putCommit = forceCommit("Conflicting Commit")
        .put("t1", "v1_3")
        .put("t2", "v2_2")
        .put("t3", "v3_2")
        .toBranch(branch);

    assertThat(store().toHash(branch), is(putCommit));
    assertThat(store().getValues(branch, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"))),
        contains(
            Optional.of("v1_3"),
            Optional.of("v2_2"),
            Optional.of("v3_2")
        ));

    final Hash unchangedCommit = commit("Conflicting Commit")
        .unchanged("t1")
        .unchanged("t2")
        .unchanged("t3")
        .toBranch(branch);
    assertThat(store().toHash(branch), is(unchangedCommit));
    assertThat(store().getValues(branch, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"))),
        contains(
            Optional.of("v1_3"),
            Optional.of("v2_2"),
            Optional.of("v3_2")
        ));

    final Hash deleteCommit = commit("Conflicting Commit")
        .delete("t1")
        .delete("t2")
        .delete("t3")
        .toBranch(branch);
    assertThat(store().toHash(branch), is(deleteCommit));
    assertThat(store().getValues(branch, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"))),
        contains(
            Optional.empty(),
            Optional.empty(),
            Optional.empty()
        ));
  }

  /*
   * Test:
   *  - Check that store allows storing the same value under different keys
   */
  @Test
  public void commitDuplicateValues() throws Exception {
    BranchName branch = BranchName.of("dupe-values");
    store().create(branch, Optional.empty());
    store().commit(branch, Optional.empty(), "metadata", ImmutableList.of(
        put("keyA", "foo"),
        put("keyB", "foo"))
    );

    assertThat(store().getValue(branch, Key.of("keyA")), is("foo"));
    assertThat(store().getValue(branch, Key.of("keyB")), is("foo"));
  }

  /*
   * Test:
   * - Check that store throws RNFE if branch doesn't exist
   */
  @Test
  public void commitWithInvalidBranch() {
    final BranchName branch = BranchName.of("unknown");

    assertThrows(ReferenceNotFoundException.class,
        () -> store().commit(branch, Optional.empty(), "New commit", Collections.emptyList()));
  }

  /*
   * Test:
   * - Check that store throws RNFE if reference hash doesn't exist
   */
  @Test
  public void commitWithUnknownReference() throws ReferenceNotFoundException, ReferenceAlreadyExistsException {
    final BranchName branch = BranchName.of("foo");
    store().create(branch, Optional.empty());

    assertThrows(ReferenceNotFoundException.class,
        () -> store().commit(branch, Optional.of(Hash.of("1234567890abcdef")), "New commit", Collections.emptyList()));
  }

  /*
   * Test:
   * - Check that store throws IllegalArgumentException if reference hash is not in branch ancestry
   */
  @Test
  public void commitWithInvalidReference() throws ReferenceNotFoundException, ReferenceConflictException, ReferenceAlreadyExistsException {
    final BranchName branch = BranchName.of("foo");
    store().create(branch, Optional.empty());


    final Hash initialHash = store().toHash(branch);
    store().commit(branch, Optional.of(initialHash), "Some commit", Collections.emptyList());

    final Hash commitHash = store().toHash(branch);

    final BranchName branch2 = BranchName.of("bar");
    store().create(branch2, Optional.empty());

    assertThrows(ReferenceNotFoundException.class,
        () -> store().commit(branch2, Optional.of(commitHash), "Another commit", Collections.emptyList()));
  }

  @Test
  public void getValueForEmptyBranch() throws ReferenceNotFoundException, ReferenceAlreadyExistsException {
    BranchName branch = BranchName.of("empty-branch");
    store().create(branch, Optional.empty());
    final Hash hash = store().toHash(branch);

    assertThat(store().getValue(hash, Key.of("arbitrary")), is(nullValue()));
  }

  @Test
  public void assign() throws VersionStoreException {
    final BranchName branch = BranchName.of("foo");
    store().create(branch, Optional.empty());
    final Hash initialHash = store().toHash(branch);

    final Hash commit = commit("Some commit").toBranch(branch);
    store().create(BranchName.of("bar"), Optional.of(commit));
    store().create(TagName.of("tag1"),  Optional.of(commit));
    store().create(TagName.of("tag2"),  Optional.of(commit));
    store().create(TagName.of("tag3"),  Optional.of(commit));

    final Hash anotherCommit = commit("Another commit").toBranch(branch);
    store().assign(TagName.of("tag2"), Optional.of(commit), anotherCommit);
    store().assign(TagName.of("tag3"), Optional.empty(), anotherCommit);

    assertThrows(ReferenceNotFoundException.class,
        () -> store().assign(BranchName.of("baz"), Optional.empty(), anotherCommit));
    assertThrows(ReferenceNotFoundException.class,
        () -> store().assign(TagName.of("unknowon-tag"), Optional.empty(), anotherCommit));

    assertThrows(ReferenceConflictException.class,
        () -> store().assign(TagName.of("tag1"), Optional.of(initialHash), commit));
    assertThrows(ReferenceConflictException.class,
        () -> store().assign(TagName.of("tag1"), Optional.of(initialHash), anotherCommit));
    assertThrows(ReferenceNotFoundException.class,
        () -> store().assign(TagName.of("tag1"), Optional.of(commit), Hash.of("1234567890abcdef")));

    assertThat(store().getCommits(branch).collect(Collectors.toList()), contains(
        WithHash.of(anotherCommit, "Another commit"),
        WithHash.of(commit, "Some commit")
    ));

    assertThat(store().getCommits(BranchName.of("bar")).collect(Collectors.toList()), contains(
        WithHash.of(commit, "Some commit")
    ));

    assertThat(store().getCommits(TagName.of("tag1")).collect(Collectors.toList()), contains(
        WithHash.of(commit, "Some commit")
    ));

    assertThat(store().getCommits(TagName.of("tag2")).collect(Collectors.toList()), contains(
        WithHash.of(anotherCommit, "Another commit"),
        WithHash.of(commit, "Some commit")
    ));
  }

  @Nested
  @DisplayName("when transplanting")
  protected class WhenTransplanting {
    private Hash initialHash;
    private Hash firstCommit;
    private Hash secondCommit;
    private Hash thirdCommit;

    @BeforeEach
    protected void setupCommits() throws VersionStoreException {
      final BranchName branch = BranchName.of("foo");
      store().create(branch, Optional.empty());

      initialHash = store().toHash(branch);

      firstCommit = commit("Initial Commit")
          .put("t1", "v1_1")
          .put("t2", "v2_1")
          .put("t3", "v3_1")
          .toBranch(branch);

      secondCommit = commit("Second Commit")
          .put("t1", "v1_2")
          .delete("t2")
          .delete("t3")
          .put("t4", "v4_1")
          .toBranch(branch);

      thirdCommit = commit("Third Commit")
          .put("t2", "v2_2")
          .unchanged("t4")
          .toBranch(branch);
    }

    @Test
    protected void checkTransplantOnEmptyBranch() throws VersionStoreException {
      final BranchName newBranch = BranchName.of("bar_1");
      store().create(newBranch, Optional.empty());

      store().transplant(newBranch, Optional.of(initialHash), Arrays.asList(firstCommit, secondCommit, thirdCommit));
      assertThat(store().getValues(newBranch, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"), Key.of("t4"))),
          contains(
              Optional.of("v1_2"),
              Optional.of("v2_2"),
              Optional.empty(),
              Optional.of("v4_1")
      ));
    }

    @Test
    protected void checkTransplantWithPreviousCommit() throws VersionStoreException {
      final BranchName newBranch = BranchName.of("bar_2");
      store().create(newBranch, Optional.empty());
      commit("Unrelated commit").put("t5", "v5_1").toBranch(newBranch);

      store().transplant(newBranch, Optional.of(initialHash), Arrays.asList(firstCommit, secondCommit, thirdCommit));
      assertThat(store().getValues(newBranch, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"), Key.of("t4"), Key.of("t5"))),
          contains(
              Optional.of("v1_2"),
              Optional.of("v2_2"),
              Optional.empty(),
              Optional.of("v4_1"),
              Optional.of("v5_1")
      ));
    }

    @Test
    protected void checkTransplantWitConflictingCommit() throws VersionStoreException {
      final BranchName newBranch = BranchName.of("bar_3");
      store().create(newBranch, Optional.empty());
      commit("Another commit").put("t1", "v1_4").toBranch(newBranch);

      assertThrows(ReferenceConflictException.class,
          () -> store().transplant(newBranch, Optional.of(initialHash), Arrays.asList(firstCommit, secondCommit, thirdCommit)));
    }

    @Test
    protected void checkTransplantWithDelete() throws VersionStoreException {
      final BranchName newBranch = BranchName.of("bar_4");
      store().create(newBranch, Optional.empty());
      commit("Another commit").put("t1", "v1_4").toBranch(newBranch);
      commit("Another commit").delete("t1").toBranch(newBranch);

      store().transplant(newBranch, Optional.of(initialHash), Arrays.asList(firstCommit, secondCommit, thirdCommit));
      assertThat(store().getValues(newBranch, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"), Key.of("t4"))),
          contains(
              Optional.of("v1_2"),
              Optional.of("v2_2"),
              Optional.empty(),
              Optional.of("v4_1")
      ));
    }

    @Test
    protected void checkTransplantOnNonExistingBranch() throws VersionStoreException {
      final BranchName newBranch = BranchName.of("bar_5");
      assertThrows(ReferenceNotFoundException.class,
          () -> store().transplant(newBranch, Optional.of(initialHash), Arrays.asList(firstCommit, secondCommit, thirdCommit)));
    }

    @Test
    protected void checkTransplantWithNonExistingCommit() throws VersionStoreException {
      final BranchName newBranch = BranchName.of("bar_6");
      store().create(newBranch, Optional.empty());
      assertThrows(ReferenceNotFoundException.class,
          () -> store().transplant(newBranch, Optional.of(initialHash), Arrays.asList(Hash.of("1234567890abcdef"))));
    }

    @Test
    protected void checkTransplantWithNoExpectedHash() throws VersionStoreException {
      final BranchName newBranch = BranchName.of("bar_7");
      store().create(newBranch, Optional.empty());
      commit("Another commit").put("t5", "v5_1").toBranch(newBranch);
      commit("Another commit").put("t1", "v1_4").toBranch(newBranch);

      store().transplant(newBranch, Optional.empty(), Arrays.asList(firstCommit, secondCommit, thirdCommit));
      assertThat(store().getValues(newBranch, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"), Key.of("t4"), Key.of("t5"))),
          contains(
              Optional.of("v1_2"),
              Optional.of("v2_2"),
              Optional.empty(),
              Optional.of("v4_1"),
              Optional.of("v5_1")
      ));
    }

    @Test
    protected void checkTransplantWithCommitsInWrongOrder() throws VersionStoreException {
      final BranchName newBranch = BranchName.of("bar_8");
      store().create(newBranch, Optional.empty());

      assertThrows(IllegalArgumentException.class,
          () -> store().transplant(newBranch, Optional.empty(), Arrays.asList(secondCommit, firstCommit, thirdCommit)));
    }

    @Test
    protected void checkInvalidBranchHash() throws VersionStoreException {
      final BranchName anotherBranch = BranchName.of("bar");
      store().create(anotherBranch, Optional.empty());
      final Hash unrelatedCommit = commit("Another Commit")
          .put("t1", "v1_1")
          .put("t2", "v2_1")
          .put("t3", "v3_1")
          .toBranch(anotherBranch);

      final BranchName newBranch = BranchName.of("bar_1");
      store().create(newBranch, Optional.empty());

      assertThrows(ReferenceNotFoundException.class,
          () -> store().transplant(newBranch, Optional.of(unrelatedCommit), Arrays.asList(firstCommit, secondCommit, thirdCommit)));
    }

    @Test
    protected void transplantBasic() throws VersionStoreException {
      final BranchName newBranch = BranchName.of("bar_2");
      store().create(newBranch, Optional.empty());
      commit("Unrelated commit").put("t5", "v5_1").toBranch(newBranch);

      store().transplant(newBranch, Optional.of(initialHash), Arrays.asList(firstCommit, secondCommit));
      assertThat(store().getValues(newBranch, Arrays.asList(Key.of("t1"), Key.of("t4"), Key.of("t5"))),
                 contains(
                   Optional.of("v1_2"),
                   Optional.of("v4_1"),
                   Optional.of("v5_1")
                 ));

    }
  }

  @Nested
  protected class WhenMerging {
    private Hash initialHash;
    private Hash firstCommit;
    private Hash secondCommit;
    private Hash thirdCommit;

    @BeforeEach
    protected void setupCommits() throws VersionStoreException {
      final BranchName branch = BranchName.of("foo");
      store().create(branch, Optional.empty());

      initialHash = store().toHash(branch);

      firstCommit = commit("First Commit").put("t1", "v1_1").put("t2", "v2_1").put("t3", "v3_1").toBranch(branch);
      secondCommit = commit("Second Commit").put("t1", "v1_2").delete("t2").delete("t3").put("t4", "v4_1").toBranch(branch);
      thirdCommit = commit("Third Commit").put("t2", "v2_2").unchanged("t4").toBranch(branch);
    }

    @Test
    protected void mergeIntoEmptyBranch() throws VersionStoreException {
      final BranchName newBranch = BranchName.of("bar_1");
      store().create(newBranch, Optional.empty());

      store().merge(thirdCommit, newBranch, Optional.of(initialHash));
      assertThat(store().getValues(newBranch, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"), Key.of("t4"))),
                 contains(
                   Optional.of("v1_2"),
                   Optional.of("v2_2"),
                   Optional.empty(),
                   Optional.of("v4_1")
                 ));

      assertThat(store().toHash(newBranch), is(thirdCommit));
    }

    @Test
    protected void mergeIntoNonConflictingBranch() throws VersionStoreException {
      final BranchName newBranch = BranchName.of("bar_2");
      store().create(newBranch, Optional.empty());
      final Hash newCommit = commit("Unrelated commit").put("t5", "v5_1").toBranch(newBranch);

      store().merge(thirdCommit, newBranch, Optional.empty());
      assertThat(store().getValues(newBranch, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"), Key.of("t4"), Key.of("t5"))),
                 contains(
                   Optional.of("v1_2"),
                   Optional.of("v2_2"),
                   Optional.empty(),
                   Optional.of("v4_1"),
                   Optional.of("v5_1")
                 ));

      final List<WithHash<String>> commits = store().getCommits(newBranch).collect(Collectors.toList());
      assertThat(commits.size(), is(4));
      assertThat(commits.get(3).getHash(), is(newCommit));
      assertThat(commits.get(2).getValue(), is("First Commit"));
      assertThat(commits.get(1).getValue(), is("Second Commit"));
      assertThat(commits.get(0).getValue(), is("Third Commit"));
    }

    @Test
    protected void nonEmptyFastForwardMerge() throws VersionStoreException {
      final Key key = Key.of("t1");
      final BranchName etl = BranchName.of("etl");
      final BranchName review = BranchName.of("review");
      store().create(etl, Optional.empty());
      store().create(review, Optional.empty());
      store().commit(etl, Optional.empty(), "commit 1", Arrays.<Operation<String>>asList(Put.of(key, "value1")));
      store().merge(store().toHash(etl), review, Optional.empty());
      store().commit(etl, Optional.empty(), "commit 2", Arrays.<Operation<String>>asList(Put.of(key, "value2")));
      store().merge(store().toHash(etl), review, Optional.empty());
      assertEquals(store().getValue(review, key), "value2");
    }

    @Test
    protected void mergeWithCommonAncestor() throws VersionStoreException {
      final BranchName newBranch = BranchName.of("bar_2");
      store().create(newBranch, Optional.of(firstCommit));

      final Hash newCommit = commit("Unrelated commit").put("t5", "v5_1").toBranch(newBranch);

      store().merge(thirdCommit, newBranch, Optional.empty());
      assertThat(store().getValues(newBranch, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"), Key.of("t4"), Key.of("t5"))),
                 contains(
                   Optional.of("v1_2"),
                   Optional.of("v2_2"),
                   Optional.empty(),
                   Optional.of("v4_1"),
                   Optional.of("v5_1")
                 ));

      final List<WithHash<String>> commits = store().getCommits(newBranch).collect(Collectors.toList());
      assertThat(commits.size(), is(4));
      assertThat(commits.get(3).getHash(), is(firstCommit));
      assertThat(commits.get(2).getHash(), is(newCommit));
      assertThat(commits.get(1).getValue(), is("Second Commit"));
      assertThat(commits.get(0).getValue(), is("Third Commit"));
    }

    @Test
    protected void mergeIntoConflictingBranch() throws VersionStoreException {
      final BranchName newBranch = BranchName.of("bar_3");
      store().create(newBranch, Optional.empty());
      commit("Another commit").put("t1", "v1_4").toBranch(newBranch);

      assertThrows(ReferenceConflictException.class,
          () -> store().merge(thirdCommit, newBranch, Optional.of(initialHash)));
    }

    @Test
    protected void mergeIntoNonExistingBranch() throws VersionStoreException {
      final BranchName newBranch = BranchName.of("bar_5");
      assertThrows(ReferenceNotFoundException.class,
          () -> store().merge(thirdCommit, newBranch, Optional.of(initialHash)));
    }

    @Test
    protected void mergeIntoNonExistingReference() throws VersionStoreException {
      final BranchName newBranch = BranchName.of("bar_6");
      store().create(newBranch, Optional.empty());
      assertThrows(ReferenceNotFoundException.class,
          () -> store().merge(Hash.of("1234567890abcdef"), newBranch, Optional.of(initialHash)));
    }
  }

  @Test
  void toRef() throws VersionStoreException {
    final BranchName branch = BranchName.of("main");
    store().create(branch, Optional.empty());
    store().toHash(branch);

    final Hash firstCommit = commit("First Commit").toBranch(branch);
    final Hash secondCommit = commit("Second Commit").toBranch(branch);
    final Hash thirdCommit = commit("Third Commit").toBranch(branch);

    store().create(BranchName.of(thirdCommit.asString()), Optional.of(firstCommit));
    store().create(TagName.of(secondCommit.asString()), Optional.of(firstCommit));

    assertThat(store().toRef(firstCommit.asString()), is(WithHash.of(firstCommit, firstCommit)));
    assertThat(store().toRef(secondCommit.asString()), is(WithHash.of(firstCommit, TagName.of(secondCommit.asString()))));
    assertThat(store().toRef(thirdCommit.asString()), is(WithHash.of(firstCommit, BranchName.of(thirdCommit.asString()))));
    // Is it correct to allow a reference with the sentinel reference?
    //assertThat(store().toRef(initialCommit.asString()), is(WithHash.of(initialCommit, initialCommit)));
    assertThrows(ReferenceNotFoundException.class, () -> store().toRef("unknown-ref"));
    assertThrows(ReferenceNotFoundException.class, () -> store().toRef("1234567890abcdef"));
  }

  @Test
  protected void checkDiff() throws VersionStoreException {
    final BranchName branch = BranchName.of("main");
    store().create(branch, Optional.empty());
    final Hash initial = store().toHash(branch);

    final Hash firstCommit = commit("First Commit").put("k1", "v1").put("k2", "v2").toBranch(branch);
    final Hash secondCommit = commit("Second Commit").put("k2", "v2a").put("k3", "v3").toBranch(branch);

    List<Diff<String>> startToSecond = store().getDiffs(initial, secondCommit).collect(Collectors.toList());
    assertThat(startToSecond, containsInAnyOrder(
        Diff.of(Key.of("k1"), Optional.empty(), Optional.of("v1")),
        Diff.of(Key.of("k2"), Optional.empty(), Optional.of("v2a")),
        Diff.of(Key.of("k3"), Optional.empty(), Optional.of("v3"))
    ));

    List<Diff<String>> secondToStart = store().getDiffs(secondCommit, initial).collect(Collectors.toList());
    assertThat(secondToStart, containsInAnyOrder(
        Diff.of(Key.of("k1"), Optional.of("v1"), Optional.empty()),
        Diff.of(Key.of("k2"), Optional.of("v2a"), Optional.empty()),
        Diff.of(Key.of("k3"), Optional.of("v3"), Optional.empty())
    ));

    List<Diff<String>> firstToSecond = store().getDiffs(firstCommit, secondCommit).collect(Collectors.toList());
    assertThat(firstToSecond, containsInAnyOrder(
        Diff.of(Key.of("k2"), Optional.of("v2"), Optional.of("v2a")),
        Diff.of(Key.of("k3"), Optional.empty(), Optional.of("v3"))
    ));

    List<Diff<String>> firstToFirst = store().getDiffs(firstCommit, firstCommit).collect(Collectors.toList());
    assertTrue(firstToFirst.isEmpty());
  }

  @Test
  void checkValueEntityType() throws Exception {
    BranchName branch = BranchName.of("entity-types");
    store().create(branch, Optional.empty());

    // have to do this here as tiered store stores payload at commit time
    Mockito.doReturn((byte) 24).when(StringSerializer.getInstance()).getPayload("world");
    store().commit(branch, Optional.empty(), "metadata", ImmutableList.of(
        Put.of(Key.of("hi"), "world"))
    );

    assertEquals("world", store().getValue(branch, Key.of("hi")));
    List<Optional<String>> values = store().getValues(branch, Lists.newArrayList(Key.of("hi")));
    assertEquals(1, values.size());
    assertTrue(values.get(0).isPresent());

    // have to do this here as non-tiered store reads payload when getKeys is called
    Mockito.doReturn((byte) 24).when(StringSerializer.getInstance()).getPayload("world");
    List<WithPayload<Key>> keys = store().getKeys(branch).collect(Collectors.toList());

    assertEquals(1, keys.size());
    assertEquals(Key.of("hi"), keys.get(0).getValue());
    assertEquals((byte)24, keys.get(0).getPayload());
  }


  protected CommitBuilder<String, String> forceCommit(String message) {
    return new CommitBuilder<>(store()).withMetadata(message);
  }

  protected CommitBuilder<String, String> commit(String message) {
    return new CommitBuilder<>(store()).withMetadata(message).fromLatest();
  }

  protected Put<String> put(String key, String value) {
    return Put.of(Key.of(key), value);
  }

  protected Delete<String> delete(String key) {
    return Delete.of(Key.of(key));
  }

  protected Unchanged<String> unchanged(String key) {
    return Unchanged.of(Key.of(key));
  }
}
