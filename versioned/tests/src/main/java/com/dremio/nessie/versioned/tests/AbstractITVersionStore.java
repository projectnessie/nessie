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
package com.dremio.nessie.versioned.tests;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import com.dremio.nessie.versioned.BranchName;
import com.dremio.nessie.versioned.Delete;
import com.dremio.nessie.versioned.Hash;
import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.NamedRef;
import com.dremio.nessie.versioned.Operation;
import com.dremio.nessie.versioned.Put;
import com.dremio.nessie.versioned.ReferenceAlreadyExistsException;
import com.dremio.nessie.versioned.ReferenceConflictException;
import com.dremio.nessie.versioned.ReferenceNotFoundException;
import com.dremio.nessie.versioned.TagName;
import com.dremio.nessie.versioned.Unchanged;
import com.dremio.nessie.versioned.VersionStore;
import com.dremio.nessie.versioned.VersionStoreException;
import com.dremio.nessie.versioned.WithHash;
import com.google.common.collect.ImmutableList;

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
    final Hash commitHash = addCommit(anotherBranch, "Some Commit");

    final BranchName anotherAnotherBranch = BranchName.of("baz");
    store().create(anotherAnotherBranch, Optional.of(commitHash));

    final List<WithHash<NamedRef>> namedRefs = store().getNamedRefs().collect(Collectors.toList());
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
    assertThat(store().getNamedRefs().count(), is(2L)); // bar + baz
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
    final Hash commitHash = addCommit(branch, "Some commit");

    final TagName tag = TagName.of("tag");
    store().create(tag, Optional.of(initialHash));

    final TagName anotherTag = TagName.of("another-tag");
    store().create(anotherTag, Optional.of(commitHash));

    assertThrows(ReferenceAlreadyExistsException.class, () -> store().create(tag, Optional.of(initialHash)));
    assertThrows(IllegalArgumentException.class, () -> store().create(tag,  Optional.empty()));

    assertThat(store().toHash(tag), is(initialHash));
    assertThat(store().toHash(anotherTag), is(commitHash));

    final List<WithHash<NamedRef>> namedRefs = store().getNamedRefs().collect(Collectors.toList());
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
    assertThat(store().getNamedRefs().count(), is(2L)); // foo + another-tag
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
    assertThat(store().getNamedRefs().count(), is(0L));
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

    final Hash initialCommit = addCommit(branch, "Initial Commit",
        Put.of(Key.of("t1"), "v1_1"),
        Put.of(Key.of("t2"), "v2_1"),
        Put.of(Key.of("t3"), "v3_1")
        );
    final Hash secondCommit = addCommit(branch, "Second Commit",
        Put.of(Key.of("t1"), "v1_2"),
        Delete.of(Key.of("t2")),
        Delete.of(Key.of("t3")),
        Put.of(Key.of("t4"), "v4_1")
        );
    final Hash thirdCommit = addCommit(branch, "Third Commit",
        Put.of(Key.of("t2"), "v2_2"),
        Unchanged.of(Key.of("t4"))
        );

    assertThat(store().getCommits(branch).collect(Collectors.toList()), contains(
        WithHash.of(thirdCommit, "Third Commit"),
        WithHash.of(secondCommit, "Second Commit"),
        WithHash.of(initialCommit, "Initial Commit")
        ));

    assertThat(store().getKeys(branch).collect(Collectors.toList()), containsInAnyOrder(
        Key.of("t1"),
        Key.of("t2"),
        Key.of("t4")
        ));

    assertThat(store().getKeys(secondCommit).collect(Collectors.toList()), containsInAnyOrder(
        Key.of("t1"),
        Key.of("t4")
        ));

    assertThat(store().getKeys(initialCommit).collect(Collectors.toList()), containsInAnyOrder(
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

    final Hash initialCommit = addCommit(branch, "Initial Commit",
        Put.of(Key.of("t1"), "v1_1"),
        Put.of(Key.of("t2"), "v2_1"),
        Put.of(Key.of("t3"), "v3_1")
        );

    final Hash t1Commit = addCommit(branch, initialCommit, "T1 Commit", Put.of(Key.of("t1"), "v1_2"));
    final Hash t2Commit = addCommit(branch, initialCommit, "T2 Commit", Delete.of(Key.of("t2")));
    final Hash t3Commit = addCommit(branch, initialCommit, "T3 Commit", Unchanged.of(Key.of("t3")));
    final Hash extraCommit = addCommit(branch, t1Commit, "Extra Commit", Put.of(Key.of("t1"), "v1_3"), Put.of(Key.of("t3"), "v3_2"));
    final Hash newT2Commit = addCommit(branch, t2Commit, "New T2 Commit", Put.of(Key.of("t2"), "new_v2_1"));

    assertThat(store().getCommits(branch).collect(Collectors.toList()), contains(
        WithHash.of(newT2Commit, "New T2 Commit"),
        WithHash.of(extraCommit, "Extra Commit"),
        WithHash.of(t3Commit, "T3 Commit"),
        WithHash.of(t2Commit, "T2 Commit"),
        WithHash.of(t1Commit, "T1 Commit"),
        WithHash.of(initialCommit, "Initial Commit")
        ));

    assertThat(store().getKeys(branch).collect(Collectors.toList()), containsInAnyOrder(
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

    final Hash initialCommit = addCommit(branch, "Initial Commit",
        Put.of(Key.of("t1"), "v1_1"),
        Put.of(Key.of("t2"), "v2_1")
        );

    final Hash secondCommit = addCommit(branch, "Second Commit",
        Put.of(Key.of("t1"), "v1_2"),
        Delete.of(Key.of("t2")),
        Put.of(Key.of("t3"), "v3_1")
        );

    assertThrows(ReferenceConflictException.class,
        () -> addCommit(branch, initialCommit, "Conflicting Commit", Put.of(Key.of("t1"), "v1_3")));
    assertThrows(ReferenceConflictException.class,
        () -> addCommit(branch, initialCommit, "Conflicting Commit", Put.of(Key.of("t2"), "v2_2")));
    assertThrows(ReferenceConflictException.class,
        () -> addCommit(branch, initialCommit, "Conflicting Commit", Put.of(Key.of("t3"), "v3_2")));

    assertThrows(ReferenceConflictException.class,
        () -> addCommit(branch, initialCommit, "Conflicting Commit", Delete.of(Key.of("t1"))));
    assertThrows(ReferenceConflictException.class,
        () -> addCommit(branch, initialCommit, "Conflicting Commit", Delete.of(Key.of("t2"))));
    assertThrows(ReferenceConflictException.class,
        () -> addCommit(branch, initialCommit, "Conflicting Commit", Delete.of(Key.of("t3"))));

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

    final Hash initialCommit = addCommit(branch, "Initial Commit",
        Put.of(Key.of("t1"), "v1_1"),
        Put.of(Key.of("t2"), "v2_1")
        );

    final Hash secondCommit = addCommit(branch, "Second Commit",
        Put.of(Key.of("t1"), "v1_2"),
        Delete.of(Key.of("t2")),
        Put.of(Key.of("t3"), "v3_1")
        );

    final Hash putCommit = forceCommit(branch, "Conflicting Commit",
            Put.of(Key.of("t1"), "v1_3"), Put.of(Key.of("t2"), "v2_2"),
            Put.of(Key.of("t3"), "v3_2"));
    assertThat(store().toHash(branch), is(putCommit));
    assertThat(store().getValues(branch, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"))),
        contains(
            Optional.of("v1_3"),
            Optional.of("v2_2"),
            Optional.of("v3_2")
        ));

    final Hash unchangedCommit = forceCommit(branch, "Conflicting Commit",
        Unchanged.of(Key.of("t1")), Unchanged.of(Key.of("t2")), Unchanged.of(Key.of("t3")));
    assertThat(store().toHash(branch), is(unchangedCommit));
    assertThat(store().getValues(branch, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"))),
        contains(
            Optional.of("v1_3"),
            Optional.of("v2_2"),
            Optional.of("v3_2")
        ));

    final Hash deleteCommit = forceCommit(branch, "Conflicting Commit", Delete.of(Key.of("t1")),
        Delete.of(Key.of("t2")), Delete.of(Key.of("t3")));
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
        Put.of(Key.of("keyA"), "foo"),
        Put.of(Key.of("keyB"), "foo"))
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

    final Hash commit = addCommit(branch, "Some commit");
    store().create(BranchName.of("bar"), Optional.of(commit));
    store().create(TagName.of("tag1"),  Optional.of(commit));
    store().create(TagName.of("tag2"),  Optional.of(commit));
    store().create(TagName.of("tag3"),  Optional.of(commit));

    final Hash anotherCommit = addCommit(branch, "Another commit");
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

  @Test
  protected void transplant() throws VersionStoreException {
    final BranchName branch = BranchName.of("foo");
    store().create(branch, Optional.empty());

    final Hash initialHash = store().toHash(branch);

    final Hash firstCommit = addCommit(branch, "First Commit",
        Put.of(Key.of("t1"), "v1_1"),
        Put.of(Key.of("t2"), "v2_1"),
        Put.of(Key.of("t3"), "v3_1")
        );
    final Hash secondCommit = addCommit(branch, "Second Commit",
        Put.of(Key.of("t1"), "v1_2"),
        Delete.of(Key.of("t2")),
        Delete.of(Key.of("t3")),
        Put.of(Key.of("t4"), "v4_1")
        );
    final Hash thirdCommit = addCommit(branch, "Third Commit",
        Put.of(Key.of("t2"), "v2_2"),
        Unchanged.of(Key.of("t4"))
        );

    {
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

    {
      final BranchName newBranch = BranchName.of("bar_2");
      store().create(newBranch, Optional.empty());
      addCommit(newBranch, "Unrelated commit", Put.of(Key.of("t5"), "v5_1"));

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

    {
      final BranchName newBranch = BranchName.of("bar_3");
      store().create(newBranch, Optional.empty());
      addCommit(newBranch, "Another commit", Put.of(Key.of("t1"), "v1_4"));

      assertThrows(ReferenceConflictException.class,
          () -> store().transplant(newBranch, Optional.of(initialHash), Arrays.asList(firstCommit, secondCommit, thirdCommit)));
    }

    {
      final BranchName newBranch = BranchName.of("bar_4");
      store().create(newBranch, Optional.empty());
      addCommit(newBranch, "Another commit", Put.of(Key.of("t1"), "v1_4"));
      addCommit(newBranch, "Another commit", Delete.of(Key.of("t1")));

      store().transplant(newBranch, Optional.of(initialHash), Arrays.asList(firstCommit, secondCommit, thirdCommit));
      assertThat(store().getValues(newBranch, Arrays.asList(Key.of("t1"), Key.of("t2"), Key.of("t3"), Key.of("t4"))),
          contains(
              Optional.of("v1_2"),
              Optional.of("v2_2"),
              Optional.empty(),
              Optional.of("v4_1")
      ));
    }

    {
      final BranchName newBranch = BranchName.of("bar_5");
      assertThrows(ReferenceNotFoundException.class,
          () -> store().transplant(newBranch, Optional.of(initialHash), Arrays.asList(firstCommit, secondCommit, thirdCommit)));
    }

    {
      final BranchName newBranch = BranchName.of("bar_6");
      store().create(newBranch, Optional.empty());
      assertThrows(ReferenceNotFoundException.class,
          () -> store().transplant(newBranch, Optional.of(initialHash), Arrays.asList(Hash.of("1234567890abcdef"))));
    }

    {
      final BranchName newBranch = BranchName.of("bar_7");
      store().create(newBranch, Optional.empty());
      addCommit(newBranch, "Another commit", Put.of(Key.of("t5"), "v5_1"));
      addCommit(newBranch, "Another commit", Put.of(Key.of("t1"), "v1_4"));

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

    {
      final BranchName newBranch = BranchName.of("bar_8");
      store().create(newBranch, Optional.empty());

      assertThrows(IllegalArgumentException.class,
          () -> store().transplant(newBranch, Optional.empty(), Arrays.asList(secondCommit, firstCommit, thirdCommit)));
    }
  }

  @Test
  protected void transplantInvalidReference() throws VersionStoreException {
    final BranchName branch = BranchName.of("foo");
    store().create(branch, Optional.empty());

    final Hash initialHash = store().toHash(branch);

    final Hash firstCommit = addCommit(branch, "First Commit",
        Put.of(Key.of("t1"), "v1_1"),
        Put.of(Key.of("t2"), "v2_1"),
        Put.of(Key.of("t3"), "v3_1")
        );
    final Hash secondCommit = addCommit(branch, "Second Commit",
        Put.of(Key.of("t1"), "v1_2"),
        Delete.of(Key.of("t2")),
        Delete.of(Key.of("t3")),
        Put.of(Key.of("t4"), "v4_1")
        );
    final Hash thirdCommit = addCommit(branch, "Third Commit",
        Put.of(Key.of("t2"), "v2_2"),
        Unchanged.of(Key.of("t4"))
        );

    final BranchName anotherBranch = BranchName.of("bar");
    store().create(anotherBranch, Optional.empty());
    final Hash unrelatedCommit = addCommit(anotherBranch, "Another Commit",
        Put.of(Key.of("t1"), "v1_1"),
        Put.of(Key.of("t2"), "v2_1"),
        Put.of(Key.of("t3"), "v3_1")
        );

      final BranchName newBranch = BranchName.of("bar_1");
      store().create(newBranch, Optional.empty());

      assertThrows(ReferenceNotFoundException.class,
          () -> store().transplant(newBranch, Optional.of(unrelatedCommit), Arrays.asList(firstCommit, secondCommit, thirdCommit)));
  }

  @Test
  void toRef() throws VersionStoreException {
    final BranchName branch = BranchName.of("main");
    store().create(branch, Optional.empty());
    final Hash initialCommit = store().toHash(branch);

    final Hash firstCommit = addCommit(branch, "First Commit");
    final Hash secondCommit = addCommit(branch, "Second Commit");
    final Hash thirdCommit = addCommit(branch, "Third Commit");

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

  @SuppressWarnings("unchecked")
  protected Hash addCommit(BranchName branch, String message, Operation<String>... operations)
      throws ReferenceConflictException, ReferenceNotFoundException {
    final Hash commit = store().toHash(branch);
    return addCommit(branch, commit, message, operations);
  }

  @SuppressWarnings("unchecked")
  protected Hash addCommit(BranchName branch, Hash referenceCommit, String message, Operation<String>... operations)
      throws ReferenceConflictException, ReferenceNotFoundException {
    store().commit(branch, Optional.of(referenceCommit), message, Arrays.asList(operations));
    return store().toHash(branch);
  }

  @SuppressWarnings("unchecked")
  protected Hash forceCommit(BranchName branch, String message, Operation<String>... operations)
      throws ReferenceConflictException, ReferenceNotFoundException {
    store().commit(branch, Optional.empty(), message, Arrays.asList(operations));
    return store().toHash(branch);
  }
}
