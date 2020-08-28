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
import com.dremio.nessie.versioned.WithHash;

/**
 * Base class used for integration tests against version store implementations.
 */
public abstract class AbstractITVersionStore {

  protected abstract VersionStore<String, String> store();

  /*
   * Test:
   * - Try to create a tag with no hash assigned to it
   *   - check it fails with IllegalArgumentException
   */
  @Test
  void createTagWithoutTarget() throws Exception {
    final TagName tag = TagName.of("foo");

    // check that we can't assign an empty tag.
    assertThrows(IllegalArgumentException.class, () -> store().create(tag,  Optional.empty()));
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
  void createAndDeleteBranch() throws Exception {
    final BranchName branch = BranchName.of("foo");

    store().create(branch, Optional.empty());

    final Hash hash = store().toHash(branch);
    assertThat(hash, is(notNullValue()));

    final List<WithHash<NamedRef>> namedRefs = store().getNamedRefs().collect(Collectors.toList());
    assertThat(namedRefs, contains(WithHash.of(hash, branch)));

    assertThat(store().getCommits(branch).count(), is(0L));
    assertThat(store().getCommits(hash).count(), is(0L));


    assertThrows(ReferenceAlreadyExistsException.class, () -> store().create(branch, Optional.empty()));
    assertThrows(ReferenceAlreadyExistsException.class, () -> store().create(branch, Optional.of(hash)));

    store().delete(branch, Optional.of(hash));
    assertThrows(ReferenceNotFoundException.class, () -> store().toHash(branch));
    assertThat(store().getNamedRefs().count(), is(0L));
    assertThrows(ReferenceNotFoundException.class, () -> store().delete(branch, Optional.of(hash)));

  }

  /*
   * Test:
   * - Create a new branch
   * - Add a commit to it
   * - Check that another commit cannot be added with the initial hash
   * - Check the commit can be listed
   * - Check that the commit can be deleted
   */
  @Test
  void addCommitsToBranch() throws Exception {
    final BranchName branch = BranchName.of("foo");

    store().create(branch, Optional.empty());
    final Hash initialHash = store().toHash(branch);

    store().commit(branch, Optional.of(initialHash), "Some commit", Collections.emptyList());
    final Hash commitHash = store().toHash(branch);

    assertThat(commitHash, is(Matchers.not(initialHash)));
    assertThrows(ReferenceConflictException.class,
        () -> store().commit(branch, Optional.of(initialHash), "Another commit", Collections.emptyList()));

    assertThat(store().getCommits(branch).collect(Collectors.toList()), contains(WithHash.of(commitHash, "Some commit")));
    assertThat(store().getCommits(commitHash).collect(Collectors.toList()), contains(WithHash.of(commitHash, "Some commit")));

    assertThrows(ReferenceConflictException.class, () -> store().delete(branch, Optional.of(initialHash)));
    store().delete(branch, Optional.of(commitHash));
    assertThrows(ReferenceNotFoundException.class, () -> store().toHash(branch));
    assertThat(store().getNamedRefs().count(), is(0L));
    assertThrows(ReferenceNotFoundException.class, () -> store().delete(branch, Optional.of(commitHash)));
  }

  /*
   * Test:
   * - Create a new branch
   * - Add 3 commits to it with put and delete operations
   * - Check commit metadata
   * - Check keys for each commit hash
   * - Check values for each commit hash
   *
   */
  @Test
  void commitsWithOperations() throws Exception {
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

  @SuppressWarnings("unchecked")
  protected Hash addCommit(BranchName branch, String message, Operation<String>... operations)
      throws ReferenceConflictException, ReferenceNotFoundException {
    final Hash commit = store().toHash(branch);
    store().commit(branch, Optional.of(commit), message, Arrays.asList(operations));
    return store().toHash(branch);
  }
}
