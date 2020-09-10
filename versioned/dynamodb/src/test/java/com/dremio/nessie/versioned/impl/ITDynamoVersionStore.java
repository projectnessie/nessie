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
package com.dremio.nessie.versioned.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import com.dremio.nessie.backend.dynamodb.LocalDynamoDB;
import com.dremio.nessie.versioned.BranchName;
import com.dremio.nessie.versioned.Delete;
import com.dremio.nessie.versioned.Hash;
import com.dremio.nessie.versioned.ImmutableBranchName;
import com.dremio.nessie.versioned.ImmutableKey;
import com.dremio.nessie.versioned.ImmutablePut;
import com.dremio.nessie.versioned.ImmutableTagName;
import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.Put;
import com.dremio.nessie.versioned.Ref;
import com.dremio.nessie.versioned.ReferenceAlreadyExistsException;
import com.dremio.nessie.versioned.ReferenceConflictException;
import com.dremio.nessie.versioned.ReferenceNotFoundException;
import com.dremio.nessie.versioned.TagName;
import com.dremio.nessie.versioned.Unchanged;
import com.dremio.nessie.versioned.WithHash;
import com.dremio.nessie.versioned.impl.DynamoStore.ValueType;
import com.dremio.nessie.versioned.impl.InconsistentValue.InconsistentValueException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

@ExtendWith(LocalDynamoDB.class)
class ITDynamoVersionStore {
  private DynamoStoreFixture fixture;

  @BeforeEach
  void setup() {
    fixture = new DynamoStoreFixture();
  }

  @AfterEach
  void deleteResources() {
    fixture.close();
  }

  @Test
  void checkDuplicateValueCommit() throws Exception {
    BranchName branch = BranchName.of("dupe-values");
    fixture.create(branch, Optional.empty());
    fixture.commit(branch, Optional.empty(), "metadata", ImmutableList.of(
        Put.of(Key.of("hi"), "world"),
        Put.of(Key.of("no"), "world"))
    );

    assertEquals("world", fixture.getValue(branch, Key.of("hi")));
    assertEquals("world", fixture.getValue(branch, Key.of("no")));
  }


  @Test
  void checkKeyList() throws Exception {
    BranchName branch = BranchName.of("my-key-list");
    fixture.create(branch, Optional.empty());
    assertEquals(0, fixture.getStore().<L2>loadSingle(ValueType.L2, L2.EMPTY_ID).size());
    fixture.commit(branch, Optional.empty(), "metadata", ImmutableList.of(
        Put.of(Key.of("hi"), "world"),
        Put.of(Key.of("no"), "world"),
        Put.of(Key.of("mad mad"), "world")));
    assertEquals(0, fixture.getStore().<L2>loadSingle(ValueType.L2, L2.EMPTY_ID).size());
    assertThat(fixture.getKeys(branch).map(Key::toString).collect(ImmutableSet.toImmutableSet()),
        containsInAnyOrder("hi", "no", "mad mad"));
  }

  @Test
  void multiload() throws Exception {
    BranchName branch = BranchName.of("my-key-list");
    fixture.create(branch, Optional.empty());
    fixture.commit(branch, Optional.empty(), "metadata", ImmutableList.of(
        Put.of(Key.of("hi"), "world1"),
        Put.of(Key.of("no"), "world2"),
        Put.of(Key.of("mad mad"), "world3")));

    assertEquals(
        Arrays.asList("world1", "world2", "world3"),
        fixture.getValues(branch, Arrays.asList(Key.of("hi"), Key.of("no"), Key.of("mad mad")))
          .stream()
          .map(Optional::get)
          .collect(Collectors.toList()));
  }

  @Test
  void ensureValidEmptyBranchState() throws ReferenceNotFoundException, ReferenceAlreadyExistsException {
    BranchName branch = BranchName.of("empty_branch");
    fixture.create(branch, Optional.empty());
    Hash hash = fixture.toHash(branch);
    assertEquals(null, fixture.getValue(hash, Key.of("arbitrary")));
  }

  @Test
  void createAndDeleteTag() throws Exception {
    TagName tag = TagName.of("foo");

    // check that we can't assign an empty tag.
    assertThrows(ReferenceNotFoundException.class, () -> fixture.create(tag,  Optional.empty()));

    // create a tag using the default empty hash.
    fixture.create(tag, Optional.of(L1.EMPTY_ID.toHash()));
    assertEquals(L1.EMPTY_ID.toHash(), fixture.toHash(tag));

    // avoid dupe
    assertThrows(ReferenceAlreadyExistsException.class, () -> fixture.create(tag, Optional.of(L1.EMPTY_ID.toHash())));

    // delete without condition
    fixture.delete(tag, Optional.empty());

    // create a tag using the default empty hash.
    fixture.create(tag, Optional.of(L1.EMPTY_ID.toHash()));

    // check that wrong id is rejected
    assertThrows(ReferenceConflictException.class, () -> fixture.delete(tag, Optional.of(Id.EMPTY.toHash())));

    // delete with correct id.
    fixture.delete(tag, Optional.of(L1.EMPTY_ID.toHash()));


    // avoid create to invalid l1.
    assertThrows(ReferenceNotFoundException.class, () -> fixture.create(tag, Optional.of(Id.generateRandom().toHash())));

    // fail on delete of non-existent.
    assertThrows(ReferenceNotFoundException.class, () -> fixture.delete(tag, Optional.empty()));
  }

  @Test
  void createAndDeleteBranch() throws Exception {
    BranchName branch = BranchName.of("foo");

    // create a tag using the default empty hash.
    fixture.create(branch, Optional.of(L1.EMPTY_ID.toHash()));
    assertEquals(L1.EMPTY_ID.toHash(), fixture.toHash(branch));

    // delete without condition
    fixture.delete(branch, Optional.empty());

    // create a tag using no commit.
    fixture.create(branch, Optional.empty());

    // avoid dupe
    assertThrows(ReferenceAlreadyExistsException.class, () -> fixture.create(branch, Optional.empty()));
    assertThrows(ReferenceAlreadyExistsException.class, () -> fixture.create(branch, Optional.of(L1.EMPTY_ID.toHash())));

    // check that wrong id is rejected for deletion (non-existing)
    assertThrows(ReferenceConflictException.class, () -> fixture.delete(branch, Optional.of(Id.EMPTY.toHash())));

    // delete with correct id.
    fixture.delete(branch, Optional.of(L1.EMPTY_ID.toHash()));

    // avoid create to invalid l1
    assertThrows(ReferenceNotFoundException.class, () -> fixture.create(branch, Optional.of(Id.generateRandom().toHash())));

    // fail on delete of non-existent.
    assertThrows(ReferenceNotFoundException.class, () -> fixture.delete(branch, Optional.empty()));

    fixture.create(branch, Optional.empty());
    fixture.commit(branch, Optional.empty(), "metadata", ImmutableList.of(Put.of(Key.of("hi"), "world")));
    // check that wrong id is rejected for deletion (valid but not matching)
    assertThrows(ReferenceConflictException.class, () -> fixture.delete(branch, Optional.of(L1.EMPTY_ID.toHash())));

    // can't use tag delete on branch.
    assertThrows(ReferenceConflictException.class, () -> fixture.delete(TagName.of("foo"), Optional.empty()));
  }

  @Test
  void conflictingCommit() throws Exception {
    BranchName branch = BranchName.of("foo");
    fixture.create(branch, Optional.empty());
    // first commit.
    fixture.commit(branch, Optional.empty(), "metadata", ImmutableList.of(Put.of(Key.of("hi"), "hello world")));

    //first hash.
    Hash originalHash = fixture.getCommits(branch).findFirst().get().getHash();

    //second commit.
    fixture.commit(branch, Optional.empty(), "metadata", ImmutableList.of(Put.of(Key.of("hi"), "goodbye world")));

    // do an extra commit to make sure it has a different hash even though it has the same value.
    fixture.commit(branch, Optional.empty(), "metadata", ImmutableList.of(Put.of(Key.of("hi"), "goodbye world")));

    //attempt commit using first hash which has conflicting key change.
    assertThrows(InconsistentValueException.class, () -> fixture.commit(branch, Optional.of(originalHash),
        "metadata", ImmutableList.of(Put.of(Key.of("hi"), "my world"))));
  }

  @Test
  void checkRefs() throws Exception {
    fixture.create(BranchName.of("b1"), Optional.empty());
    fixture.create(BranchName.of("b2"), Optional.empty());
    fixture.create(TagName.of("t1"), Optional.of(L1.EMPTY_ID.toHash()));
    fixture.create(TagName.of("t2"), Optional.of(L1.EMPTY_ID.toHash()));
    assertEquals(ImmutableSet.of("b1", "b2", "t1", "t2"), fixture.getNamedRefs()
        .map(wh -> wh.getValue().getName()).collect(Collectors.toSet()));
  }

  @Test
  void checkCommits() throws Exception {
    BranchName branch = BranchName.of("foo");
    fixture.create(branch, Optional.empty());
    String c1 = "c1";
    String c2 = "c2";
    Key k1 = Key.of("hi");
    String v1 = "hello world";
    String v1p = "goodbye world";
    Key k2 = Key.of("my", "friend");
    String v2 = "not here";
    fixture.commit(branch, Optional.empty(), c1, ImmutableList.of(Put.of(k1, v1), Put.of(k2, v2)));
    fixture.commit(branch, Optional.empty(), c2, ImmutableList.of(Put.of(k1, v1p)));
    List<WithHash<String>> commits = fixture.getCommits(branch).collect(Collectors.toList());
    assertEquals(ImmutableList.of(c2, c1), commits.stream().map(wh -> wh.getValue()).collect(Collectors.toList()));

    // changed across commits
    assertEquals(v1, fixture.getValue(commits.get(1).getHash(), k1));
    assertEquals(v1p, fixture.getValue(commits.get(0).getHash(), k1));

    // not changed across commits
    assertEquals(v2, fixture.getValue(commits.get(0).getHash(), k2));
    assertEquals(v2, fixture.getValue(commits.get(1).getHash(), k2));

    assertEquals(2, fixture.getCommits(commits.get(0).getHash()).count());

    TagName tag = TagName.of("tag1");
    fixture.create(tag, Optional.of(commits.get(0).getHash()));
    assertEquals(2, fixture.getCommits(tag).count());
  }

  @Test
  void assignments() throws Exception {
    BranchName branch = BranchName.of("foo");
    final Key k1 = Key.of("p1");
    fixture.create(branch, Optional.empty());
    fixture.commit(branch, Optional.empty(), "c1", ImmutableList.of(Put.of(k1, "v1")));
    Hash c1 = fixture.toHash(branch);
    fixture.commit(branch, Optional.empty(), "c1", ImmutableList.of(Put.of(k1, "v2")));
    Hash c2 = fixture.toHash(branch);
    TagName t1 = TagName.of("t1");
    BranchName b2 = BranchName.of("b2");

    // ensure tag create assignment is correct.
    fixture.create(t1, Optional.of(c1));
    assertEquals("v1", fixture.getValue(t1, k1));

    // ensure branch create non-assignment works
    fixture.create(b2, Optional.empty());
    assertEquals(null, fixture.getValue(b2, k1));

    // ensure tag reassignment is correct.
    fixture.assign(t1, Optional.of(c1), c2);
    assertEquals("v2", fixture.getValue(t1, k1));

    // ensure branch assignment (no current) is correct
    fixture.assign(b2, Optional.empty(), c1);
    assertEquals("v1", fixture.getValue(b2, k1));

    // ensure branch assignment (with current) is current
    fixture.assign(b2, Optional.of(c1), c2);
    assertEquals("v2", fixture.getValue(b2, k1));

  }

  @Test
  void delete() throws Exception {
    BranchName branch = BranchName.of("foo");
    final Key k1 = Key.of("p1");
    fixture.create(branch, Optional.empty());
    fixture.commit(branch, Optional.empty(), "c1", ImmutableList.of(Put.of(k1, "v1")));
    assertEquals("v1", fixture.getValue(branch, k1));

    fixture.commit(branch, Optional.empty(), "c1", ImmutableList.of(Delete.of(k1)));
    assertEquals(null, fixture.getValue(branch, k1));
  }

  @Test
  void unchangedOperation() throws Exception {
    BranchName branch = BranchName.of("foo");
    fixture.create(branch, Optional.empty());
    // first commit.
    fixture.commit(branch, Optional.empty(), "metadata", ImmutableList.of(Put.of(Key.of("hi"), "hello world")));

    //first hash.
    Hash originalHash = fixture.getCommits(branch).findFirst().get().getHash();

    //second commit.
    fixture.commit(branch, Optional.empty(), "metadata", ImmutableList.of(Put.of(Key.of("hi"), "goodbye world")));

    fixture.commit(branch, Optional.empty(), "metadata", ImmutableList.of(Put.of(Key.of("hi"), "goodbye world")));

    //attempt commit using first hash which has conflicting key change.
    assertThrows(InconsistentValueException.class, () -> fixture.commit(branch, Optional.of(originalHash),
        "metadata", ImmutableList.of(Put.of(Key.of("hi"), "my world"))));

    // attempt commit using first hash, put on on-conflicting key, unchanged on conflicting key.
    assertThrows(ReferenceConflictException.class,
        () -> fixture.commit(branch, Optional.of(originalHash), "metadata",
            ImmutableList.of(Put.of(Key.of("bar"), "mellow"), Unchanged.of(Key.of("hi")))));
  }

  @Test
  void checkEmptyHistory() throws Exception {
    BranchName branch = BranchName.of("foo");
    fixture.create(branch, Optional.empty());
    assertEquals(0L, fixture.getCommits(branch).count());
  }

  @Disabled
  @Test
  void completeFlow() throws Exception {
    final BranchName branch = ImmutableBranchName.builder().name("main").build();
    final BranchName branch2 = ImmutableBranchName.builder().name("b2").build();
    final TagName tag = ImmutableTagName.builder().name("t1").build();
    final Key p1 = ImmutableKey.builder().addElements("my.path").build();
    final String commit1 = "my commit 1";
    final String commit2 = "my commit 2";
    final String v1 = "my.value";
    final String v2 = "my.value2";

    // create a branch
    fixture.create(branch, Optional.empty());

    try {
      fixture.create(branch, Optional.empty());
      assertFalse(true, "Creating the a branch with the same name as an existing one should fail but didn't.");
    } catch (ReferenceAlreadyExistsException ex) {
      // expected.
    }

    fixture.commit(branch, Optional.empty(), commit1, ImmutableList.of(
        ImmutablePut.<String>builder().key(
            p1
            )
        .shouldMatchHash(false)
        .value(v1)
        .build()
        )
    );

    assertEquals(v1, fixture.getValue(branch, p1));

    fixture.create(tag, Optional.of(fixture.toHash(branch)));

    fixture.commit(branch, Optional.empty(), commit2, ImmutableList.of(
        ImmutablePut.<String>builder().key(
            p1
            )
        .shouldMatchHash(false)
        .value(v2)
        .build()
        )
    );

    assertEquals(v2, fixture.getValue(branch, p1));
    assertEquals(v1, fixture.getValue(tag, p1));

    List<WithHash<String>> commits = fixture.getCommits(branch).collect(Collectors.toList());

    assertEquals(v1, fixture.getValue(commits.get(1).getHash(), p1));
    assertEquals(commit1, commits.get(1).getValue());
    assertEquals(v2, fixture.getValue(commits.get(0).getHash(), p1));
    assertEquals(commit2, commits.get(0).getValue());

    fixture.assign(tag, Optional.of(commits.get(1).getHash()), commits.get(0).getHash());

    assertEquals(commits, fixture.getCommits(tag).collect(Collectors.toList()));
    assertEquals(commits, fixture.getCommits(commits.get(0).getHash()).collect(Collectors.toList()));

    assertEquals(2, fixture.getNamedRefs().count());

    fixture.create(branch2, Optional.of(commits.get(1).getHash()));

    fixture.delete(branch, Optional.of(commits.get(0).getHash()));

    assertEquals(2, fixture.getNamedRefs().count());

    assertEquals(v1, fixture.getValue(branch2, p1));


  }

  @Test
  void unknownRef() throws Exception {
    BranchName branch = BranchName.of("bar");
    fixture.create(branch, Optional.empty());
    fixture.commit(branch, Optional.empty(), "metadata", ImmutableList.of(Put.of(Key.of("hi"), "hello world")));
    TagName tag = TagName.of("foo");
    Hash expected = fixture.toHash(branch);
    fixture.create(tag, Optional.of(expected));

    testRefMatchesToRef(branch, expected, branch.getName());
    testRefMatchesToRef(tag, expected, tag.getName());
    testRefMatchesToRef(expected, expected, expected.asString());
  }

  private void testRefMatchesToRef(Ref ref, Hash hash, String name) throws ReferenceNotFoundException {
    WithHash<Ref> val = fixture.toRef(name);
    assertEquals(ref, val.getValue());
    assertEquals(hash, val.getHash());
  }
}
