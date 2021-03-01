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
package org.projectnessie.versioned.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Delete;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ImmutableBranchName;
import org.projectnessie.versioned.ImmutableKey;
import org.projectnessie.versioned.ImmutablePut;
import org.projectnessie.versioned.ImmutableTagName;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.Put;
import org.projectnessie.versioned.Ref;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.StringSerializer;
import org.projectnessie.versioned.TagName;
import org.projectnessie.versioned.Unchanged;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.WithHash;
import org.projectnessie.versioned.WithPayload;
import org.projectnessie.versioned.impl.InconsistentValue.InconsistentValueException;
import org.projectnessie.versioned.store.Id;
import org.projectnessie.versioned.store.Store;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

public abstract class AbstractITTieredVersionStore {

  private AbstractTieredStoreFixture<?, ?> fixture;

  @BeforeEach
  void setup() {
    fixture = createNewFixture();
  }

  @AfterEach
  void deleteResources() throws Exception {
    fixture.close();
  }

  public VersionStore<String, String> versionStore() {
    return fixture;
  }

  public Store store() {
    return fixture.getStore();
  }

  protected abstract AbstractTieredStoreFixture<?, ?> createNewFixture();

  @Test
  void checkValueEntityType() throws Exception {

    BranchName branch = BranchName.of("entity-types");
    versionStore().create(branch, Optional.empty());
    StringSerializer.setPayload((byte) 24);
    versionStore().commit(branch, Optional.empty(), "metadata", ImmutableList.of(
        Put.of(Key.of("hi"), "world"))
    );
    StringSerializer.unsetPayload();

    assertEquals("world", versionStore().getValue(branch, Key.of("hi")));
    List<Optional<String>> values = versionStore().getValues(branch, Lists.newArrayList(Key.of("hi")));
    assertEquals(1, values.size());
    assertTrue(values.get(0).isPresent());

    List<WithPayload<Key>> keys = versionStore().getKeys(branch).collect(Collectors.toList());
    assertEquals(1, keys.size());
    assertEquals(Key.of("hi"), keys.get(0).getValue());
    assertEquals((byte)24, keys.get(0).getPayload());

  }

  @Test
  void checkValueEntityTypeWithRemoval() throws Exception {

    BranchName branch = BranchName.of("entity-types-with-removal");
    versionStore().create(branch, Optional.empty());
    StringSerializer.setPayload((byte) 24);
    versionStore().commit(branch, Optional.empty(), "metadata", ImmutableList.of(
        Put.of(Key.of("hi"), "world"))
    );
    StringSerializer.unsetPayload();
    versionStore().commit(branch, Optional.empty(), "metadata", ImmutableList.of(Delete.of(Key.of("hi"))));

    List<WithPayload<Key>> keys = versionStore().getKeys(branch).collect(Collectors.toList());
    assertTrue(keys.isEmpty());

  }

  @Test
  void checkValueEntityTypeAfterKeyRoll() throws Exception {
    BranchName branch = BranchName.of("entity-types-key-roll");
    versionStore().create(branch, Optional.empty());
    for (int i = 0; i < 128; i++) {
      StringSerializer.setPayload((byte) i);
      versionStore().commit(branch, Optional.empty(), "metadata", ImmutableList.of(
          Put.of(Key.of("hi" + i), "world" + i))
      );
      StringSerializer.unsetPayload();
    }

    List<WithPayload<Key>> keys = versionStore().getKeys(branch).collect(Collectors.toList());
    assertEquals(128, keys.size());
    assertThat(keys.stream().map(x -> x.getValue().getElements().get(0)).collect(Collectors.toList()),
        containsInAnyOrder(IntStream.range(0, 128).mapToObj(i -> "hi" + i).toArray(String[]::new)));
    assertThat(keys.stream().map(WithPayload::getPayload).collect(Collectors.toList()),
        containsInAnyOrder(IntStream.range(0, 128).mapToObj(i -> (byte)i).toArray(Byte[]::new)));
  }

  @Test
  void checkValueEntityTypeAfterKeyRollWithModification() throws Exception {
    BranchName branch = BranchName.of("entity-types-key-roll-modification");
    versionStore().create(branch, Optional.empty());
    for (int i = 0; i < 128; i++) {
      StringSerializer.setPayload((byte) i);
      versionStore().commit(branch, Optional.empty(), "metadata", ImmutableList.of(
          Put.of(Key.of("hi" + i), "world" + i))
      );
      StringSerializer.unsetPayload();
    }
    StringSerializer.setPayload((byte) 22);
    versionStore().commit(branch, Optional.empty(), "metadata", ImmutableList.of(
        Put.of(Key.of("hi" + 12), "world-weary" + 12))
    );
    StringSerializer.unsetPayload();
    versionStore().commit(branch, Optional.empty(), "metadata", ImmutableList.of(Delete.of(Key.of("hi" + 22))));

    List<WithPayload<Key>> keys = versionStore().getKeys(branch).collect(Collectors.toList());
    assertEquals(127, keys.size());
    assertThat(keys.stream().map(x -> x.getValue().getElements().get(0)).collect(Collectors.toList()),
        containsInAnyOrder(IntStream.range(0, 128).filter(i -> i != 22).mapToObj(i -> "hi" + i).toArray(String[]::new)));
    assertThat(keys.stream().map(WithPayload::getPayload).collect(Collectors.toList()),
        containsInAnyOrder(IntStream.range(0, 128)
          .filter(i -> i != 22).map(i -> i == 12 ? 22 : i).mapToObj(i -> (byte)i).toArray(Byte[]::new)));
  }

  @Test
  void checkValueEntityTypeAfterKeyRollWithModification2() throws Exception {
    BranchName branch = BranchName.of("entity-types-key-roll-modification2");
    versionStore().create(branch, Optional.empty());
    Map<String, Byte> payloads = new HashMap<>();
    for (int i = 0; i < 128; i++) {
      StringSerializer.setPayload((byte) i);
      versionStore().commit(branch, Optional.empty(), "metadata", ImmutableList.of(
          Put.of(Key.of("hi" + i), "world" + i))
      );
      payloads.put("hi" + i, (byte) i);
      StringSerializer.unsetPayload();
      if (i % 26 == 0 && i > 25) {
        StringSerializer.setPayload((byte) (i + 10));
        versionStore().commit(branch, Optional.empty(), "metadata", ImmutableList.of(
            Put.of(Key.of("hi" + i), "world-weary" + i))
        );
        payloads.put("hi" + i, (byte) (i + 10));
        StringSerializer.unsetPayload();
      }
      if (i % 26 == 0 && i > 25) {
        versionStore().commit(branch, Optional.empty(), "metadata", ImmutableList.of(Delete.of(Key.of("hi" + i))));
        payloads.remove("hi" + i);
      }
    }

    List<WithPayload<Key>> keys = versionStore().getKeys(branch).collect(Collectors.toList());
    assertEquals(payloads.size(), keys.size());
    assertThat(keys.stream().map(x -> x.getValue().getElements().get(0)).collect(Collectors.toList()),
        containsInAnyOrder(payloads.keySet().toArray(new String[0])));
    assertThat(keys.stream().map(WithPayload::getPayload).collect(Collectors.toList()),
        containsInAnyOrder(payloads.values().toArray(new Byte[0])));
  }

  @Test
  void checkDuplicateValueCommit() throws Exception {
    BranchName branch = BranchName.of("dupe-values");
    versionStore().create(branch, Optional.empty());
    versionStore().commit(branch, Optional.empty(), "metadata", ImmutableList.of(
        Put.of(Key.of("hi"), "world"),
        Put.of(Key.of("no"), "world"))
    );

    assertEquals("world", versionStore().getValue(branch, Key.of("hi")));
    assertEquals("world", versionStore().getValue(branch, Key.of("no")));
  }

  @Test
  void mergeToEmpty() throws Exception {
    BranchName branch1 = BranchName.of("b1");
    BranchName branch2 = BranchName.of("b2");
    versionStore().create(branch1, Optional.empty());
    versionStore().create(branch2, Optional.empty());
    versionStore().commit(branch2, Optional.empty(), "metadata", ImmutableList.of(
        Put.of(Key.of("hi"), "world"),
        Put.of(Key.of("no"), "world")));
    versionStore().merge(versionStore().toHash(branch2), branch1, Optional.of(versionStore().toHash(branch1)));
  }

  @Test
  void mergeNoConflict() throws Exception {
    BranchName branch1 = BranchName.of("b1");
    BranchName branch2 = BranchName.of("b2");
    versionStore().create(branch1, Optional.empty());
    versionStore().commit(branch1, Optional.empty(), "metadata", ImmutableList.of(
        Put.of(Key.of("foo"), "world1"),
        Put.of(Key.of("bar"), "world2")));

    versionStore().create(branch2, Optional.empty());
    versionStore().commit(branch2, Optional.empty(), "metadata", ImmutableList.of(
        Put.of(Key.of("hi"), "world3"),
        Put.of(Key.of("no"), "world4")));
    versionStore().merge(versionStore().toHash(branch2), branch1, Optional.of(versionStore().toHash(branch1)));

    assertEquals("world1", versionStore().getValue(branch1, Key.of("foo")));
    assertEquals("world2", versionStore().getValue(branch1, Key.of("bar")));
    assertEquals("world3", versionStore().getValue(branch1, Key.of("hi")));
    assertEquals("world4", versionStore().getValue(branch1, Key.of("no")));

  }

  @Test
  void mergeConflict() throws Exception {
    BranchName branch1 = BranchName.of("b1");
    BranchName branch2 = BranchName.of("b2");
    versionStore().create(branch1, Optional.empty());
    versionStore().commit(branch1, Optional.empty(), "metadata", ImmutableList.of(Put.of(Key.of("conflictKey"), "world1")));

    versionStore().create(branch2, Optional.empty());
    versionStore().commit(branch2, Optional.empty(), "metadata2", ImmutableList.of(Put.of(Key.of("conflictKey"), "world2")));

    ReferenceConflictException ex = assertThrows(ReferenceConflictException.class, () ->
        versionStore().merge(versionStore().toHash(branch2), branch1, Optional.of(versionStore().toHash(branch1))));
    assertThat(ex.getMessage(), Matchers.containsString("conflictKey"));
  }

  @Test
  void checkKeyList() throws Exception {
    BranchName branch = BranchName.of("my-key-list");
    versionStore().create(branch, Optional.empty());
    assertEquals(0, EntityType.L2.loadSingle(store(), InternalL2.EMPTY_ID).size());
    versionStore().commit(branch, Optional.empty(), "metadata", ImmutableList.of(
        Put.of(Key.of("hi"), "world"),
        Put.of(Key.of("no"), "world"),
        Put.of(Key.of("mad mad"), "world")));
    assertEquals(0, EntityType.L2.loadSingle(store(), InternalL2.EMPTY_ID).size());
    assertThat(versionStore().getKeys(branch).map(WithPayload::getValue).map(Key::toString).collect(ImmutableSet.toImmutableSet()),
        Matchers.containsInAnyOrder("hi", "no", "mad mad"));
  }

  @Test
  void ensureKeyCheckpointsAndMultiFragmentsWork() throws Exception {
    BranchName branch = BranchName.of("lots-of-keys");
    versionStore().create(branch, Optional.empty());
    Hash current = versionStore().toHash(branch);
    Random r = new Random(1234);
    char[] longName = new char[4096];
    Arrays.fill(longName, 'a');
    String prefix = new String(longName);
    List<Key> names = new LinkedList<>();
    for (int i = 1; i < 200; i++) {
      if (i % 5 == 0) {
        // every so often, remove a key.
        Key removal = names.remove(r.nextInt(names.size()));
        versionStore().commit(branch, Optional.of(current), "commit " + i, Collections.singletonList(Delete.of(removal)));
      } else {
        Key name = Key.of(prefix + i);
        names.add(name);
        versionStore().commit(branch, Optional.of(current), "commit " + i, Collections.singletonList(Put.of(name, "bar")));
      }
      current = versionStore().toHash(branch);
    }

    List<Key> keysFromStore = versionStore().getKeys(branch).map(WithPayload::getValue).collect(Collectors.toList());

    // ensure that our total key size is greater than a single dynamo page.
    assertThat(keysFromStore.size() * longName.length, Matchers.greaterThan(400000));

    // ensure that keys stored match those expected.
    assertThat(keysFromStore, Matchers.containsInAnyOrder(names.toArray(new Key[0])));
  }

  @Test
  void multiload() throws Exception {
    BranchName branch = BranchName.of("my-key-list");
    versionStore().create(branch, Optional.empty());
    versionStore().commit(branch, Optional.empty(), "metadata", ImmutableList.of(
        Put.of(Key.of("hi"), "world1"),
        Put.of(Key.of("no"), "world2"),
        Put.of(Key.of("mad mad"), "world3")));

    assertEquals(
        Arrays.asList("world1", "world2", "world3"),
        versionStore().getValues(branch, Arrays.asList(Key.of("hi"), Key.of("no"), Key.of("mad mad")))
          .stream()
          .map(Optional::get)
          .collect(Collectors.toList()));
  }

  @Test
  void ensureValidEmptyBranchState() throws ReferenceNotFoundException, ReferenceAlreadyExistsException {
    BranchName branch = BranchName.of("empty_branch");
    versionStore().create(branch, Optional.empty());
    Hash hash = versionStore().toHash(branch);
    assertEquals(null, versionStore().getValue(hash, Key.of("arbitrary")));
  }

  @Test
  void createAndDeleteTag() throws Exception {
    TagName tag = TagName.of("foo");

    // check that we can't assign an empty tag.
    assertThrows(IllegalArgumentException.class, () -> versionStore().create(tag,  Optional.empty()));

    // create a tag using the default empty hash.
    versionStore().create(tag, Optional.of(InternalL1.EMPTY_ID.toHash()));
    assertEquals(InternalL1.EMPTY_ID.toHash(), versionStore().toHash(tag));

    // avoid dupe
    assertThrows(ReferenceAlreadyExistsException.class, () -> versionStore().create(tag, Optional.of(InternalL1.EMPTY_ID.toHash())));

    // delete without condition
    versionStore().delete(tag, Optional.empty());

    // create a tag using the default empty hash.
    versionStore().create(tag, Optional.of(InternalL1.EMPTY_ID.toHash()));

    // check that wrong id is rejected
    assertThrows(ReferenceConflictException.class, () -> versionStore().delete(tag, Optional.of(Id.EMPTY.toHash())));

    // delete with correct id.
    versionStore().delete(tag, Optional.of(InternalL1.EMPTY_ID.toHash()));


    // avoid create to invalid l1.
    assertThrows(ReferenceNotFoundException.class, () -> versionStore().create(tag, Optional.of(Id.generateRandom().toHash())));

    // fail on delete of non-existent.
    assertThrows(ReferenceNotFoundException.class, () -> versionStore().delete(tag, Optional.empty()));
  }

  @Test
  void createAndDeleteBranch() throws Exception {
    BranchName branch = BranchName.of("foo");

    // create a tag using the default empty hash.
    versionStore().create(branch, Optional.of(InternalL1.EMPTY_ID.toHash()));
    assertEquals(InternalL1.EMPTY_ID.toHash(), versionStore().toHash(branch));

    // delete without condition
    versionStore().delete(branch, Optional.empty());

    // create a tag using no commit.
    versionStore().create(branch, Optional.empty());

    // avoid dupe
    assertThrows(ReferenceAlreadyExistsException.class, () -> versionStore().create(branch, Optional.empty()));
    assertThrows(ReferenceAlreadyExistsException.class, () -> versionStore().create(branch, Optional.of(InternalL1.EMPTY_ID.toHash())));

    // check that wrong id is rejected for deletion (non-existing)
    assertThrows(ReferenceConflictException.class, () -> versionStore().delete(branch, Optional.of(Id.EMPTY.toHash())));

    // delete with correct id.
    versionStore().delete(branch, Optional.of(InternalL1.EMPTY_ID.toHash()));

    // avoid create to invalid l1
    assertThrows(ReferenceNotFoundException.class, () -> versionStore().create(branch, Optional.of(Id.generateRandom().toHash())));

    // fail on delete of non-existent.
    assertThrows(ReferenceNotFoundException.class, () -> versionStore().delete(branch, Optional.empty()));

    versionStore().create(branch, Optional.empty());
    versionStore().commit(branch, Optional.empty(), "metadata", ImmutableList.of(Put.of(Key.of("hi"), "world")));
    // check that wrong id is rejected for deletion (valid but not matching)
    assertThrows(ReferenceConflictException.class, () -> versionStore().delete(branch, Optional.of(InternalL1.EMPTY_ID.toHash())));

    // can't use tag delete on branch.
    assertThrows(ReferenceConflictException.class, () -> versionStore().delete(TagName.of("foo"), Optional.empty()));
  }

  @Test
  void conflictingCommit() throws Exception {
    BranchName branch = BranchName.of("foo");
    versionStore().create(branch, Optional.empty());
    // first commit.
    versionStore().commit(branch, Optional.empty(), "metadata", ImmutableList.of(Put.of(Key.of("hi"), "hello world")));

    //first hash.
    Hash originalHash = versionStore().getCommits(branch).findFirst().get().getHash();

    //second commit.
    versionStore().commit(branch, Optional.empty(), "metadata", ImmutableList.of(Put.of(Key.of("hi"), "goodbye world")));

    // do an extra commit to make sure it has a different hash even though it has the same value.
    versionStore().commit(branch, Optional.empty(), "metadata", ImmutableList.of(Put.of(Key.of("hi"), "goodbye world")));

    //attempt commit using first hash which has conflicting key change.
    assertThrows(InconsistentValueException.class, () -> versionStore().commit(branch, Optional.of(originalHash),
        "metadata", ImmutableList.of(Put.of(Key.of("hi"), "my world"))));
  }

  @Test
  void checkRefs() throws Exception {
    versionStore().create(BranchName.of("b1"), Optional.empty());
    versionStore().create(BranchName.of("b2"), Optional.empty());
    versionStore().create(TagName.of("t1"), Optional.of(InternalL1.EMPTY_ID.toHash()));
    versionStore().create(TagName.of("t2"), Optional.of(InternalL1.EMPTY_ID.toHash()));
    try (Stream<WithHash<NamedRef>> str = versionStore().getNamedRefs()) {
      assertEquals(ImmutableSet.of("b1", "b2", "t1", "t2"), str
          .map(wh -> wh.getValue().getName()).collect(Collectors.toSet()));
    }
  }

  @Test
  void checkCommits() throws Exception {
    BranchName branch = BranchName.of("foo");
    versionStore().create(branch, Optional.empty());
    String c1 = "c1";
    String c2 = "c2";
    Key k1 = Key.of("hi");
    String v1 = "hello world";
    String v1p = "goodbye world";
    Key k2 = Key.of("my", "friend");
    String v2 = "not here";
    versionStore().commit(branch, Optional.empty(), c1, ImmutableList.of(Put.of(k1, v1), Put.of(k2, v2)));
    versionStore().commit(branch, Optional.empty(), c2, ImmutableList.of(Put.of(k1, v1p)));
    List<WithHash<String>> commits = versionStore().getCommits(branch).collect(Collectors.toList());
    assertEquals(ImmutableList.of(c2, c1), commits.stream().map(wh -> wh.getValue()).collect(Collectors.toList()));

    // changed across commits
    assertEquals(v1, versionStore().getValue(commits.get(1).getHash(), k1));
    assertEquals(v1p, versionStore().getValue(commits.get(0).getHash(), k1));

    // not changed across commits
    assertEquals(v2, versionStore().getValue(commits.get(0).getHash(), k2));
    assertEquals(v2, versionStore().getValue(commits.get(1).getHash(), k2));

    assertEquals(2, versionStore().getCommits(commits.get(0).getHash()).count());

    TagName tag = TagName.of("tag1");
    versionStore().create(tag, Optional.of(commits.get(0).getHash()));
    assertEquals(2, versionStore().getCommits(tag).count());
  }

  @Test
  void assignments() throws Exception {
    BranchName branch = BranchName.of("foo");
    final Key k1 = Key.of("p1");
    versionStore().create(branch, Optional.empty());
    versionStore().commit(branch, Optional.empty(), "c1", ImmutableList.of(Put.of(k1, "v1")));
    Hash c1 = versionStore().toHash(branch);
    versionStore().commit(branch, Optional.empty(), "c1", ImmutableList.of(Put.of(k1, "v2")));
    Hash c2 = versionStore().toHash(branch);
    TagName t1 = TagName.of("t1");
    BranchName b2 = BranchName.of("b2");

    // ensure tag create assignment is correct.
    versionStore().create(t1, Optional.of(c1));
    assertEquals("v1", versionStore().getValue(t1, k1));

    // ensure branch create non-assignment works
    versionStore().create(b2, Optional.empty());
    assertEquals(null, versionStore().getValue(b2, k1));

    // ensure tag reassignment is correct.
    versionStore().assign(t1, Optional.of(c1), c2);
    assertEquals("v2", versionStore().getValue(t1, k1));

    // ensure branch assignment (no current) is correct
    versionStore().assign(b2, Optional.empty(), c1);
    assertEquals("v1", versionStore().getValue(b2, k1));

    // ensure branch assignment (with current) is current
    versionStore().assign(b2, Optional.of(c1), c2);
    assertEquals("v2", versionStore().getValue(b2, k1));

  }

  @Test
  void delete() throws Exception {
    BranchName branch = BranchName.of("foo");
    final Key k1 = Key.of("p1");
    versionStore().create(branch, Optional.empty());
    versionStore().commit(branch, Optional.empty(), "c1", ImmutableList.of(Put.of(k1, "v1")));
    assertEquals("v1", versionStore().getValue(branch, k1));

    versionStore().commit(branch, Optional.empty(), "c1", ImmutableList.of(Delete.of(k1)));
    assertEquals(null, versionStore().getValue(branch, k1));
  }

  @Test
  void unchangedOperation() throws Exception {
    BranchName branch = BranchName.of("foo");
    versionStore().create(branch, Optional.empty());
    // first commit.
    versionStore().commit(branch, Optional.empty(), "metadata", ImmutableList.of(Put.of(Key.of("hi"), "hello world")));

    //first hash.
    Hash originalHash = versionStore().getCommits(branch).findFirst().get().getHash();

    //second commit.
    versionStore().commit(branch, Optional.empty(), "metadata", ImmutableList.of(Put.of(Key.of("hi"), "goodbye world")));

    versionStore().commit(branch, Optional.empty(), "metadata", ImmutableList.of(Put.of(Key.of("hi"), "goodbye world")));

    //attempt commit using first hash which has conflicting key change.
    assertThrows(InconsistentValueException.class, () -> versionStore().commit(branch, Optional.of(originalHash),
        "metadata", ImmutableList.of(Put.of(Key.of("hi"), "my world"))));

    // attempt commit using first hash, put on on-conflicting key, unchanged on conflicting key.
    assertThrows(ReferenceConflictException.class,
        () -> versionStore().commit(branch, Optional.of(originalHash), "metadata",
            ImmutableList.of(Put.of(Key.of("bar"), "mellow"), Unchanged.of(Key.of("hi")))));
  }

  @Test
  void checkEmptyHistory() throws Exception {
    BranchName branch = BranchName.of("foo");
    versionStore().create(branch, Optional.empty());
    assertEquals(0L, versionStore().getCommits(branch).count());
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
    versionStore().create(branch, Optional.empty());

    try {
      versionStore().create(branch, Optional.empty());
      assertFalse(true, "Creating the a branch with the same name as an existing one should fail but didn't.");
    } catch (ReferenceAlreadyExistsException ex) {
      // expected.
    }

    versionStore().commit(branch, Optional.empty(), commit1, ImmutableList.of(
        ImmutablePut.<String>builder().key(
            p1
            )
        .shouldMatchHash(false)
        .value(v1)
        .build()
        )
    );

    assertEquals(v1, versionStore().getValue(branch, p1));

    versionStore().create(tag, Optional.of(versionStore().toHash(branch)));

    versionStore().commit(branch, Optional.empty(), commit2, ImmutableList.of(
        ImmutablePut.<String>builder().key(
            p1
            )
        .shouldMatchHash(false)
        .value(v2)
        .build()
        )
    );

    assertEquals(v2, versionStore().getValue(branch, p1));
    assertEquals(v1, versionStore().getValue(tag, p1));

    List<WithHash<String>> commits = versionStore().getCommits(branch).collect(Collectors.toList());

    assertEquals(v1, versionStore().getValue(commits.get(1).getHash(), p1));
    assertEquals(commit1, commits.get(1).getValue());
    assertEquals(v2, versionStore().getValue(commits.get(0).getHash(), p1));
    assertEquals(commit2, commits.get(0).getValue());

    versionStore().assign(tag, Optional.of(commits.get(1).getHash()), commits.get(0).getHash());

    assertEquals(commits, versionStore().getCommits(tag).collect(Collectors.toList()));
    assertEquals(commits, versionStore().getCommits(commits.get(0).getHash()).collect(Collectors.toList()));

    try (Stream<WithHash<NamedRef>> str = versionStore().getNamedRefs()) {
      assertEquals(2, str.count());
    }

    versionStore().create(branch2, Optional.of(commits.get(1).getHash()));

    versionStore().delete(branch, Optional.of(commits.get(0).getHash()));

    try (Stream<WithHash<NamedRef>> str = versionStore().getNamedRefs()) {
      assertEquals(2, str.count());
    }

    assertEquals(v1, versionStore().getValue(branch2, p1));


  }

  @Test
  void unknownRef() throws Exception {
    BranchName branch = BranchName.of("bar");
    versionStore().create(branch, Optional.empty());
    versionStore().commit(branch, Optional.empty(), "metadata", ImmutableList.of(Put.of(Key.of("hi"), "hello world")));
    TagName tag = TagName.of("foo");
    Hash expected = versionStore().toHash(branch);
    versionStore().create(tag, Optional.of(expected));

    testRefMatchesToRef(branch, expected, branch.getName());
    testRefMatchesToRef(tag, expected, tag.getName());
    testRefMatchesToRef(expected, expected, expected.asString());
  }

  private void testRefMatchesToRef(Ref ref, Hash hash, String name) throws ReferenceNotFoundException {
    WithHash<Ref> val = versionStore().toRef(name);
    assertEquals(ref, val.getValue());
    assertEquals(hash, val.getHash());
  }
}
