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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Delete;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ImmutableBranchName;
import org.projectnessie.versioned.ImmutableKey;
import org.projectnessie.versioned.ImmutablePut;
import org.projectnessie.versioned.ImmutableTagName;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.Operation;
import org.projectnessie.versioned.Put;
import org.projectnessie.versioned.Ref;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.Serializer;
import org.projectnessie.versioned.SerializerWithPayload;
import org.projectnessie.versioned.StringSerializer;
import org.projectnessie.versioned.TagName;
import org.projectnessie.versioned.Unchanged;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.WithHash;
import org.projectnessie.versioned.WithType;
import org.projectnessie.versioned.impl.InconsistentValue.InconsistentValueException;
import org.projectnessie.versioned.store.Id;
import org.projectnessie.versioned.store.Store;
import org.projectnessie.versioned.store.ValueType;

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

  public VersionStore<String, String, StringSerializer.TestEnum> versionStore() {
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
    doReturn((byte) 24).when(StringSerializer.getInstance()).getPayload("world");
    versionStore()
        .commit(
            branch, Optional.empty(), "metadata", ImmutableList.of(Put.of(Key.of("hi"), "world")));

    assertEquals("world", versionStore().getValue(branch, Key.of("hi")));
    List<Optional<String>> values =
        versionStore().getValues(branch, Lists.newArrayList(Key.of("hi")));
    assertEquals(1, values.size());
    assertTrue(values.get(0).isPresent());

    List<WithType<Key, StringSerializer.TestEnum>> keys =
        versionStore().getKeys(branch).collect(Collectors.toList());
    assertEquals(1, keys.size());
    assertEquals(Key.of("hi"), keys.get(0).getValue());
    assertEquals(StringSerializer.TestEnum.NO, keys.get(0).getType());
  }

  @Test
  void checkValueEntityTypeWithRemoval() throws Exception {

    BranchName branch = BranchName.of("entity-types-with-removal");
    versionStore().create(branch, Optional.empty());
    doReturn((byte) 24).when(StringSerializer.getInstance()).getPayload("world");
    versionStore()
        .commit(
            branch, Optional.empty(), "metadata", ImmutableList.of(Put.of(Key.of("hi"), "world")));

    versionStore()
        .commit(branch, Optional.empty(), "metadata", ImmutableList.of(Delete.of(Key.of("hi"))));

    List<WithType<Key, StringSerializer.TestEnum>> keys =
        versionStore().getKeys(branch).collect(Collectors.toList());
    assertTrue(keys.isEmpty());
  }

  @Test
  void checkValueEntityTypeWithModification() throws Exception {

    BranchName branch = BranchName.of("entity-types-with-removal");
    versionStore().create(branch, Optional.empty());
    doReturn((byte) 24).when(StringSerializer.getInstance()).getPayload("world");
    versionStore()
        .commit(
            branch, Optional.empty(), "metadata", ImmutableList.of(Put.of(Key.of("hi"), "world")));

    List<WithType<Key, StringSerializer.TestEnum>> keys =
        versionStore().getKeys(branch).collect(Collectors.toList());
    assertEquals(1, keys.size());
    assertEquals(Key.of("hi"), keys.get(0).getValue());
    assertEquals(StringSerializer.TestEnum.NO, keys.get(0).getType());

    doReturn((byte) 80).when(StringSerializer.getInstance()).getPayload("world-weary");
    versionStore()
        .commit(
            branch,
            Optional.empty(),
            "metadata",
            ImmutableList.of(Put.of(Key.of("hi"), "world-weary")));

    keys = versionStore().getKeys(branch).collect(Collectors.toList());
    assertEquals(1, keys.size());
    assertEquals(Key.of("hi"), keys.get(0).getValue());
    assertEquals(StringSerializer.TestEnum.YES, keys.get(0).getType());
  }

  @Test
  void checkDuplicateValueCommit() throws Exception {
    BranchName branch = BranchName.of("dupe-values");
    versionStore().create(branch, Optional.empty());
    versionStore()
        .commit(
            branch,
            Optional.empty(),
            "metadata",
            ImmutableList.of(Put.of(Key.of("hi"), "world"), Put.of(Key.of("no"), "world")));

    assertEquals("world", versionStore().getValue(branch, Key.of("hi")));
    assertEquals("world", versionStore().getValue(branch, Key.of("no")));
  }

  @Test
  void mergeToEmpty() throws Exception {
    BranchName branch1 = BranchName.of("b1");
    BranchName branch2 = BranchName.of("b2");
    versionStore().create(branch1, Optional.empty());
    versionStore().create(branch2, Optional.empty());
    versionStore()
        .commit(
            branch2,
            Optional.empty(),
            "metadata",
            ImmutableList.of(Put.of(Key.of("hi"), "world"), Put.of(Key.of("no"), "world")));
    versionStore()
        .merge(
            versionStore().toHash(branch2), branch1, Optional.of(versionStore().toHash(branch1)));
  }

  @Test
  void mergeNoConflict() throws Exception {
    BranchName branch1 = BranchName.of("b1");
    BranchName branch2 = BranchName.of("b2");
    Hash initial1 = versionStore().create(branch1, Optional.empty());
    Hash commit1 =
        versionStore()
            .commit(
                branch1,
                Optional.empty(),
                "metadata",
                ImmutableList.of(Put.of(Key.of("foo"), "world1"), Put.of(Key.of("bar"), "world2")));
    assertNotEquals(initial1, commit1);

    Hash initial2 = versionStore().create(branch2, Optional.empty());
    Hash commit2 =
        versionStore()
            .commit(
                branch2,
                Optional.empty(),
                "metadata",
                ImmutableList.of(Put.of(Key.of("hi"), "world3"), Put.of(Key.of("no"), "world4")));
    assertNotEquals(initial2, commit2);
    versionStore()
        .merge(
            versionStore().toHash(branch2), branch1, Optional.of(versionStore().toHash(branch1)));

    assertEquals("world1", versionStore().getValue(branch1, Key.of("foo")));
    assertEquals("world2", versionStore().getValue(branch1, Key.of("bar")));
    assertEquals("world3", versionStore().getValue(branch1, Key.of("hi")));
    assertEquals("world4", versionStore().getValue(branch1, Key.of("no")));
  }

  @Test
  void mergeConflict() throws Exception {
    BranchName branch1 = BranchName.of("b1");
    BranchName branch2 = BranchName.of("b2");
    Hash initial1 = versionStore().create(branch1, Optional.empty());
    Hash commit1 =
        versionStore()
            .commit(
                branch1,
                Optional.empty(),
                "metadata",
                ImmutableList.of(Put.of(Key.of("conflictKey"), "world1")));
    assertNotEquals(initial1, commit1);

    Hash initial2 = versionStore().create(branch2, Optional.empty());
    Hash commit2 =
        versionStore()
            .commit(
                branch2,
                Optional.empty(),
                "metadata2",
                ImmutableList.of(Put.of(Key.of("conflictKey"), "world2")));
    assertNotEquals(initial2, commit2);

    ReferenceConflictException ex =
        assertThrows(
            ReferenceConflictException.class,
            () ->
                versionStore()
                    .merge(
                        versionStore().toHash(branch2),
                        branch1,
                        Optional.of(versionStore().toHash(branch1))));
    assertThat(ex.getMessage()).contains("conflictKey");
  }

  @Test
  void checkKeyList() throws Exception {
    BranchName branch = BranchName.of("my-key-list");
    versionStore().create(branch, Optional.empty());
    assertEquals(0, EntityType.L2.loadSingle(store(), InternalL2.EMPTY_ID).size());
    versionStore()
        .commit(
            branch,
            Optional.empty(),
            "metadata",
            ImmutableList.of(
                Put.of(Key.of("hi"), "world"),
                Put.of(Key.of("no"), "world"),
                Put.of(Key.of("mad mad"), "world")));
    assertEquals(0, EntityType.L2.loadSingle(store(), InternalL2.EMPTY_ID).size());
    assertThat(versionStore().getKeys(branch).map(WithType::getValue).map(Key::toString))
        .containsExactlyInAnyOrder("hi", "no", "mad mad");
  }

  @Test
  void ensureKeyCheckpointsAndMultiFragmentsWork() throws Exception {
    BranchName branch = BranchName.of("lots-of-keys");
    Hash initial = versionStore().create(branch, Optional.empty());
    Hash current = versionStore().toHash(branch);
    assertEquals(current, initial);
    Random r = new Random(1234);
    char[] longName = new char[4096];
    Arrays.fill(longName, 'a');
    String prefix = new String(longName);
    List<Key> names = new LinkedList<>();
    Hash last = current;
    for (int i = 1; i < 200; i++) {
      Hash commitHash;
      if (i % 5 == 0) {
        // every so often, remove a key.
        Key removal = names.remove(r.nextInt(names.size()));
        commitHash =
            versionStore()
                .commit(
                    branch,
                    Optional.of(current),
                    "commit " + i,
                    Collections.singletonList(Delete.of(removal)));
      } else {
        Key name = Key.of(prefix + i);
        names.add(name);
        commitHash =
            versionStore()
                .commit(
                    branch,
                    Optional.of(current),
                    "commit " + i,
                    Collections.singletonList(Put.of(name, "bar")));
      }
      current = versionStore().toHash(branch);
      assertEquals(current, commitHash);
      assertNotEquals(last, current);
      last = current;
    }

    List<Key> keysFromStore =
        versionStore().getKeys(branch).map(WithType::getValue).collect(Collectors.toList());

    // ensure that our total key size is greater than a single dynamo page.
    assertThat(keysFromStore.size() * longName.length).isGreaterThan(400000);

    // ensure that keys stored match those expected.
    assertThat(keysFromStore).containsExactlyInAnyOrder(names.toArray(new Key[0]));
  }

  @Test
  void multiload() throws Exception {
    BranchName branch = BranchName.of("my-key-list");
    Hash initialHash = versionStore().create(branch, Optional.empty());
    Hash commitHash =
        versionStore()
            .commit(
                branch,
                Optional.empty(),
                "metadata",
                ImmutableList.of(
                    Put.of(Key.of("hi"), "world1"),
                    Put.of(Key.of("no"), "world2"),
                    Put.of(Key.of("mad mad"), "world3")));
    assertNotEquals(initialHash, commitHash);

    assertEquals(
        Arrays.asList("world1", "world2", "world3"),
        versionStore()
            .getValues(branch, Arrays.asList(Key.of("hi"), Key.of("no"), Key.of("mad mad")))
            .stream()
            .map(Optional::get)
            .collect(Collectors.toList()));
  }

  @Test
  void ensureValidEmptyBranchState()
      throws ReferenceNotFoundException, ReferenceAlreadyExistsException {
    BranchName branch = BranchName.of("empty_branch");
    versionStore().create(branch, Optional.empty());
    Hash hash = versionStore().toHash(branch);
    assertEquals(null, versionStore().getValue(hash, Key.of("arbitrary")));
  }

  @Test
  void createAndDeleteTag() throws Exception {
    TagName tag = TagName.of("foo");

    // check that we can't assign an empty tag.
    assertThrows(
        IllegalArgumentException.class, () -> versionStore().create(tag, Optional.empty()));

    // create a tag using the default empty hash.
    versionStore().create(tag, Optional.of(InternalL1.EMPTY_ID.toHash()));
    assertEquals(InternalL1.EMPTY_ID.toHash(), versionStore().toHash(tag));

    // avoid dupe
    assertThrows(
        ReferenceAlreadyExistsException.class,
        () -> versionStore().create(tag, Optional.of(InternalL1.EMPTY_ID.toHash())));

    // delete without condition
    versionStore().delete(tag, Optional.empty());

    // create a tag using the default empty hash.
    versionStore().create(tag, Optional.of(InternalL1.EMPTY_ID.toHash()));

    // check that wrong id is rejected
    assertThrows(
        ReferenceConflictException.class,
        () -> versionStore().delete(tag, Optional.of(Id.EMPTY.toHash())));

    // delete with correct id.
    versionStore().delete(tag, Optional.of(InternalL1.EMPTY_ID.toHash()));

    // avoid create to invalid l1.
    assertThrows(
        ReferenceNotFoundException.class,
        () -> versionStore().create(tag, Optional.of(Id.generateRandom().toHash())));

    // fail on delete of non-existent.
    assertThrows(
        ReferenceNotFoundException.class, () -> versionStore().delete(tag, Optional.empty()));
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
    assertThrows(
        ReferenceAlreadyExistsException.class,
        () -> versionStore().create(branch, Optional.empty()));
    assertThrows(
        ReferenceAlreadyExistsException.class,
        () -> versionStore().create(branch, Optional.of(InternalL1.EMPTY_ID.toHash())));

    // check that wrong id is rejected for deletion (non-existing)
    assertThrows(
        ReferenceConflictException.class,
        () -> versionStore().delete(branch, Optional.of(Id.EMPTY.toHash())));

    // delete with correct id.
    versionStore().delete(branch, Optional.of(InternalL1.EMPTY_ID.toHash()));

    // avoid create to invalid l1
    assertThrows(
        ReferenceNotFoundException.class,
        () -> versionStore().create(branch, Optional.of(Id.generateRandom().toHash())));

    // fail on delete of non-existent.
    assertThrows(
        ReferenceNotFoundException.class, () -> versionStore().delete(branch, Optional.empty()));

    versionStore().create(branch, Optional.empty());
    versionStore()
        .commit(
            branch, Optional.empty(), "metadata", ImmutableList.of(Put.of(Key.of("hi"), "world")));
    // check that wrong id is rejected for deletion (valid but not matching)
    assertThrows(
        ReferenceConflictException.class,
        () -> versionStore().delete(branch, Optional.of(InternalL1.EMPTY_ID.toHash())));

    // can't use tag delete on branch.
    assertThrows(
        ReferenceConflictException.class,
        () -> versionStore().delete(TagName.of("foo"), Optional.empty()));
  }

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
    Hash createHash = versionStore().create(branch, Optional.empty());
    Arrays.fill(hashesKnownByUser, createHash);

    List<String> expectedValues = new ArrayList<>();
    for (int commitNum = 0; commitNum < numCommits; commitNum++) {
      for (int user = 0; user < numUsers; user++) {
        Hash hashKnownByUser = hashesKnownByUser[user];

        String msg = String.format("user %03d/commit %03d", user, commitNum);
        expectedValues.add(msg);
        String value = String.format("data_file_%03d_%03d", user, commitNum);
        Put<String> put = Put.of(Key.of(tableNameGen.apply(user)), value);

        Hash commitHash;
        try {
          commitHash =
              versionStore()
                  .commit(branch, Optional.of(hashKnownByUser), msg, ImmutableList.of(put));
        } catch (InconsistentValueException inconsistentValueException) {
          if (allowInconsistentValueException) {
            hashKnownByUser = versionStore().toHash(branch);
            commitHash =
                versionStore()
                    .commit(branch, Optional.of(hashKnownByUser), msg, ImmutableList.of(put));
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
        versionStore().getCommits(branch).map(WithHash::getValue).collect(Collectors.toList());
    Collections.reverse(expectedValues);
    assertEquals(expectedValues, committedValues);
  }

  @Test
  void checkpointWithUnsavedL1() throws Exception {
    // KeyList.IncrementalList.generateNewCheckpoint collects parent L1s for a branch
    // via HistoryRetriever to "checkpoint" the keylist. However, this only works if
    // HistoryRetriever has access to both saved AND unsaved L1s, so L1s that are persisted
    // and those that are still in the branch's REF. This test verifies that generateNewCheckpoint
    // does not fail in that case.

    BranchName branch = BranchName.of("checkpointWithUnsavedL1");

    versionStore().create(branch, Optional.empty());

    InternalRefId ref = InternalRefId.of(branch);

    // generate MAX_DELTAS-1 keys in the key-list - just enough to *NOT*
    // trigger KeyList.IncrementalList.generateNewCheckpoint
    for (int i = 1; i < KeyList.IncrementalList.MAX_DELTAS; i++) {
      InternalBranch internalBranch = simulateCommit(ref, i);

      // verify that the branch has an unsaved L1
      assertEquals(i, internalBranch.getCommits().stream().filter(c -> !c.isSaved()).count());
      KeyList keyList = internalBranch.getUpdateState(store()).unsafeGetL1().getKeyList();
      assertFalse(keyList.isFull());
      assertFalse(keyList.isEmptyIncremental());
      KeyList.IncrementalList incrementalList = (KeyList.IncrementalList) keyList;
      assertEquals(i, incrementalList.getDistanceFromCheckpointCommits());
    }

    InternalBranch internalBranch = simulateCommit(ref, KeyList.IncrementalList.MAX_DELTAS);
    KeyList keyList = internalBranch.getUpdateState(store()).unsafeGetL1().getKeyList();
    assertTrue(keyList.isFull());
    assertFalse(keyList.isEmptyIncremental());
  }

  /**
   * This is a copy of {@link TieredVersionStore#commit(BranchName, Optional, Object, List)} that
   * allows the test {@link #checkpointWithUnsavedL1()} to produce commits to a branch with unsaved
   * commits and without collapsing the intention log.
   *
   * <p>It is not particularly great to have a "stripped down" and "heavily adjusted" version of the
   * original {@link TieredVersionStore#commit(BranchName, Optional, Object, List)} in a unit test,
   * but the other option to prepare the pre-requisites for {@link #checkpointWithUnsavedL1()},
   * namely unsaved commits + uncollapsed branch, would have been to refactor the original method
   * and add a bunch of hooks, which felt too heavy.
   *
   * @param ref branch ID
   * @param num number of the commit
   * @return the updated branch
   */
  private InternalBranch simulateCommit(InternalRefId ref, int num) {
    List<Operation<String>> ops =
        Collections.singletonList(Put.of(Key.of("key" + num), "foo" + num));
    List<InternalKey> keys =
        ops.stream().map(op -> new InternalKey(op.getKey())).collect(Collectors.toList());

    SerializerWithPayload<String, StringSerializer.TestEnum> serializer =
        AbstractTieredStoreFixture.WORKER.getValueSerializer();
    Serializer<String> metadataSerializer =
        AbstractTieredStoreFixture.WORKER.getMetadataSerializer();

    PartialTree<String, StringSerializer.TestEnum> current = PartialTree.of(serializer, ref, keys);

    String incomingCommit = "metadata";
    InternalCommitMetadata metadata =
        InternalCommitMetadata.of(metadataSerializer.toBytes(incomingCommit));

    store()
        .load(
            current.getLoadChain(
                b -> {
                  InternalBranch.UpdateState updateState = b.getUpdateState(store());
                  return updateState.unsafeGetL1();
                },
                PartialTree.LoadType.NO_VALUES));

    // do updates.
    ops.forEach(
        op ->
            current.setValueForKey(
                new InternalKey(op.getKey()), Optional.of(((Put<String>) op).getValue())));

    // save all but l1 and branch.
    store()
        .save(
            Stream.concat(
                    current.getMostSaveOps(),
                    Stream.of(EntityType.COMMIT_METADATA.createSaveOpForEntity(metadata)))
                .collect(Collectors.toList()));

    PartialTree.CommitOp commitOp =
        current.getCommitOp(metadata.getId(), Collections.emptyList(), true, true);

    InternalRef.Builder<?> builder = EntityType.REF.newEntityProducer();
    boolean updated =
        store()
            .update(
                ValueType.REF,
                ref.getId(),
                commitOp.getUpdateWithCommit(),
                Optional.of(commitOp.getTreeCondition()),
                Optional.of(builder));
    assertTrue(updated);
    return builder.build().getBranch();
  }

  @Test
  void conflictingCommit() throws Exception {
    BranchName branch = BranchName.of("conflictingCommit");
    Hash createHash = versionStore().create(branch, Optional.empty());

    // first commit.
    Hash initialHash =
        versionStore()
            .commit(
                branch,
                Optional.empty(),
                "metadata",
                ImmutableList.of(Put.of(Key.of("hi"), "hello world")));
    assertNotEquals(createHash, initialHash);

    // first hash.
    Hash originalHash = versionStore().getCommits(branch).findFirst().get().getHash();
    assertEquals(initialHash, originalHash);

    // second commit.
    Hash firstHash =
        versionStore()
            .commit(
                branch,
                Optional.empty(),
                "metadata",
                ImmutableList.of(Put.of(Key.of("hi"), "goodbye world")));
    assertNotEquals(originalHash, firstHash);

    // do an extra commit to make sure it has a different hash even though it has the same value.
    Hash secondHash =
        versionStore()
            .commit(
                branch,
                Optional.empty(),
                "metadata",
                ImmutableList.of(Put.of(Key.of("hi"), "goodbye world")));
    assertNotEquals(originalHash, secondHash);
    assertNotEquals(firstHash, secondHash);

    // attempt commit using first hash which has conflicting key change.
    assertThrows(
        InconsistentValueException.class,
        () ->
            versionStore()
                .commit(
                    branch,
                    Optional.of(originalHash),
                    "metadata",
                    ImmutableList.of(Put.of(Key.of("hi"), "my world"))));
  }

  @Test
  void conflictingCommitWithHash() throws Exception {
    BranchName branch = BranchName.of("conflictingCommitWithHash");
    Hash createHash = versionStore().create(branch, Optional.empty());

    // first commit.
    Hash initialHash =
        versionStore()
            .commit(
                branch,
                Optional.of(createHash),
                "metadata",
                ImmutableList.of(Put.of(Key.of("hi"), "hello world")));
    assertNotEquals(createHash, initialHash);

    // first hash.
    Hash originalHash = versionStore().getCommits(branch).findFirst().get().getHash();
    assertEquals(initialHash, originalHash);

    // second commit.
    Hash firstHash =
        versionStore()
            .commit(
                branch,
                Optional.of(originalHash),
                "metadata",
                ImmutableList.of(Put.of(Key.of("hi"), "goodbye world")));
    assertNotEquals(originalHash, firstHash);

    // do an extra commit to make sure it has a different hash even though it has the same value.
    Hash secondHash =
        versionStore()
            .commit(
                branch,
                Optional.of(firstHash),
                "metadata",
                ImmutableList.of(Put.of(Key.of("hi"), "goodbye world")));
    assertNotEquals(originalHash, secondHash);
    assertNotEquals(firstHash, secondHash);

    // attempt commit using first hash which has conflicting key change.
    assertThrows(
        InconsistentValueException.class,
        () ->
            versionStore()
                .commit(
                    branch,
                    Optional.of(originalHash),
                    "metadata",
                    ImmutableList.of(Put.of(Key.of("hi"), "my world"))));
  }

  @Test
  void checkRefs() throws Exception {
    versionStore().create(BranchName.of("b1"), Optional.empty());
    versionStore().create(BranchName.of("b2"), Optional.empty());
    versionStore().create(TagName.of("t1"), Optional.of(InternalL1.EMPTY_ID.toHash()));
    versionStore().create(TagName.of("t2"), Optional.of(InternalL1.EMPTY_ID.toHash()));
    try (Stream<WithHash<NamedRef>> str = versionStore().getNamedRefs()) {
      assertEquals(
          ImmutableSet.of("b1", "b2", "t1", "t2"),
          str.map(wh -> wh.getValue().getName()).collect(Collectors.toSet()));
    }
  }

  @Test
  void commitRetryCountExceeded() throws Exception {
    BranchName branch = BranchName.of("commitRetryCountExceeded");
    versionStore().create(branch, Optional.empty());
    String c1 = "c1";
    String c2 = "c2";
    Key k1 = Key.of("hi");
    String v1 = "hello world";
    Key k2 = Key.of("my", "friend");
    String v2 = "not here";

    doReturn(false)
        .when(store())
        .update(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());

    assertEquals(
        "Unable to complete commit due to conflicting events. Retried 5 times before failing.",
        assertThrows(
                ReferenceConflictException.class,
                () ->
                    versionStore()
                        .commit(
                            branch,
                            Optional.empty(),
                            c1,
                            ImmutableList.of(Put.of(k1, v1), Put.of(k2, v2))))
            .getMessage());
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2, 4})
  void checkCommits(int numStoreUpdateFailures) throws Exception {
    BranchName branch = BranchName.of("checkCommits" + numStoreUpdateFailures);
    versionStore().create(branch, Optional.empty());
    String c1 = "c1";
    String c2 = "c2";
    Key k1 = Key.of("hi");
    String v1 = "hello world";
    String v1p = "goodbye world";
    Key k2 = Key.of("my", "friend");
    String v2 = "not here";

    AtomicInteger commitUpdateTry = new AtomicInteger();
    doAnswer(
            invocationOnMock -> {
              if (commitUpdateTry.getAndIncrement() < numStoreUpdateFailures) {
                return false;
              }
              return invocationOnMock.callRealMethod();
            })
        .when(store())
        .update(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
    versionStore()
        .commit(branch, Optional.empty(), c1, ImmutableList.of(Put.of(k1, v1), Put.of(k2, v2)));
    commitUpdateTry.set(0);
    versionStore().commit(branch, Optional.empty(), c2, ImmutableList.of(Put.of(k1, v1p)));

    List<WithHash<String>> commits = versionStore().getCommits(branch).collect(Collectors.toList());
    assertEquals(
        ImmutableList.of(c2, c1),
        commits.stream().map(WithHash::getValue).collect(Collectors.toList()));

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
    versionStore()
        .commit(
            branch,
            Optional.empty(),
            "metadata",
            ImmutableList.of(Put.of(Key.of("hi"), "hello world")));

    // first hash.
    Hash originalHash = versionStore().getCommits(branch).findFirst().get().getHash();

    // second commit.
    versionStore()
        .commit(
            branch,
            Optional.empty(),
            "metadata",
            ImmutableList.of(Put.of(Key.of("hi"), "goodbye world")));

    versionStore()
        .commit(
            branch,
            Optional.empty(),
            "metadata",
            ImmutableList.of(Put.of(Key.of("hi"), "goodbye world")));

    // attempt commit using first hash which has conflicting key change.
    assertThrows(
        InconsistentValueException.class,
        () ->
            versionStore()
                .commit(
                    branch,
                    Optional.of(originalHash),
                    "metadata",
                    ImmutableList.of(Put.of(Key.of("hi"), "my world"))));

    // attempt commit using first hash, put on on-conflicting key, unchanged on conflicting key.
    assertThrows(
        ReferenceConflictException.class,
        () ->
            versionStore()
                .commit(
                    branch,
                    Optional.of(originalHash),
                    "metadata",
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
      assertFalse(
          true,
          "Creating the a branch with the same name as an existing one should fail but didn't.");
    } catch (ReferenceAlreadyExistsException ex) {
      // expected.
    }

    versionStore()
        .commit(
            branch,
            Optional.empty(),
            commit1,
            ImmutableList.of(
                ImmutablePut.<String>builder().key(p1).shouldMatchHash(false).value(v1).build()));

    assertEquals(v1, versionStore().getValue(branch, p1));

    versionStore().create(tag, Optional.of(versionStore().toHash(branch)));

    versionStore()
        .commit(
            branch,
            Optional.empty(),
            commit2,
            ImmutableList.of(
                ImmutablePut.<String>builder().key(p1).shouldMatchHash(false).value(v2).build()));

    assertEquals(v2, versionStore().getValue(branch, p1));
    assertEquals(v1, versionStore().getValue(tag, p1));

    List<WithHash<String>> commits = versionStore().getCommits(branch).collect(Collectors.toList());

    assertEquals(v1, versionStore().getValue(commits.get(1).getHash(), p1));
    assertEquals(commit1, commits.get(1).getValue());
    assertEquals(v2, versionStore().getValue(commits.get(0).getHash(), p1));
    assertEquals(commit2, commits.get(0).getValue());

    versionStore().assign(tag, Optional.of(commits.get(1).getHash()), commits.get(0).getHash());

    assertEquals(commits, versionStore().getCommits(tag).collect(Collectors.toList()));
    assertEquals(
        commits, versionStore().getCommits(commits.get(0).getHash()).collect(Collectors.toList()));

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
    versionStore()
        .commit(
            branch,
            Optional.empty(),
            "metadata",
            ImmutableList.of(Put.of(Key.of("hi"), "hello world")));
    TagName tag = TagName.of("foo");
    Hash expected = versionStore().toHash(branch);
    versionStore().create(tag, Optional.of(expected));

    testRefMatchesToRef(branch, expected, branch.getName());
    testRefMatchesToRef(tag, expected, tag.getName());
    testRefMatchesToRef(expected, expected, expected.asString());
  }

  private void testRefMatchesToRef(Ref ref, Hash hash, String name)
      throws ReferenceNotFoundException {
    WithHash<Ref> val = versionStore().toRef(name);
    assertEquals(ref, val.getValue());
    assertEquals(hash, val.getHash());
  }
}
