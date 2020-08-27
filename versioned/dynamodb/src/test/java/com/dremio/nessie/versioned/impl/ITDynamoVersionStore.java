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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
import com.dremio.nessie.versioned.ReferenceConflictException;
import com.dremio.nessie.versioned.ReferenceNotFoundException;
import com.dremio.nessie.versioned.Serializer;
import com.dremio.nessie.versioned.StoreWorker;
import com.dremio.nessie.versioned.TagName;
import com.dremio.nessie.versioned.Unchanged;
import com.dremio.nessie.versioned.VersionStore;
import com.dremio.nessie.versioned.WithHash;
import com.dremio.nessie.versioned.impl.InconsistentValue.InconsistentValueException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.ByteString;

@ExtendWith(LocalDynamoDB.class)
class ITDynamoVersionStore {

  private DynamoStoreConfig config;

  private DynamoStore store;
  private VersionStore<String, String> impl;

  @BeforeEach
  void setup() throws URISyntaxException {
    store = new DynamoStore(config);
    store.start();
    impl = new DynamoVersionStore<>(WORKER, store, true);
  }

  @AfterEach
  void deleteResources() {
    store.deleteTables();
    store.close();
  }

  ITDynamoVersionStore() throws URISyntaxException {
    config = DynamoStoreConfig.builder().endpoint(new URI("http://localhost:8000")).build();
  }

  @Test
  void createAndDeleteTag() throws Exception {
    TagName tag = TagName.of("foo");

    // check that we can't assign an empty tag.
    assertThrows(ReferenceNotFoundException.class, () -> impl.create(tag,  Optional.empty()));

    // create a tag using the default empty hash.
    impl.create(tag, Optional.of(L1.EMPTY_ID.toHash()));
    assertEquals(L1.EMPTY_ID.toHash(), impl.toHash(tag));

    // avoid dupe
    assertThrows(ReferenceConflictException.class, () -> impl.create(tag, Optional.of(L1.EMPTY_ID.toHash())));

    // delete without condition
    impl.delete(tag, Optional.empty());

    // create a tag using the default empty hash.
    impl.create(tag, Optional.of(L1.EMPTY_ID.toHash()));

    // check that wrong id is rejected
    assertThrows(ReferenceConflictException.class, () -> impl.delete(tag, Optional.of(Id.EMPTY.toHash())));

    // delete with correct id.
    impl.delete(tag, Optional.of(L1.EMPTY_ID.toHash()));


    // avoid create to invalid l1.
    assertThrows(ReferenceNotFoundException.class, () -> impl.create(tag, Optional.of(Id.generateRandom().toHash())));

    // fail on delete of non-existent.
    assertThrows(ReferenceNotFoundException.class, () -> impl.delete(tag, Optional.empty()));
  }

  @Test
  void createAndDeleteBranch() throws Exception {
    BranchName branch = BranchName.of("foo");

    // create a tag using the default empty hash.
    impl.create(branch, Optional.of(L1.EMPTY_ID.toHash()));
    assertEquals(L1.EMPTY_ID.toHash(), impl.toHash(branch));

    // delete without condition
    impl.delete(branch, Optional.empty());

    // create a tag using no commit.
    impl.create(branch, Optional.empty());

    // avoid dupe
    assertThrows(ReferenceConflictException.class, () -> impl.create(branch, Optional.empty()));
    assertThrows(ReferenceConflictException.class, () -> impl.create(branch, Optional.of(L1.EMPTY_ID.toHash())));

    // check that wrong id is rejected for deletion (non-existing)
    assertThrows(ReferenceConflictException.class, () -> impl.delete(branch, Optional.of(Id.EMPTY.toHash())));

    // delete with correct id.
    impl.delete(branch, Optional.of(L1.EMPTY_ID.toHash()));

    // avoid create to invalid l1
    assertThrows(ReferenceNotFoundException.class, () -> impl.create(branch, Optional.of(Id.generateRandom().toHash())));

    // fail on delete of non-existent.
    assertThrows(ReferenceNotFoundException.class, () -> impl.delete(branch, Optional.empty()));

    impl.create(branch, Optional.empty());
    impl.commit(branch, Optional.empty(), "metadata", ImmutableList.of(Put.of(Key.of("hi"), "world")));
    // check that wrong id is rejected for deletion (valid but not matching)
    assertThrows(ReferenceConflictException.class, () -> impl.delete(branch, Optional.of(L1.EMPTY_ID.toHash())));

    // can't use tag delete on branch.
    assertThrows(ReferenceConflictException.class, () -> impl.delete(TagName.of("foo"), Optional.empty()));
  }

  @Test
  void conflictingCommit() throws Exception {
    BranchName branch = BranchName.of("foo");
    impl.create(branch, Optional.empty());
    // first commit.
    impl.commit(branch, Optional.empty(), "metadata", ImmutableList.of(Put.of(Key.of("hi"), "hello world")));

    //first hash.
    Hash originalHash = impl.getCommits(branch).findFirst().get().getHash();

    //second commit.
    impl.commit(branch, Optional.empty(), "metadata", ImmutableList.of(Put.of(Key.of("hi"), "goodbye world")));

    // do an extra commit to make sure it has a different hash even though it has the same value.
    impl.commit(branch, Optional.empty(), "metadata", ImmutableList.of(Put.of(Key.of("hi"), "goodbye world")));

    //attempt commit using first hash which has conflicting key change.
    assertThrows(InconsistentValueException.class, () -> impl.commit(branch, Optional.of(originalHash),
        "metadata", ImmutableList.of(Put.of(Key.of("hi"), "my world"))));
  }

  @Test
  void checkRefs() throws Exception {
    impl.create(BranchName.of("b1"), Optional.empty());
    impl.create(BranchName.of("b2"), Optional.empty());
    impl.create(TagName.of("t1"), Optional.of(L1.EMPTY_ID.toHash()));
    impl.create(TagName.of("t2"), Optional.of(L1.EMPTY_ID.toHash()));
    assertEquals(ImmutableSet.of("b1", "b2", "t1", "t2"), impl.getNamedRefs()
        .map(wh -> wh.getValue().getName()).collect(Collectors.toSet()));
  }

  @Test
  void checkCommits() throws Exception {
    BranchName branch = BranchName.of("foo");
    impl.create(branch, Optional.empty());
    String c1 = "c1";
    String c2 = "c2";
    Key k1 = Key.of("hi");
    String v1 = "hello world";
    String v1p = "goodbye world";
    Key k2 = Key.of("my", "friend");
    String v2 = "not here";
    impl.commit(branch, Optional.empty(), c1, ImmutableList.of(Put.of(k1, v1), Put.of(k2, v2)));
    impl.commit(branch, Optional.empty(), c2, ImmutableList.of(Put.of(k1, v1p)));
    List<WithHash<String>> commits = impl.getCommits(branch).collect(Collectors.toList());
    assertEquals(ImmutableList.of(c2, c1), commits.stream().map(wh -> wh.getValue()).collect(Collectors.toList()));

    // changed across commits
    assertEquals(v1, impl.getValue(commits.get(1).getHash(), k1));
    assertEquals(v1p, impl.getValue(commits.get(0).getHash(), k1));

    // not changed across commits
    assertEquals(v2, impl.getValue(commits.get(0).getHash(), k2));
    assertEquals(v2, impl.getValue(commits.get(1).getHash(), k2));

    assertEquals(2, impl.getCommits(commits.get(0).getHash()).count());

    TagName tag = TagName.of("tag1");
    impl.create(tag, Optional.of(commits.get(0).getHash()));
    assertEquals(2, impl.getCommits(tag).count());
  }

  @Test
  void assignments() throws Exception {
    BranchName branch = BranchName.of("foo");
    final Key k1 = Key.of("p1");
    impl.create(branch, Optional.empty());
    impl.commit(branch, Optional.empty(), "c1", ImmutableList.of(Put.of(k1, "v1")));
    Hash c1 = impl.toHash(branch);
    impl.commit(branch, Optional.empty(), "c1", ImmutableList.of(Put.of(k1, "v2")));
    Hash c2 = impl.toHash(branch);
    TagName t1 = TagName.of("t1");
    BranchName b2 = BranchName.of("b2");

    // ensure tag create assignment is correct.
    impl.create(t1, Optional.of(c1));
    assertEquals("v1", impl.getValue(t1, k1));

    // ensure branch create non-assignment works
    impl.create(b2, Optional.empty());
    assertEquals(null, impl.getValue(b2, k1));

    // ensure tag reassignment is correct.
    impl.assign(t1, Optional.of(c1), c2);
    assertEquals("v2", impl.getValue(t1, k1));

    // ensure branch assignment (no current) is correct
    impl.assign(b2, Optional.empty(), c1);
    assertEquals("v1", impl.getValue(b2, k1));

    // ensure branch assignment (with current) is current
    impl.assign(b2, Optional.of(c1), c2);
    assertEquals("v2", impl.getValue(b2, k1));

  }

  @Test
  void delete() throws Exception {
    BranchName branch = BranchName.of("foo");
    final Key k1 = Key.of("p1");
    impl.create(branch, Optional.empty());
    impl.commit(branch, Optional.empty(), "c1", ImmutableList.of(Put.of(k1, "v1")));
    assertEquals("v1", impl.getValue(branch, k1));

    impl.commit(branch, Optional.empty(), "c1", ImmutableList.of(Delete.of(k1)));
    assertEquals(null, impl.getValue(branch, k1));
  }

  @Test
  void unchangedOperation() throws Exception {
    BranchName branch = BranchName.of("foo");
    impl.create(branch, Optional.empty());
    // first commit.
    impl.commit(branch, Optional.empty(), "metadata", ImmutableList.of(Put.of(Key.of("hi"), "hello world")));

    //first hash.
    Hash originalHash = impl.getCommits(branch).findFirst().get().getHash();

    //second commit.
    impl.commit(branch, Optional.empty(), "metadata", ImmutableList.of(Put.of(Key.of("hi"), "goodbye world")));

    impl.commit(branch, Optional.empty(), "metadata", ImmutableList.of(Put.of(Key.of("hi"), "goodbye world")));

    //attempt commit using first hash which has conflicting key change.
    assertThrows(InconsistentValueException.class, () -> impl.commit(branch, Optional.of(originalHash),
        "metadata", ImmutableList.of(Put.of(Key.of("hi"), "my world"))));

    // attempt commit using first hash, put on on-conflicting key, unchanged on conflicting key.
    assertThrows(ReferenceConflictException.class,
        () -> impl.commit(branch, Optional.of(originalHash), "metadata",
            ImmutableList.of(Put.of(Key.of("bar"), "mellow"), Unchanged.of(Key.of("hi")))));
  }

  @Test
  void checkEmptyHistory() throws Exception {
    BranchName branch = BranchName.of("foo");
    impl.create(branch, Optional.empty());
    assertEquals(0L, impl.getCommits(branch).count());
  }

  @Disabled
  @Test
  void completeFlow() throws Exception {
    DynamoStore store = new DynamoStore(config);
    store.start();
    VersionStore<String, String> impl = new DynamoVersionStore<>(WORKER, store, true);
    final BranchName branch = ImmutableBranchName.builder().name("main").build();
    final BranchName branch2 = ImmutableBranchName.builder().name("b2").build();
    final TagName tag = ImmutableTagName.builder().name("t1").build();
    final Key p1 = ImmutableKey.builder().addElements("my.path").build();
    final String commit1 = "my commit 1";
    final String commit2 = "my commit 2";
    final String v1 = "my.value";
    final String v2 = "my.value2";

    // create a branch
    impl.create(branch, Optional.empty());

    try {
      impl.create(branch, Optional.empty());
      assertFalse(true, "Creating the a branch with the same name as an existing one should fail but didn't.");
    } catch (ReferenceConflictException ex) {
      // expected.
    }

    impl.commit(branch, Optional.empty(), commit1, ImmutableList.of(
        ImmutablePut.<String>builder().key(
            p1
            )
        .shouldMatchHash(false)
        .value(v1)
        .build()
        )
    );

    assertEquals(v1, impl.getValue(branch, p1));

    impl.create(tag, Optional.of(impl.toHash(branch)));

    impl.commit(branch, Optional.empty(), commit2, ImmutableList.of(
        ImmutablePut.<String>builder().key(
            p1
            )
        .shouldMatchHash(false)
        .value(v2)
        .build()
        )
    );

    assertEquals(v2, impl.getValue(branch, p1));
    assertEquals(v1, impl.getValue(tag, p1));

    List<WithHash<String>> commits = impl.getCommits(branch).collect(Collectors.toList());

    assertEquals(v1, impl.getValue(commits.get(1).getHash(), p1));
    assertEquals(commit1, commits.get(1).getValue());
    assertEquals(v2, impl.getValue(commits.get(0).getHash(), p1));
    assertEquals(commit2, commits.get(0).getValue());

    impl.assign(tag, Optional.of(commits.get(1).getHash()), commits.get(0).getHash());

    assertEquals(commits, impl.getCommits(tag).collect(Collectors.toList()));
    assertEquals(commits, impl.getCommits(commits.get(0).getHash()).collect(Collectors.toList()));

    assertEquals(2, impl.getNamedRefs().count());

    impl.create(branch2, Optional.of(commits.get(1).getHash()));

    impl.delete(branch, Optional.of(commits.get(0).getHash()));

    assertEquals(2, impl.getNamedRefs().count());

    assertEquals(v1, impl.getValue(branch2, p1));


  }

  private static final StoreWorker<String, String> WORKER = new StoreWorker<String, String>() {

    @Override
    public Serializer<String> getValueSerializer() {
      return STRING_SERIALIZER;
    }

    @Override
    public Serializer<String> getMetadataSerializer() {
      return STRING_SERIALIZER;
    }

    @Override
    public Stream<AssetKey> getAssetKeys(String value) {
      return Stream.of();
    }

    @Override
    public CompletableFuture<Void> deleteAsset(AssetKey key) {
      throw new UnsupportedOperationException();
    }
  };

  private static final Serializer<String> STRING_SERIALIZER = new Serializer<String>() {

    @Override
    public ByteString toBytes(String value) {
      return ByteString.copyFrom(value.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public String fromBytes(ByteString bytes) {
      return bytes.toString(StandardCharsets.UTF_8);
    }
  };
}
