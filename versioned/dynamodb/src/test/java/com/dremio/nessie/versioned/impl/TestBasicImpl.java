package com.dremio.nessie.versioned.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import com.dremio.nessie.backend.dynamodb.LocalDynamoDB;
import com.dremio.nessie.versioned.BranchName;
import com.dremio.nessie.versioned.ImmutableBranchName;
import com.dremio.nessie.versioned.ImmutableKey;
import com.dremio.nessie.versioned.ImmutablePut;
import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.ReferenceConflictException;
import com.dremio.nessie.versioned.Serializer;
import com.dremio.nessie.versioned.StoreWorker;
import com.dremio.nessie.versioned.VersionStore;
import com.dremio.nessie.versioned.WithHash;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;

@ExtendWith(LocalDynamoDB.class)
class TestBasicImpl {

  @Test
  void first() throws Exception {
    DynamoStore store = new DynamoStore();
    store.start();
    VersionStore<String, String> impl = new VSImpl<>(WORKER, store);
    final BranchName branch = ImmutableBranchName.builder().name("main").build();
    final Key p1 = ImmutableKey.builder().addElements("my.path").build();
    final String commit1 = "my commit 1";
    final String commit2 = "my commit 2";
    final String v1 = "my.value";
    final String v2 = "my.value2";

    // create a branch
    impl.create(branch, Optional.empty());

    try {
      impl.create(branch, Optional.empty());
      assertFalse(true);
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

    List<WithHash<String>> commits = impl.getCommits(branch).collect(Collectors.toList());

    assertEquals(v1, impl.getValue(commits.get(1).getHash(), p1));
    assertEquals(commit1, commits.get(1).getValue());
    assertEquals(v2, impl.getValue(commits.get(0).getHash(), p1));
    assertEquals(commit2, commits.get(0).getValue());


    assertEquals(1, impl.getNamedRefs().count());
    impl.delete(branch, Optional.of(commits.get(0).getHash()));

    assertEquals(0, impl.getNamedRefs().count());
  }


  private static final StoreWorker<String, String> WORKER = new StoreWorker<String, String>(){

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
    }};

  private static final Serializer<String> STRING_SERIALIZER = new Serializer<String>() {

    @Override
    public ByteString toBytes(String value) {
      return ByteString.copyFrom(value.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public String fromBytes(ByteString bytes) {
      return bytes.toString(StandardCharsets.UTF_8);
    }};
}
