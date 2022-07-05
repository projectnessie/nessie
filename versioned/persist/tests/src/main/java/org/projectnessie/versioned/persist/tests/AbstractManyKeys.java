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
package org.projectnessie.versioned.persist.tests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.mockito.Mockito.spy;
import static org.projectnessie.versioned.persist.tests.DatabaseAdapterTestUtils.ALWAYS_THROWING_ATTACHMENT_CONSUMER;

import com.google.protobuf.ByteString;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.ContentAndState;
import org.projectnessie.versioned.persist.adapter.ContentId;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitParams;
import org.projectnessie.versioned.persist.adapter.KeyFilterPredicate;
import org.projectnessie.versioned.persist.adapter.KeyList;
import org.projectnessie.versioned.persist.adapter.KeyListEntity;
import org.projectnessie.versioned.persist.adapter.KeyListEntry;
import org.projectnessie.versioned.persist.adapter.KeyWithBytes;
import org.projectnessie.versioned.persist.adapter.spi.AbstractDatabaseAdapter;
import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapter;
import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapterConfigItem;
import org.projectnessie.versioned.testworker.OnRefOnly;
import org.projectnessie.versioned.testworker.SimpleStoreWorker;

/**
 * Verifies that a big-ish number of keys, split across multiple commits works and the correct
 * results are returned for the commit-log, keys, global-states. This test is especially useful to
 * verify that the embedded and nested key-lists (think: full-key-lists in a commit-log-entry) work
 * correctly.
 */
public abstract class AbstractManyKeys {

  private final DatabaseAdapter databaseAdapter;

  protected AbstractManyKeys(DatabaseAdapter databaseAdapter) {
    this.databaseAdapter = databaseAdapter;
  }

  static class ManyKeysParams {
    final int keys;
    final int commits;

    public ManyKeysParams(int keys, int commits) {
      this.keys = keys;
      this.commits = commits;
    }

    @Override
    public String toString() {
      return "keys=" + keys + ", commits=" + commits;
    }
  }

  static List<ManyKeysParams> manyKeysParams() {
    return Arrays.asList(
        new ManyKeysParams(1000, 25),
        new ManyKeysParams(100, 10),
        new ManyKeysParams(500, 2),
        new ManyKeysParams(500, 10));
  }

  @ParameterizedTest
  @MethodSource("manyKeysParams")
  void manyKeys(ManyKeysParams params) throws Exception {
    BranchName main = BranchName.of("main");

    List<ImmutableCommitParams.Builder> commits =
        IntStream.range(0, params.commits)
            .mapToObj(
                i ->
                    ImmutableCommitParams.builder()
                        .commitMetaSerialized(ByteString.copyFromUtf8("commit #" + i))
                        .toBranch(main))
            .collect(Collectors.toList());
    AtomicInteger commitDist = new AtomicInteger();

    Set<Key> allKeys = new HashSet<>();

    IntStream.range(0, params.keys)
        .mapToObj(
            i -> {
              Key key =
                  Key.of(
                      "some",
                      Integer.toString(i),
                      "long",
                      "key",
                      "value",
                      "foobarbazfoobarbazfoobarbazfoobarbazfoobarbazfoobarbaz");
              allKeys.add(key);
              return KeyWithBytes.of(
                  key, ContentId.of("cid-" + i), (byte) 0, ByteString.copyFromUtf8("value " + i));
            })
        .forEach(kb -> commits.get(commitDist.incrementAndGet() % params.commits).addPuts(kb));

    for (ImmutableCommitParams.Builder commit : commits) {
      databaseAdapter.commit(commit.build());
    }

    Hash mainHead = databaseAdapter.hashOnReference(main, Optional.empty());
    try (Stream<KeyListEntry> keys = databaseAdapter.keys(mainHead, KeyFilterPredicate.ALLOW_ALL)) {
      List<Key> fetchedKeys = keys.map(KeyListEntry::getKey).collect(Collectors.toList());

      // containsExactlyInAnyOrderElementsOf() is quite expensive and slow with Key's
      // implementation of 'Key.equals()' since it uses a collator.
      List<String> fetchedKeysStrings =
          fetchedKeys.stream().map(Key::toString).collect(Collectors.toList());
      List<String> allKeysStrings =
          allKeys.stream().map(Key::toString).collect(Collectors.toList());

      assertThat(fetchedKeysStrings)
          .hasSize(allKeysStrings.size())
          .containsExactlyInAnyOrderElementsOf(allKeysStrings);
    }
  }

  /**
   * Nessie versions before 0.22.0 write key-lists without the ID of the commit that last updated
   * the key. This test ensures that the commit-ID is added for newly written key-lists.
   */
  @Test
  void enhanceKeyListWithCommitId(
      @NessieDbAdapter
          @NessieDbAdapterConfigItem(name = "key.list.distance", value = "5")
          @NessieDbAdapterConfigItem(name = "max.key.list.size", value = "100")
          @NessieDbAdapterConfigItem(name = "max.key.list.entity.size", value = "100")
          DatabaseAdapter da)
      throws Exception {

    // All other database adapter implementation don't work, because they use "INSERT"-ish (no
    // "upsert"), so the code to "break" the key-lists below, which is essential for the test to
    // work, will fail.
    assumeThat(da)
        .extracting(Object::getClass)
        .extracting(Class::getSimpleName)
        .isIn("InmemoryDatabaseAdapter", "RocksDatabaseAdapter");

    // Because we cannot write "old" KeyListEntry's, write a series of keys + commits here and then
    // "break" the last written key-list-entities by removing the commitID. Then write a new
    // key-list
    // and verify that the key-list-entries have the (right) commit IDs.

    @SuppressWarnings("unchecked")
    AbstractDatabaseAdapter<AutoCloseable, ?> ada = (AbstractDatabaseAdapter<AutoCloseable, ?>) da;

    int keyListDistance = ada.getConfig().getKeyListDistance();

    String longString =
        "keykeykeykeykeykeykeykeykeykeykeykeykeykeykeykeykeykeykey"
            + "keykeykeykeykeykeykeykeykeykeykeykeykeykeykeykeykeykeykey";

    BranchName branch = BranchName.of("main");
    ByteString meta = ByteString.copyFromUtf8("msg");

    Map<Key, Hash> keyToCommit = new HashMap<>();
    for (int i = 0; i < 10 * keyListDistance; i++) {
      Key key = Key.of("k" + i, longString);
      Hash hash =
          da.commit(
              ImmutableCommitParams.builder()
                  .toBranch(branch)
                  .commitMetaSerialized(meta)
                  .addPuts(
                      KeyWithBytes.of(
                          key,
                          ContentId.of("c" + i),
                          (byte) 0,
                          SimpleStoreWorker.INSTANCE.toStoreOnReferenceState(
                              OnRefOnly.onRef("r" + i, "c" + i),
                              ALWAYS_THROWING_ATTACHMENT_CONSUMER)))
                  .build());
      keyToCommit.put(key, hash);
    }

    Hash head = da.namedRef(branch.getName(), GetNamedRefsParams.DEFAULT).getHash();

    try (AutoCloseable ctx = ada.borrowConnection()) {
      // Sanity: fetchKeyLists called exactly once
      AbstractDatabaseAdapter<AutoCloseable, ?> adaSpy = spy(ada);
      adaSpy.values(head, keyToCommit.keySet(), KeyFilterPredicate.ALLOW_ALL);

      CommitLogEntry commit = ada.fetchFromCommitLog(ctx, head);
      assertThat(commit).isNotNull();
      try (Stream<KeyListEntity> keyListEntityStream =
          ada.fetchKeyLists(ctx, commit.getKeyListsIds())) {

        List<KeyListEntity> noCommitIds =
            keyListEntityStream
                .map(
                    e ->
                        KeyListEntity.of(
                            e.getId(),
                            KeyList.of(
                                e.getKeys().getKeys().stream()
                                    .filter(Objects::nonNull)
                                    .map(
                                        k ->
                                            KeyListEntry.of(
                                                k.getKey(), k.getContentId(), k.getType(), null))
                                    .collect(Collectors.toList()))))
                .collect(Collectors.toList());
        ada.writeKeyListEntities(ctx, noCommitIds);
      }

      // Cross-check that commitIDs in all KeyListEntry is null.
      try (Stream<KeyListEntity> keyListEntityStream =
          ada.fetchKeyLists(ctx, commit.getKeyListsIds())) {
        assertThat(keyListEntityStream)
            .allSatisfy(
                kl ->
                    assertThat(kl.getKeys().getKeys())
                        .allSatisfy(e -> assertThat(e.getCommitId()).isNull()));
      }
    }

    for (int i = 0; i < keyListDistance; i++) {
      Key key = Key.of("pre-fix-" + i);
      Hash hash =
          da.commit(
              ImmutableCommitParams.builder()
                  .toBranch(branch)
                  .commitMetaSerialized(meta)
                  .addPuts(
                      KeyWithBytes.of(
                          key,
                          ContentId.of("c" + i),
                          (byte) 0,
                          SimpleStoreWorker.INSTANCE.toStoreOnReferenceState(
                              OnRefOnly.onRef("pf" + i, "cpf" + i),
                              ALWAYS_THROWING_ATTACHMENT_CONSUMER)))
                  .build());
      keyToCommit.put(key, hash);
    }

    Hash fixedHead = da.namedRef(branch.getName(), GetNamedRefsParams.DEFAULT).getHash();

    try (AutoCloseable ctx = ada.borrowConnection()) {
      CommitLogEntry commit = ada.fetchFromCommitLog(ctx, fixedHead);
      assertThat(commit).isNotNull();

      // Check that commitIDs in all KeyListEntry is NOT null and points to the expected commitId.
      try (Stream<KeyListEntity> keyListEntityStream =
          ada.fetchKeyLists(ctx, commit.getKeyListsIds())) {
        assertThat(keyListEntityStream)
            .allSatisfy(
                kl ->
                    assertThat(kl.getKeys().getKeys())
                        .filteredOn(Objects::nonNull)
                        .allSatisfy(
                            e ->
                                assertThat(e)
                                    .extracting(KeyListEntry::getCommitId, KeyListEntry::getKey)
                                    .containsExactly(keyToCommit.get(e.getKey()), e.getKey())));
      }
    }
  }

  @Test
  void pointContentKeyLookups(
      @NessieDbAdapterConfigItem(name = "max.key.list.size", value = "16384")
          @NessieDbAdapterConfigItem(name = "max.key.list.entity.size", value = "16384")
          @NessieDbAdapterConfigItem(name = "key.list.distance", value = "20")
          @NessieDbAdapter
          DatabaseAdapter databaseAdapter)
      throws Exception {

    // generate a commits that lead to (at least) 5 key-list entities

    // 64 chars
    String keyElement = "1234567890123456789012345678901234567890123456789012345678901234";
    // generates "long" keys
    IntFunction<Key> keyGen = i -> Key.of("k-" + i, keyElement, keyElement);
    IntFunction<ByteString> valueGen = i -> ByteString.copyFromUtf8("value-" + i);

    BranchName branch = BranchName.of("main");
    Hash head = databaseAdapter.hashOnReference(branch, Optional.empty());

    // It's bigger in reality...
    int assumedKeyEntryLen = 160;
    int estimatedTotalKeyLength = 5 * 16384 + 16384;
    int keyNum;
    for (keyNum = 0;
        estimatedTotalKeyLength > 0;
        keyNum++, estimatedTotalKeyLength -= assumedKeyEntryLen) {
      head =
          databaseAdapter.commit(
              ImmutableCommitParams.builder()
                  .toBranch(branch)
                  .commitMetaSerialized(ByteString.EMPTY)
                  .addPuts(
                      KeyWithBytes.of(
                          keyGen.apply(keyNum),
                          ContentId.of("cid-" + keyNum),
                          (byte) 0,
                          valueGen.apply(keyNum)))
                  .build());
    }

    for (int i = 0; i < keyNum; i++) {
      Key key = keyGen.apply(i);
      Map<Key, ContentAndState<ByteString>> values =
          databaseAdapter.values(
              head, Collections.singletonList(key), KeyFilterPredicate.ALLOW_ALL);
      assertThat(values)
          .extractingByKey(key)
          .extracting(ContentAndState::getRefState)
          .isEqualTo(valueGen.apply(i));
    }
  }
}
