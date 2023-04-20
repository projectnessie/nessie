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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.projectnessie.versioned.store.DefaultStoreWorker.payloadForContent;
import static org.projectnessie.versioned.testworker.OnRefOnly.newOnRef;
import static org.projectnessie.versioned.testworker.OnRefOnly.onRef;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.ContentAndState;
import org.projectnessie.versioned.persist.adapter.ContentId;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitParams;
import org.projectnessie.versioned.persist.adapter.KeyFilterPredicate;
import org.projectnessie.versioned.persist.adapter.KeyListEntry;
import org.projectnessie.versioned.persist.adapter.KeyWithBytes;
import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapter;
import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapterConfigItem;
import org.projectnessie.versioned.store.DefaultStoreWorker;
import org.projectnessie.versioned.testworker.OnRefOnly;

/** Tests performing commits. */
public abstract class AbstractCommitScenarios {

  private final DatabaseAdapter databaseAdapter;

  protected AbstractCommitScenarios(DatabaseAdapter databaseAdapter) {
    this.databaseAdapter = databaseAdapter;
  }

  static class RenameTable {
    final int setupCommits;
    final int afterInitialCommits;
    final int afterRenameCommits;
    final int afterDeleteCommits;

    RenameTable(
        int setupCommits, int afterInitialCommits, int afterRenameCommits, int afterDeleteCommits) {
      this.setupCommits = setupCommits;
      this.afterInitialCommits = afterInitialCommits;
      this.afterRenameCommits = afterRenameCommits;
      this.afterDeleteCommits = afterDeleteCommits;
    }

    @Override
    public String toString() {
      return "setupCommits="
          + setupCommits
          + ", afterInitialCommits="
          + afterInitialCommits
          + ", afterRenameCommits="
          + afterRenameCommits
          + ", afterDeleteCommits="
          + afterDeleteCommits;
    }
  }

  static Stream<RenameTable> commitRenameTableParams() {
    Stream<RenameTable> zero = Stream.of(new RenameTable(0, 0, 0, 0));

    Stream<RenameTable> intervals =
        IntStream.of(19, 20, 21)
            .boxed()
            .flatMap(i -> Stream.of(new RenameTable(i, i, i, i), new RenameTable(0, 0, 0, 0)));

    return Stream.concat(zero, intervals);
  }

  /**
   * Test whether a "table rename operation" works as expected.
   *
   * <p>A "table rename" is effectively a remove-operation plus a put-operation with a different key
   * but the same content-id.
   *
   * <p>Parameterized to force operations to cross/not-cross the number of commits between "full key
   * lists" and whether global-state shall be used.
   */
  @ParameterizedTest
  @MethodSource("commitRenameTableParams")
  void commitRenameTable(RenameTable param) throws Exception {
    BranchName branch = BranchName.of("main");

    ContentKey dummyKey = ContentKey.of("dummy");
    ContentKey oldKey = ContentKey.of("hello-table");
    ContentKey newKey = ContentKey.of("new-name");
    ContentId contentId = ContentId.of("id-42");

    IntFunction<Hash> performDummyCommit =
        i -> {
          try {
            return databaseAdapter
                .commit(
                    ImmutableCommitParams.builder()
                        .toBranch(branch)
                        .commitMetaSerialized(ByteString.copyFromUtf8("dummy commit meta " + i))
                        .addUnchanged(dummyKey)
                        .build())
                .getCommit()
                .getHash();
          } catch (ReferenceNotFoundException | ReferenceConflictException e) {
            throw new RuntimeException(e);
          }
        };

    List<Hash> beforeInitial =
        IntStream.range(0, param.setupCommits)
            .mapToObj(performDummyCommit)
            .collect(Collectors.toList());

    ImmutableCommitParams.Builder commit;

    Content initialContent = newOnRef("initial commit content");
    Content renamContent = OnRefOnly.onRef("rename commit content", initialContent.getId());
    byte payload = (byte) payloadForContent(initialContent);

    commit =
        ImmutableCommitParams.builder()
            .toBranch(branch)
            .commitMetaSerialized(ByteString.copyFromUtf8("initial commit meta"))
            .addPuts(
                KeyWithBytes.of(
                    oldKey,
                    contentId,
                    payload,
                    DefaultStoreWorker.instance().toStoreOnReferenceState(initialContent)));
    Hash hashInitial = databaseAdapter.commit(commit.build()).getCommit().getHash();

    List<Hash> beforeRename =
        IntStream.range(0, param.afterInitialCommits)
            .mapToObj(performDummyCommit)
            .collect(Collectors.toList());

    commit =
        ImmutableCommitParams.builder()
            .toBranch(branch)
            .commitMetaSerialized(ByteString.copyFromUtf8("rename table"))
            .addDeletes(oldKey)
            .addPuts(
                KeyWithBytes.of(
                    newKey,
                    contentId,
                    payload,
                    DefaultStoreWorker.instance().toStoreOnReferenceState(renamContent)));
    Hash hashRename = databaseAdapter.commit(commit.build()).getCommit().getHash();

    List<Hash> beforeDelete =
        IntStream.range(0, param.afterRenameCommits)
            .mapToObj(performDummyCommit)
            .collect(Collectors.toList());

    commit =
        ImmutableCommitParams.builder()
            .toBranch(branch)
            .commitMetaSerialized(ByteString.copyFromUtf8("delete table"))
            .addDeletes(newKey);
    Hash hashDelete = databaseAdapter.commit(commit.build()).getCommit().getHash();

    List<Hash> afterDelete =
        IntStream.range(0, param.afterDeleteCommits)
            .mapToObj(performDummyCommit)
            .collect(Collectors.toList());

    int expectedCommitCount = 1;

    // Verify that the commits before the initial put return _no_ keys
    expectedCommitCount =
        renameCommitVerify(
            beforeInitial.stream(), expectedCommitCount, keys -> assertThat(keys).isEmpty());

    // Verify that the commits since the initial put and before the rename-operation return the
    // _old_ key
    expectedCommitCount =
        renameCommitVerify(
            Stream.concat(Stream.of(hashInitial), beforeRename.stream()),
            expectedCommitCount,
            keys ->
                assertThat(keys)
                    .containsExactly(KeyListEntry.of(oldKey, contentId, payload, hashInitial)));

    // Verify that the commits since the rename-operation and before the delete-operation return the
    // _new_ key
    expectedCommitCount =
        renameCommitVerify(
            Stream.concat(Stream.of(hashRename), beforeDelete.stream()),
            expectedCommitCount,
            keys ->
                assertThat(keys)
                    .containsExactly(KeyListEntry.of(newKey, contentId, payload, hashRename)));

    // Verify that the commits since the delete-operation return _no_ keys
    expectedCommitCount =
        renameCommitVerify(
            Stream.concat(Stream.of(hashDelete), afterDelete.stream()),
            expectedCommitCount,
            keys -> assertThat(keys).isEmpty());

    assertThat(expectedCommitCount - 1)
        .isEqualTo(
            param.setupCommits
                + 1
                + param.afterInitialCommits
                + 1
                + param.afterRenameCommits
                + 1
                + param.afterDeleteCommits);
  }

  private int renameCommitVerify(
      Stream<Hash> hashes, int expectedCommitCount, Consumer<Stream<KeyListEntry>> keysStreamAssert)
      throws Exception {
    for (Hash hash : hashes.collect(Collectors.toList())) {
      try (Stream<KeyListEntry> keys = databaseAdapter.keys(hash, KeyFilterPredicate.ALLOW_ALL)) {
        keysStreamAssert.accept(keys);
      }
      try (Stream<CommitLogEntry> log = databaseAdapter.commitLog(hash)) {
        assertThat(log).hasSize(expectedCommitCount++);
      }
    }
    return expectedCommitCount;
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 3, 5})
  void commit(int tablesPerCommit) throws Exception {
    BranchName branch = BranchName.of("main");

    ArrayList<ContentKey> keys = new ArrayList<>(tablesPerCommit);
    ImmutableCommitParams.Builder commit =
        ImmutableCommitParams.builder()
            .toBranch(branch)
            .commitMetaSerialized(ByteString.copyFromUtf8("initial commit meta"));
    for (int i = 0; i < tablesPerCommit; i++) {
      ContentKey key = ContentKey.of("my-table-num" + i);
      keys.add(key);

      String cid = "id-" + i;
      OnRefOnly c = OnRefOnly.onRef("initial commit content", cid);

      commit.addPuts(
          KeyWithBytes.of(
              key,
              ContentId.of(cid),
              (byte) payloadForContent(c),
              DefaultStoreWorker.instance().toStoreOnReferenceState(c)));
    }
    Hash head = databaseAdapter.commit(commit.build()).getCommit().getHash();

    for (int commitNum = 0; commitNum < 3; commitNum++) {
      commit =
          ImmutableCommitParams.builder()
              .toBranch(branch)
              .commitMetaSerialized(ByteString.copyFromUtf8("initial commit meta"));
      for (int i = 0; i < tablesPerCommit; i++) {
        String cid = "id-" + i;

        OnRefOnly newContent = OnRefOnly.onRef("branch value", cid);

        commit.addPuts(
            KeyWithBytes.of(
                keys.get(i),
                ContentId.of(cid),
                (byte) payloadForContent(newContent),
                DefaultStoreWorker.instance().toStoreOnReferenceState(newContent)));
      }

      Hash newHead = databaseAdapter.commit(commit.build()).getCommit().getHash();
      assertThat(newHead).isNotEqualTo(head);
      head = newHead;
    }
  }

  @Test
  void commitWithValidation() throws Exception {
    BranchName branch = BranchName.of("main");
    ContentKey key = ContentKey.of("my", "table0");
    Hash branchHead =
        databaseAdapter.namedRef(branch.getName(), GetNamedRefsParams.DEFAULT).getHash();
    String cid = "cid-0";

    RuntimeException exception = new ArithmeticException("Whatever");
    assertThatThrownBy(
            () ->
                doCommitWithValidation(
                    branch,
                    cid,
                    key,
                    () -> {
                      // do some operations here
                      databaseAdapter.globalContent(ContentId.of(cid));
                      assertThat(
                              databaseAdapter.values(
                                  branchHead,
                                  Collections.singleton(key),
                                  KeyFilterPredicate.ALLOW_ALL))
                          .isEmpty();

                      // let the custom commit-validation fail
                      throw exception;
                    }))
        .isSameAs(exception);

    assertThat(databaseAdapter.namedRef(branch.getName(), GetNamedRefsParams.DEFAULT).getHash())
        .isEqualTo(branchHead);
    assertThat(databaseAdapter.globalContent(ContentId.of(cid))).isEmpty();
  }

  void doCommitWithValidation(
      BranchName branch, String cid, ContentKey key, Callable<Void> validator) throws Exception {
    OnRefOnly c = OnRefOnly.onRef("initial commit content", cid);

    ImmutableCommitParams.Builder commit =
        ImmutableCommitParams.builder()
            .toBranch(branch)
            .commitMetaSerialized(ByteString.copyFromUtf8("initial commit meta"))
            .addPuts(
                KeyWithBytes.of(
                    key,
                    ContentId.of(cid),
                    (byte) payloadForContent(c),
                    DefaultStoreWorker.instance().toStoreOnReferenceState(c)))
            .putExpectedStates(ContentId.of(cid), Optional.empty())
            .validator(validator);
    databaseAdapter.commit(commit.build());
  }

  @Test
  void duplicateKeys() {
    BranchName branch = BranchName.of("main");

    ContentKey key = ContentKey.of("my.awesome.table");
    ContentId contentsId = ContentId.of("cid");
    OnRefOnly tableRef = newOnRef("table ref state");
    ByteString tableRefState = tableRef.serialized();

    OnRefOnly noNo = onRef("no no", contentsId.getId());
    KeyWithBytes createPut1 =
        KeyWithBytes.of(key, contentsId, (byte) payloadForContent(noNo), noNo.serialized());
    KeyWithBytes createPut2 =
        KeyWithBytes.of(key, contentsId, (byte) payloadForContent(tableRef), tableRefState);

    ImmutableCommitParams.Builder commit1 =
        ImmutableCommitParams.builder()
            .toBranch(branch)
            .commitMetaSerialized(ByteString.copyFromUtf8("initial"))
            .addPuts(createPut1)
            .addPuts(createPut2);
    assertThatThrownBy(() -> databaseAdapter.commit(commit1.build()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(key.toString());

    ImmutableCommitParams.Builder commit2 =
        ImmutableCommitParams.builder()
            .toBranch(branch)
            .commitMetaSerialized(ByteString.copyFromUtf8("initial"))
            .addDeletes(key)
            .addPuts(createPut2);
    assertThatThrownBy(() -> databaseAdapter.commit(commit2.build()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(key.toString());

    ImmutableCommitParams.Builder commit3 =
        ImmutableCommitParams.builder()
            .toBranch(branch)
            .commitMetaSerialized(ByteString.copyFromUtf8("initial"))
            .addDeletes(key)
            .addUnchanged(key);
    assertThatThrownBy(() -> databaseAdapter.commit(commit3.build()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(key.toString());

    ImmutableCommitParams.Builder commit4 =
        ImmutableCommitParams.builder()
            .toBranch(branch)
            .commitMetaSerialized(ByteString.copyFromUtf8("initial"))
            .addUnchanged(key)
            .addPuts(createPut2);
    assertThatThrownBy(() -> databaseAdapter.commit(commit4.build()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(key.toString());
  }

  @Test
  public void smallEmbeddedKeyLists(
      @NessieDbAdapterConfigItem(name = "key.list.distance", value = "1") @NessieDbAdapter
          DatabaseAdapter mine)
      throws Exception {
    BranchName branchName = BranchName.of("foo");
    Hash head = mine.noAncestorHash();
    mine.create(branchName, head);

    ContentKey keyNation = ContentKey.of("tpch-nation");
    ContentKey keyRegion = ContentKey.of("tpch-region");

    ContentId idNation = ContentId.of("id-nation");
    ContentId idRegion = ContentId.of("id-region");

    OnRefOnly onRefNation = OnRefOnly.onRef("nation", idNation.getId());
    OnRefOnly onRefRegion = OnRefOnly.onRef("region", idRegion.getId());

    ByteString stateNation = DefaultStoreWorker.instance().toStoreOnReferenceState(onRefNation);
    ByteString stateRegion = DefaultStoreWorker.instance().toStoreOnReferenceState(onRefRegion);

    Hash commitNation =
        mine.commit(
                ImmutableCommitParams.builder()
                    .toBranch(branchName)
                    .expectedHead(Optional.of(head))
                    .commitMetaSerialized(ByteString.copyFromUtf8("commit nation"))
                    .addPuts(
                        KeyWithBytes.of(
                            keyNation,
                            idNation,
                            (byte) payloadForContent(onRefNation),
                            stateNation))
                    .build())
            .getCommit()
            .getHash();

    Hash commitRegion =
        mine.commit(
                ImmutableCommitParams.builder()
                    .toBranch(branchName)
                    .expectedHead(Optional.of(commitNation))
                    .commitMetaSerialized(ByteString.copyFromUtf8("commit region"))
                    .addPuts(
                        KeyWithBytes.of(
                            keyRegion,
                            idRegion,
                            (byte) payloadForContent(onRefRegion),
                            stateRegion))
                    .build())
            .getCommit()
            .getHash();

    List<ContentKey> nonExistentKey = Collections.singletonList(ContentKey.of("non_existent"));

    assertThat(mine.values(commitNation, nonExistentKey, KeyFilterPredicate.ALLOW_ALL)).isEmpty();

    assertThat(
            mine.values(
                commitNation, Collections.singletonList(keyNation), KeyFilterPredicate.ALLOW_ALL))
        .containsEntry(
            keyNation, ContentAndState.of((byte) payloadForContent(onRefNation), stateNation));

    assertThat(
            mine.values(
                commitNation, Collections.singletonList(keyRegion), KeyFilterPredicate.ALLOW_ALL))
        .isEmpty();

    assertThat(
            mine.values(
                commitRegion, Collections.singletonList(keyNation), KeyFilterPredicate.ALLOW_ALL))
        .containsEntry(
            keyNation, ContentAndState.of((byte) payloadForContent(onRefNation), stateNation));

    assertThat(mine.values(commitNation, nonExistentKey, KeyFilterPredicate.ALLOW_ALL)).isEmpty();
    assertThat(mine.values(commitRegion, nonExistentKey, KeyFilterPredicate.ALLOW_ALL)).isEmpty();

    assertThat(
            mine.values(
                commitRegion, Arrays.asList(keyNation, keyRegion), KeyFilterPredicate.ALLOW_ALL))
        .containsEntry(
            keyNation, ContentAndState.of((byte) payloadForContent(onRefNation), stateNation))
        .containsEntry(
            keyRegion, ContentAndState.of((byte) payloadForContent(onRefRegion), stateRegion));
  }
}
