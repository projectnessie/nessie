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

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.ContentAndState;
import org.projectnessie.versioned.persist.adapter.ContentId;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitAttempt;
import org.projectnessie.versioned.persist.adapter.KeyFilterPredicate;
import org.projectnessie.versioned.persist.adapter.KeyListEntry;
import org.projectnessie.versioned.persist.adapter.KeyWithBytes;
import org.projectnessie.versioned.testworker.BaseContent;
import org.projectnessie.versioned.testworker.OnRefOnly;
import org.projectnessie.versioned.testworker.SimpleStoreWorker;
import org.projectnessie.versioned.testworker.WithGlobalStateContent;

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
    final boolean globalState;

    RenameTable(
        int setupCommits,
        int afterInitialCommits,
        int afterRenameCommits,
        int afterDeleteCommits,
        boolean globalState) {
      this.setupCommits = setupCommits;
      this.afterInitialCommits = afterInitialCommits;
      this.afterRenameCommits = afterRenameCommits;
      this.afterDeleteCommits = afterDeleteCommits;
      this.globalState = globalState;
    }

    RenameTable globalState() {
      return new RenameTable(
          setupCommits, afterInitialCommits, afterRenameCommits, afterDeleteCommits, true);
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
          + afterDeleteCommits
          + ", globalState="
          + globalState;
    }
  }

  static Stream<RenameTable> commitRenameTableParams() {
    Stream<RenameTable> zero = Stream.of(new RenameTable(0, 0, 0, 0, false));

    Stream<RenameTable> intervals =
        IntStream.of(19, 20, 21)
            .boxed()
            .flatMap(
                i ->
                    Stream.of(
                        new RenameTable(i, 0, 0, 0, false),
                        new RenameTable(0, i, 0, 0, false),
                        new RenameTable(0, 0, i, 0, false),
                        new RenameTable(0, 0, 0, i, false),
                        new RenameTable(i, i, 0, 0, false),
                        new RenameTable(i, 0, i, 0, false),
                        new RenameTable(i, 0, 0, i, false),
                        new RenameTable(0, i, 0, 0, false),
                        new RenameTable(0, i, i, 0, false),
                        new RenameTable(0, i, 0, i, false),
                        new RenameTable(i, 0, i, 0, false),
                        new RenameTable(0, i, i, 0, false),
                        new RenameTable(0, 0, i, i, false),
                        new RenameTable(i, 0, 0, i, false),
                        new RenameTable(0, i, 0, i, false),
                        new RenameTable(0, 0, i, i, false)));

    // duplicate all params to use and not use global state
    return Stream.concat(zero, intervals).flatMap(p -> Stream.of(p, p.globalState()));
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

    Key dummyKey = Key.of("dummy");
    Key oldKey = Key.of("hello", "table");
    Key newKey = Key.of("new", "name");
    ContentId contentId = ContentId.of("id-42");

    IntFunction<Hash> performDummyCommit =
        i -> {
          try {
            return databaseAdapter.commit(
                ImmutableCommitAttempt.builder()
                    .commitToBranch(branch)
                    .commitMetaSerialized(ByteString.copyFromUtf8("dummy commit meta " + i))
                    .addUnchanged(dummyKey)
                    .build());
          } catch (ReferenceNotFoundException | ReferenceConflictException e) {
            throw new RuntimeException(e);
          }
        };

    List<Hash> beforeInitial =
        IntStream.range(0, param.setupCommits)
            .mapToObj(performDummyCommit)
            .collect(Collectors.toList());

    ImmutableCommitAttempt.Builder commit;

    BaseContent initialContent;
    BaseContent renamContent;
    if (param.globalState) {
      initialContent = WithGlobalStateContent.newWithGlobal("0", "initial commit content");
      renamContent =
          WithGlobalStateContent.withGlobal("0", "rename commit content", initialContent.getId());
    } else {
      initialContent = OnRefOnly.newOnRef("initial commit content");
      renamContent = OnRefOnly.onRef("rename commit content", initialContent.getId());
    }
    byte payload = SimpleStoreWorker.INSTANCE.getPayload(initialContent);

    commit =
        ImmutableCommitAttempt.builder()
            .commitToBranch(branch)
            .commitMetaSerialized(ByteString.copyFromUtf8("initial commit meta"))
            .addPuts(
                KeyWithBytes.of(
                    oldKey,
                    contentId,
                    payload,
                    SimpleStoreWorker.INSTANCE.toStoreOnReferenceState(initialContent)));
    if (param.globalState) {
      commit
          .putGlobal(contentId, SimpleStoreWorker.INSTANCE.toStoreGlobalState(initialContent))
          .putExpectedStates(contentId, Optional.empty());
    }
    Hash hashInitial = databaseAdapter.commit(commit.build());

    List<Hash> beforeRename =
        IntStream.range(0, param.afterInitialCommits)
            .mapToObj(performDummyCommit)
            .collect(Collectors.toList());

    commit =
        ImmutableCommitAttempt.builder()
            .commitToBranch(branch)
            .commitMetaSerialized(ByteString.copyFromUtf8("rename table"))
            .addDeletes(oldKey)
            .addPuts(
                KeyWithBytes.of(
                    newKey,
                    contentId,
                    payload,
                    SimpleStoreWorker.INSTANCE.toStoreOnReferenceState(renamContent)));
    if (param.globalState) {
      commit
          .putGlobal(contentId, SimpleStoreWorker.INSTANCE.toStoreGlobalState(renamContent))
          .putExpectedStates(
              contentId,
              Optional.of(SimpleStoreWorker.INSTANCE.toStoreGlobalState(initialContent)));
    }
    Hash hashRename = databaseAdapter.commit(commit.build());

    List<Hash> beforeDelete =
        IntStream.range(0, param.afterRenameCommits)
            .mapToObj(performDummyCommit)
            .collect(Collectors.toList());

    commit =
        ImmutableCommitAttempt.builder()
            .commitToBranch(branch)
            .commitMetaSerialized(ByteString.copyFromUtf8("delete table"))
            .addDeletes(newKey);
    if (param.globalState) {
      commit
          .putGlobal(contentId, ByteString.copyFromUtf8("0"))
          .putExpectedStates(contentId, Optional.of(ByteString.copyFromUtf8("0")));
    }
    Hash hashDelete = databaseAdapter.commit(commit.build());

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

    ArrayList<Key> keys = new ArrayList<>(tablesPerCommit);
    ImmutableCommitAttempt.Builder commit =
        ImmutableCommitAttempt.builder()
            .commitToBranch(branch)
            .commitMetaSerialized(ByteString.copyFromUtf8("initial commit meta"));
    for (int i = 0; i < tablesPerCommit; i++) {
      Key key = Key.of("my", "table", "num" + i);
      keys.add(key);

      String cid = "id-" + i;
      WithGlobalStateContent c =
          WithGlobalStateContent.withGlobal("0", "initial commit content", cid);

      commit
          .addPuts(
              KeyWithBytes.of(
                  key,
                  ContentId.of(cid),
                  SimpleStoreWorker.INSTANCE.getPayload(c),
                  SimpleStoreWorker.INSTANCE.toStoreOnReferenceState(c)))
          .putGlobal(ContentId.of(cid), SimpleStoreWorker.INSTANCE.toStoreGlobalState(c))
          .putExpectedStates(ContentId.of(cid), Optional.empty());
    }
    Hash head = databaseAdapter.commit(commit.build());

    for (int commitNum = 0; commitNum < 3; commitNum++) {
      Map<Key, ContentAndState<ByteString>> values =
          databaseAdapter.values(
              databaseAdapter.hashOnReference(branch, Optional.empty()),
              keys,
              KeyFilterPredicate.ALLOW_ALL);
      commit =
          ImmutableCommitAttempt.builder()
              .commitToBranch(branch)
              .commitMetaSerialized(ByteString.copyFromUtf8("initial commit meta"));
      for (int i = 0; i < tablesPerCommit; i++) {
        String currentState = values.get(keys.get(i)).getGlobalState().toStringUtf8();
        String newGlobalState = Integer.toString(Integer.parseInt(currentState) + 1);

        String cid = "id-" + i;

        WithGlobalStateContent newContent =
            WithGlobalStateContent.withGlobal(newGlobalState, "branch value", cid);

        WithGlobalStateContent expectedContent =
            WithGlobalStateContent.withGlobal(currentState, currentState, cid);

        commit
            .addPuts(
                KeyWithBytes.of(
                    keys.get(i),
                    ContentId.of(cid),
                    SimpleStoreWorker.INSTANCE.getPayload(newContent),
                    SimpleStoreWorker.INSTANCE.toStoreOnReferenceState(newContent)))
            .putGlobal(ContentId.of(cid), SimpleStoreWorker.INSTANCE.toStoreGlobalState(newContent))
            .putExpectedStates(
                ContentId.of(cid),
                Optional.of(SimpleStoreWorker.INSTANCE.toStoreGlobalState(expectedContent)));
      }

      Hash newHead = databaseAdapter.commit(commit.build());
      assertThat(newHead).isNotEqualTo(head);
      head = newHead;
    }
  }

  @Test
  void commitWithValidation() throws Exception {
    BranchName branch = BranchName.of("main");
    Key key = Key.of("my", "table0");
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

  void doCommitWithValidation(BranchName branch, String cid, Key key, Callable<Void> validator)
      throws Exception {
    WithGlobalStateContent c =
        WithGlobalStateContent.withGlobal("0", "initial commit content", cid);

    ImmutableCommitAttempt.Builder commit =
        ImmutableCommitAttempt.builder()
            .commitToBranch(branch)
            .commitMetaSerialized(ByteString.copyFromUtf8("initial commit meta"))
            .addPuts(
                KeyWithBytes.of(
                    key,
                    ContentId.of(cid),
                    SimpleStoreWorker.INSTANCE.getPayload(c),
                    SimpleStoreWorker.INSTANCE.toStoreOnReferenceState(c)))
            .putGlobal(ContentId.of(cid), SimpleStoreWorker.INSTANCE.toStoreGlobalState(c))
            .putExpectedStates(ContentId.of(cid), Optional.empty())
            .validator(validator);
    databaseAdapter.commit(commit.build());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void duplicateKeys(boolean globalState) {
    BranchName branch = BranchName.of("main");

    Key key = Key.of("my.awesome.table");
    ContentId contentsId = ContentId.of("cid");
    ByteString tableRefState = ByteString.copyFromUtf8("table ref state");
    ByteString tableGlobalState = ByteString.copyFromUtf8("table global state");

    KeyWithBytes createPut1 =
        KeyWithBytes.of(key, contentsId, (byte) 0, ByteString.copyFromUtf8("no no"));
    KeyWithBytes createPut2 = KeyWithBytes.of(key, contentsId, (byte) 0, tableRefState);

    ImmutableCommitAttempt.Builder commit1 =
        ImmutableCommitAttempt.builder()
            .commitToBranch(branch)
            .commitMetaSerialized(ByteString.copyFromUtf8("initial"))
            .addPuts(createPut1)
            .addPuts(createPut2);
    if (globalState) {
      commit1.putGlobal(contentsId, tableGlobalState);
    }
    assertThatThrownBy(() -> databaseAdapter.commit(commit1.build()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(key.toString());

    ImmutableCommitAttempt.Builder commit2 =
        ImmutableCommitAttempt.builder()
            .commitToBranch(branch)
            .commitMetaSerialized(ByteString.copyFromUtf8("initial"))
            .addDeletes(key)
            .addPuts(createPut2);
    if (globalState) {
      commit2.putGlobal(contentsId, tableGlobalState);
    }
    assertThatThrownBy(() -> databaseAdapter.commit(commit2.build()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(key.toString());

    ImmutableCommitAttempt.Builder commit3 =
        ImmutableCommitAttempt.builder()
            .commitToBranch(branch)
            .commitMetaSerialized(ByteString.copyFromUtf8("initial"))
            .addDeletes(key)
            .addUnchanged(key);
    if (globalState) {
      commit3.putGlobal(contentsId, tableGlobalState);
    }
    assertThatThrownBy(() -> databaseAdapter.commit(commit3.build()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(key.toString());

    ImmutableCommitAttempt.Builder commit4 =
        ImmutableCommitAttempt.builder()
            .commitToBranch(branch)
            .commitMetaSerialized(ByteString.copyFromUtf8("initial"))
            .addUnchanged(key)
            .addPuts(createPut2);
    if (globalState) {
      commit4.putGlobal(contentsId, tableGlobalState);
    }
    assertThatThrownBy(() -> databaseAdapter.commit(commit4.build()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(key.toString());
  }
}
