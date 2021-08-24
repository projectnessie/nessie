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

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.ContentsAndState;
import org.projectnessie.versioned.persist.adapter.ContentsId;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitAttempt;
import org.projectnessie.versioned.persist.adapter.KeyFilterPredicate;
import org.projectnessie.versioned.persist.adapter.KeyWithBytes;
import org.projectnessie.versioned.persist.adapter.KeyWithType;

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
   * but the same contents-id.
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
    ContentsId contentsId = ContentsId.of("id-42");

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

    commit =
        ImmutableCommitAttempt.builder()
            .commitToBranch(branch)
            .commitMetaSerialized(ByteString.copyFromUtf8("initial commit meta"))
            .addPuts(
                KeyWithBytes.of(
                    oldKey,
                    contentsId,
                    (byte) 0,
                    ByteString.copyFromUtf8("initial commit contents")));
    if (param.globalState) {
      commit
          .putGlobal(contentsId, ByteString.copyFromUtf8("0"))
          .putExpectedStates(contentsId, Optional.empty());
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
                    contentsId,
                    (byte) 0,
                    ByteString.copyFromUtf8("rename commit contents")));
    if (param.globalState) {
      commit
          .putGlobal(contentsId, ByteString.copyFromUtf8("0"))
          .putExpectedStates(contentsId, Optional.of(ByteString.copyFromUtf8("0")));
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
          .putGlobal(contentsId, ByteString.copyFromUtf8("0"))
          .putExpectedStates(contentsId, Optional.of(ByteString.copyFromUtf8("0")));
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
            keys -> assertThat(keys).containsExactly(KeyWithType.of(oldKey, contentsId, (byte) 0)));

    // Verify that the commits since the rename-operation and before the delete-operation return the
    // _new_ key
    expectedCommitCount =
        renameCommitVerify(
            Stream.concat(Stream.of(hashRename), beforeDelete.stream()),
            expectedCommitCount,
            keys -> assertThat(keys).containsExactly(KeyWithType.of(newKey, contentsId, (byte) 0)));

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
      Stream<Hash> hashes, int expectedCommitCount, Consumer<Stream<KeyWithType>> keysStreamAssert)
      throws Exception {
    for (Hash hash : hashes.collect(Collectors.toList())) {
      try (Stream<KeyWithType> keys = databaseAdapter.keys(hash, KeyFilterPredicate.ALLOW_ALL)) {
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

      commit
          .addPuts(
              KeyWithBytes.of(
                  key,
                  ContentsId.of("id-" + i),
                  (byte) 0,
                  ByteString.copyFromUtf8("initial commit contents")))
          .putGlobal(ContentsId.of("id-" + i), ByteString.copyFromUtf8("0"))
          .putExpectedStates(ContentsId.of("id-" + i), Optional.empty());
    }
    Hash head = databaseAdapter.commit(commit.build());

    for (int commitNum = 0; commitNum < 3; commitNum++) {
      List<Optional<String>> contents;
      try (Stream<Optional<ContentsAndState<ByteString>>> stream =
          databaseAdapter.values(
              databaseAdapter.toHash(branch), keys, KeyFilterPredicate.ALLOW_ALL)) {
        contents =
            stream
                .map(o -> o.map(ContentsAndState::getGlobalState).map(ByteString::toStringUtf8))
                .collect(Collectors.toList());
      }
      commit =
          ImmutableCommitAttempt.builder()
              .commitToBranch(branch)
              .commitMetaSerialized(ByteString.copyFromUtf8("initial commit meta"));
      for (int i = 0; i < tablesPerCommit; i++) {
        String currentState = contents.get(i).orElseThrow(RuntimeException::new);
        String newGlobalState = Integer.toString(Integer.parseInt(currentState) + 1);

        commit
            .addPuts(
                KeyWithBytes.of(
                    keys.get(i),
                    ContentsId.of("id-" + i),
                    (byte) 0,
                    ByteString.copyFromUtf8("branch value")))
            .putGlobal(ContentsId.of("id-" + i), ByteString.copyFromUtf8(newGlobalState))
            .putExpectedStates(
                ContentsId.of("id-" + i), Optional.of(ByteString.copyFromUtf8(currentState)));
      }

      Hash newHead = databaseAdapter.commit(commit.build());
      assertThat(newHead).isNotEqualTo(head);
      head = newHead;
    }
  }
}
