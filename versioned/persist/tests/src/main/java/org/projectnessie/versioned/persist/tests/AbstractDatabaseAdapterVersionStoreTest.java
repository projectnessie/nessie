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

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.IntFunction;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Commit;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.Operation;
import org.projectnessie.versioned.Put;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.StringStoreWorker;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.persist.tests.extension.DatabaseAdapterExtension;
import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapter;
import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapterConfigItem;
import org.projectnessie.versioned.tests.AbstractVersionStoreTestBase;

@ExtendWith(DatabaseAdapterExtension.class)
@NessieDbAdapterConfigItem(name = "max.key.list.size", value = "2048")
public abstract class AbstractDatabaseAdapterVersionStoreTest extends AbstractVersionStoreTestBase {

  @NessieDbAdapter static VersionStore<String, String, StringStoreWorker.TestEnum> store;

  @Override
  protected VersionStore<String, String, StringStoreWorker.TestEnum> store() {
    return store;
  }

  @Override
  @Disabled("Not applicable for PersistVersionStore")
  protected void singleBranchManyUsersDistinctTables() {}

  @Override
  @Disabled("Not applicable for PersistVersionStore")
  protected void singleBranchManyUsersSingleTable() {}

  static class SingleBranchParam {
    final String branchName;
    final IntFunction<String> tableNameGen;
    final IntFunction<String> contentIdGen;
    final boolean allowInconsistentValueException;
    final boolean globalState;

    SingleBranchParam(
        String branchName,
        IntFunction<String> tableNameGen,
        IntFunction<String> contentIdGen,
        boolean allowInconsistentValueException,
        boolean globalState) {
      this.branchName = branchName;
      this.tableNameGen = tableNameGen;
      this.contentIdGen = contentIdGen;
      this.allowInconsistentValueException = allowInconsistentValueException;
      this.globalState = globalState;
    }

    @Override
    public String toString() {
      return "branchName='"
          + branchName
          + '\''
          + ", tableNameGen="
          + tableNameGen
          + ", allowInconsistentValueException="
          + allowInconsistentValueException
          + ", globalState="
          + globalState;
    }
  }

  @SuppressWarnings("unused")
  static List<SingleBranchParam> singleBranchManyUsersCases() {
    return Arrays.asList(
        new SingleBranchParam(
            "singleBranchManyUsersSingleTable",
            user -> "single-table",
            user -> "single-table",
            true,
            false),
        new SingleBranchParam(
            "singleBranchManyUsersSingleTableConditional",
            user -> "single-table",
            user -> "single-table",
            true,
            true),
        new SingleBranchParam(
            "singleBranchManyUsersDistinctTables",
            user -> String.format("user-table-%d", user),
            user -> String.format("user-table-%d", user),
            false,
            false),
        new SingleBranchParam(
            "singleBranchManyUsersDistinctTablesConditional",
            user -> String.format("user-table-%d", user),
            user -> String.format("user-table-%d", user),
            false,
            true));
  }

  /**
   * Use case simulation matrix: single branch, multiple users, each or all user updating a separate
   * or single table.
   */
  @ParameterizedTest
  @MethodSource("singleBranchManyUsersCases")
  void singleBranchManyUsers(SingleBranchParam param) throws Exception {
    BranchName branch = BranchName.of(param.branchName);

    int numUsers = 5;
    int numCommits = 50;

    Hash[] hashesKnownByUser = new Hash[numUsers];
    Hash createHash = store().create(branch, Optional.empty());
    Arrays.fill(hashesKnownByUser, createHash);

    List<String> expectedValues = new ArrayList<>();
    Map<Key, String> previousState = new HashMap<>();
    for (int commitNum = 0; commitNum < numCommits; commitNum++) {
      for (int user = 0; user < numUsers; user++) {
        Hash hashKnownByUser = hashesKnownByUser[user];

        String msg = String.format("user %03d/commit %03d", user, commitNum);
        expectedValues.add(msg);

        Key key = Key.of(param.tableNameGen.apply(user));
        String contentId = param.contentIdGen.apply(user);
        Operation<String> put;
        if (param.globalState) {
          String state = String.format("%03d_%03d", user, commitNum);
          if (previousState.containsKey(key)) {
            put =
                Put.of(
                    key,
                    StringStoreWorker.withStateAndId(state, "data_file", contentId),
                    StringStoreWorker.withStateAndId(previousState.get(key), "foo", contentId));
          } else {
            put = Put.of(key, StringStoreWorker.withStateAndId(state, "data_file", contentId));
          }
          previousState.put(key, state);
        } else {
          String value = String.format("data_file_%03d_%03d", user, commitNum);
          put = Put.of(key, value);
        }

        Hash commitHash;
        List<Operation<String>> ops = ImmutableList.of(put);
        try {
          commitHash = store().commit(branch, Optional.of(hashKnownByUser), msg, ops);
        } catch (ReferenceConflictException inconsistentValueException) {
          if (param.allowInconsistentValueException) {
            hashKnownByUser = store().hashOnReference(branch, Optional.empty());
            commitHash = store().commit(branch, Optional.of(hashKnownByUser), msg, ops);
          } else {
            throw inconsistentValueException;
          }
        }

        assertNotEquals(hashKnownByUser, commitHash);

        hashesKnownByUser[user] = commitHash;
      }
    }

    // Verify that all commits are there and that the order of the commits is correct
    List<String> committedValues = commitsList(branch, s -> s.map(Commit::getCommitMeta), false);
    Collections.reverse(expectedValues);
    assertEquals(expectedValues, committedValues);
  }

  enum DuplicateTableMode {
    /**
     * Non-stateful tables (think: Delta, Hive).
     *
     * <p>Okay to "blindly re-create tables": {@code Put} operations are "blindly" executed.
     */
    NO_GLOBAL,
    /**
     * Stateful tables (think: Iceberg).
     *
     * <p>Global state uses the table's {@link Key} + {@code Content.id} as the global key.
     *
     * <p>Means: creating a table with the same key and same content-id on different branches does
     * fail.
     */
    STATEFUL_SAME_CONTENT_ID,
    /**
     * Stateful tables (think: Iceberg).
     *
     * <p>Global state uses the table's {@link Key} + {@code Content.id} as the global key.
     *
     * <p>Means: creating a table with the same key but different content-ids on different branches
     * does work.
     */
    STATEFUL_DIFFERENT_CONTENT_ID
  }

  /**
   * Test behavior when a table (content-key) is created on two different branches using either
   * table-types with and without global-states and same/different content-ids.
   */
  @ParameterizedTest
  @EnumSource(value = DuplicateTableMode.class)
  void duplicateTableOnBranches(DuplicateTableMode mode) throws Throwable {
    Key key = Key.of("some", "table");

    BranchName branch0 = BranchName.of("globalStateDuplicateTable-main");
    store().create(branch0, Optional.empty());
    // commit just something to have a "real" common ancestor and not "beginning of time", which
    // means no-common-ancestor
    Hash ancestor =
        store()
            .commit(
                branch0,
                Optional.empty(),
                "initial commit",
                ImmutableList.of(Put.of(Key.of("unrelated", "table"), "value")));

    // Create a table with the same name on two branches.
    // WITH global-states, that must fail
    // WITHOUT global-state, it is okay to work
    BranchName branch1 = BranchName.of("globalStateDuplicateTable-branch1");
    BranchName branch2 = BranchName.of("globalStateDuplicateTable-branch2");
    assertThat(store().create(branch1, Optional.of(ancestor))).isEqualTo(ancestor);
    assertThat(store().create(branch2, Optional.of(ancestor))).isEqualTo(ancestor);

    List<Operation<String>> putForBranch1;
    List<Operation<String>> putForBranch2;
    String valuebranch1;
    String valuebranch2;
    switch (mode) {
      case NO_GLOBAL:
        valuebranch1 = "create table";
        valuebranch2 = "create table";
        putForBranch1 = ImmutableList.of(Put.of(key, valuebranch1));
        putForBranch2 = ImmutableList.of(Put.of(key, valuebranch2));
        break;
      case STATEFUL_SAME_CONTENT_ID:
        valuebranch1 =
            StringStoreWorker.withStateAndId("state", "create table", "content-id-equal");
        valuebranch2 =
            StringStoreWorker.withStateAndId("state", "create table", "content-id-equal");
        putForBranch1 =
            singletonList(
                Put.of(
                    key,
                    StringStoreWorker.withStateAndId("state", "create table", "content-id-equal")));
        putForBranch2 =
            singletonList(
                Put.of(
                    key,
                    StringStoreWorker.withStateAndId("state", "create table", "content-id-equal")));
        break;
      case STATEFUL_DIFFERENT_CONTENT_ID:
        valuebranch1 = StringStoreWorker.withStateAndId("state", "create table", "content-id-1");
        valuebranch2 = StringStoreWorker.withStateAndId("state", "create table", "content-id-2");
        putForBranch1 =
            singletonList(
                Put.of(
                    key,
                    StringStoreWorker.withStateAndId("state", "create table", "content-id-1")));
        putForBranch2 =
            singletonList(
                Put.of(
                    key,
                    StringStoreWorker.withStateAndId("state", "create table", "content-id-2")));
        break;
      default:
        throw new IllegalStateException();
    }

    store().commit(branch1, Optional.empty(), "create table", putForBranch1);
    assertThat(store().getValue(branch1, key)).isEqualTo(valuebranch1);

    ThrowingCallable createTableOnOtherBranch =
        () ->
            store()
                .commit(branch2, Optional.empty(), "create table on other branch", putForBranch2);

    if (mode == DuplicateTableMode.STATEFUL_SAME_CONTENT_ID) {
      assertThatThrownBy(createTableOnOtherBranch)
          .isInstanceOf(ReferenceConflictException.class)
          .hasMessage("Global-state for content-id 'content-id-equal' already exists.");
      assertThat(store().getValue(branch2, key)).isNull();
    } else {
      createTableOnOtherBranch.call();
      assertThat(store().getValue(branch2, key)).isEqualTo(valuebranch2);
    }
  }
}
