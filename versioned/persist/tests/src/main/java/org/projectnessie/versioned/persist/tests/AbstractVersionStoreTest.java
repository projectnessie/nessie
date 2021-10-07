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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Delete;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.Operation;
import org.projectnessie.versioned.Put;
import org.projectnessie.versioned.Ref;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.StringStoreWorker;
import org.projectnessie.versioned.TagName;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.VersionStoreException;
import org.projectnessie.versioned.WithHash;
import org.projectnessie.versioned.persist.tests.extension.DatabaseAdapterExtension;
import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapter;
import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapterConfigItem;
import org.projectnessie.versioned.tests.AbstractITVersionStore;

@ExtendWith(DatabaseAdapterExtension.class)
@NessieDbAdapterConfigItem(name = "max.key.list.size", value = "2048")
public abstract class AbstractVersionStoreTest extends AbstractITVersionStore {

  @NessieDbAdapter static VersionStore<String, String, StringStoreWorker.TestEnum> store;

  @Override
  protected VersionStore<String, String, StringStoreWorker.TestEnum> store() {
    return store;
  }

  @Override
  @Disabled("To be removed")
  protected void singleBranchManyUsersDistinctTables() {}

  @Override
  @Disabled("To be removed")
  protected void singleBranchManyUsersSingleTable() {}

  static class SingleBranchParam {
    final String branchName;
    final IntFunction<String> tableNameGen;
    final IntFunction<String> contentsIdGen;
    final boolean allowInconsistentValueException;
    final boolean globalState;

    SingleBranchParam(
        String branchName,
        IntFunction<String> tableNameGen,
        IntFunction<String> contentsIdGen,
        boolean allowInconsistentValueException,
        boolean globalState) {
      this.branchName = branchName;
      this.tableNameGen = tableNameGen;
      this.contentsIdGen = contentsIdGen;
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
        String contentsId = param.contentsIdGen.apply(user);
        Operation<String> put;
        if (param.globalState) {
          String state = String.format("%03d_%03d", user, commitNum);
          if (previousState.containsKey(key)) {
            put =
                Put.of(
                    key,
                    StringStoreWorker.withStateAndId(state, "data_file", contentsId),
                    StringStoreWorker.withStateAndId(previousState.get(key), "foo", contentsId));
          } else {
            put = Put.of(key, StringStoreWorker.withStateAndId(state, "data_file", contentsId));
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
            hashKnownByUser = store().toHash(branch);
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
    List<String> committedValues = commitsList(branch, s -> s.map(WithHash::getValue));
    Collections.reverse(expectedValues);
    assertEquals(expectedValues, committedValues);
  }

  /** Test behavior when a table (contents-key) is created, then dropped and then re-created. */
  @Test
  void recreateTable() throws Exception {
    BranchName branch = BranchName.of("recreateTable-main");
    Key key = Key.of("recreateTable");

    store().create(branch, Optional.empty());
    // commit just something to have a "real" common ancestor and not "beginning of time", which
    // means no-common-ancestor
    Hash ancestor =
        store()
            .commit(
                branch,
                Optional.empty(),
                "create table",
                singletonList(
                    Put.of(
                        key,
                        StringStoreWorker.withStateAndId(
                            "initial-state", "value", "CONTENTS-ID-1"))));
    assertThat(store().getValue(branch, key)).isEqualTo("initial-state|value@CONTENTS-ID-1");
    assertThat(store().getValue(ancestor, key)).isEqualTo("initial-state|value@CONTENTS-ID-1");

    Hash delete =
        store().commit(branch, Optional.empty(), "drop table", ImmutableList.of(Delete.of(key)));
    assertThat(store().getValue(branch, key)).isNull();
    assertThat(store().getValue(delete, key)).isNull();

    Hash recreate =
        store()
            .commit(
                branch,
                Optional.empty(),
                "drop table",
                ImmutableList.of(
                    Put.of(
                        key,
                        StringStoreWorker.withStateAndId(
                            "recreate-state", "value", "CONTENTS-ID-DIFFERENT"))));
    assertThat(store().getValue(branch, key))
        .isEqualTo("recreate-state|value@CONTENTS-ID-DIFFERENT");
    assertThat(store().getValue(recreate, key))
        .isEqualTo("recreate-state|value@CONTENTS-ID-DIFFERENT");
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
     * <p>Global state uses the table's {@link Key} + {@code Contents.id} as the global key.
     *
     * <p>Means: creating a table with the same key and same contents-id on different branches does
     * fail.
     */
    STATEFUL_SAME_CONTENTS_ID,
    /**
     * Stateful tables (think: Iceberg).
     *
     * <p>Global state uses the table's {@link Key} + {@code Contents.id} as the global key.
     *
     * <p>Means: creating a table with the same key but different contents-ids on different branches
     * does work.
     */
    STATEFUL_DIFFERENT_CONTENTS_ID
  }

  /**
   * Test behavior when a table (contents-key) is created on two different branches using either
   * table-types with and without global-states and same/different contents-ids.
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
      case STATEFUL_SAME_CONTENTS_ID:
        valuebranch1 =
            StringStoreWorker.withStateAndId("state", "create table", "contents-id-equal");
        valuebranch2 =
            StringStoreWorker.withStateAndId("state", "create table", "contents-id-equal");
        putForBranch1 =
            singletonList(
                Put.of(
                    key,
                    StringStoreWorker.withStateAndId(
                        "state", "create table", "contents-id-equal")));
        putForBranch2 =
            singletonList(
                Put.of(
                    key,
                    StringStoreWorker.withStateAndId(
                        "state", "create table", "contents-id-equal")));
        break;
      case STATEFUL_DIFFERENT_CONTENTS_ID:
        valuebranch1 = StringStoreWorker.withStateAndId("state", "create table", "contents-id-1");
        valuebranch2 = StringStoreWorker.withStateAndId("state", "create table", "contents-id-2");
        putForBranch1 =
            singletonList(
                Put.of(
                    key,
                    StringStoreWorker.withStateAndId("state", "create table", "contents-id-1")));
        putForBranch2 =
            singletonList(
                Put.of(
                    key,
                    StringStoreWorker.withStateAndId("state", "create table", "contents-id-2")));
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

    if (mode == DuplicateTableMode.STATEFUL_SAME_CONTENTS_ID) {
      assertThatThrownBy(createTableOnOtherBranch)
          .isInstanceOf(ReferenceConflictException.class)
          .hasMessage("Global-state for contents-id 'contents-id-equal' already exists.");
      assertThat(store().getValue(branch2, key)).isNull();
    } else {
      createTableOnOtherBranch.call();
      assertThat(store().getValue(branch2, key)).isEqualTo(valuebranch2);
    }
  }

  private static class ReferenceNotFoundFunction {

    final String name;
    String msg;
    ThrowingFunction setup;
    ThrowingFunction function;

    ReferenceNotFoundFunction(String name) {
      this.name = name;
    }

    ReferenceNotFoundFunction setup(ThrowingFunction setup) {
      this.setup = setup;
      return this;
    }

    ReferenceNotFoundFunction function(ThrowingFunction function) {
      this.function = function;
      return this;
    }

    ReferenceNotFoundFunction msg(String msg) {
      this.msg = msg;
      return this;
    }

    @Override
    public String toString() {
      return name;
    }

    @FunctionalInterface
    interface ThrowingFunction {
      void run(VersionStore<String, String, StringStoreWorker.TestEnum> store)
          throws VersionStoreException;
    }
  }

  static List<ReferenceNotFoundFunction> referenceNotFoundFunctions() {
    return Arrays.asList(
        // getCommits()
        new ReferenceNotFoundFunction("getCommits/branch")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(s -> s.getCommits(BranchName.of("this-one-should-not-exist"))),
        new ReferenceNotFoundFunction("getCommits/tag")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(s -> s.getCommits(TagName.of("this-one-should-not-exist"))),
        new ReferenceNotFoundFunction("getCommits/hash")
            .msg("Commit '12341234123412341234123412341234123412341234' not found")
            .function(s -> s.getCommits(Hash.of("12341234123412341234123412341234123412341234"))),
        // getValue()
        new ReferenceNotFoundFunction("getValue/branch")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(s -> s.getValue(BranchName.of("this-one-should-not-exist"), Key.of("foo"))),
        new ReferenceNotFoundFunction("getValue/tag")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(s -> s.getValue(TagName.of("this-one-should-not-exist"), Key.of("foo"))),
        new ReferenceNotFoundFunction("getValue/hash")
            .msg("Commit '12341234123412341234123412341234123412341234' not found")
            .function(
                s ->
                    s.getValue(
                        Hash.of("12341234123412341234123412341234123412341234"), Key.of("foo"))),
        // getValues()
        new ReferenceNotFoundFunction("getValues/branch")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(
                s ->
                    s.getValues(
                        BranchName.of("this-one-should-not-exist"), singletonList(Key.of("foo")))),
        new ReferenceNotFoundFunction("getValues/tag")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(
                s ->
                    s.getValues(
                        TagName.of("this-one-should-not-exist"), singletonList(Key.of("foo")))),
        new ReferenceNotFoundFunction("getValues/hash")
            .msg("Commit '12341234123412341234123412341234123412341234' not found")
            .function(
                s ->
                    s.getValues(
                        Hash.of("12341234123412341234123412341234123412341234"),
                        singletonList(Key.of("foo")))),
        // getKeys()
        new ReferenceNotFoundFunction("getKeys/branch")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(s -> s.getKeys(BranchName.of("this-one-should-not-exist"))),
        new ReferenceNotFoundFunction("getKeys/tag")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(s -> s.getKeys(TagName.of("this-one-should-not-exist"))),
        new ReferenceNotFoundFunction("getKeys/hash")
            .msg("Commit '12341234123412341234123412341234123412341234' not found")
            .function(s -> s.getKeys(Hash.of("12341234123412341234123412341234123412341234"))),
        // assign()
        new ReferenceNotFoundFunction("assign/branch/ok")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(
                s ->
                    s.assign(
                        BranchName.of("this-one-should-not-exist"),
                        Optional.empty(),
                        s.noAncestorHash())),
        new ReferenceNotFoundFunction("assign/hash")
            .msg("Commit '12341234123412341234123412341234123412341234' not found")
            .function(
                s ->
                    s.assign(
                        BranchName.of("main"),
                        Optional.empty(),
                        Hash.of("12341234123412341234123412341234123412341234"))),
        // delete()
        new ReferenceNotFoundFunction("delete/branch")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(s -> s.delete(BranchName.of("this-one-should-not-exist"), Optional.empty())),
        new ReferenceNotFoundFunction("delete/tag")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(s -> s.delete(BranchName.of("this-one-should-not-exist"), Optional.empty())),
        // create()
        new ReferenceNotFoundFunction("create/hash")
            .msg("Commit '12341234123412341234123412341234123412341234' not found")
            .function(
                s ->
                    s.create(
                        BranchName.of("foo"),
                        Optional.of(Hash.of("12341234123412341234123412341234123412341234")))),
        // commit()
        new ReferenceNotFoundFunction("commit/branch")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(
                s ->
                    s.commit(
                        BranchName.of("this-one-should-not-exist"),
                        Optional.empty(),
                        "meta",
                        singletonList(Delete.of(Key.of("meep"))))),
        new ReferenceNotFoundFunction("commit/hash")
            .msg(
                "Could not find commit '12341234123412341234123412341234123412341234' in reference 'main'.")
            .function(
                s ->
                    s.commit(
                        BranchName.of("main"),
                        Optional.of(Hash.of("12341234123412341234123412341234123412341234")),
                        "meta",
                        singletonList(Delete.of(Key.of("meep"))))),
        // transplant()
        new ReferenceNotFoundFunction("transplant/branch/ok")
            .msg("Named reference 'this-one-should-not-exist' not found")
            .function(
                s ->
                    s.transplant(
                        BranchName.of("this-one-should-not-exist"),
                        Optional.empty(),
                        singletonList(s.toHash(BranchName.of("main"))))),
        new ReferenceNotFoundFunction("transplant/hash/empty")
            .msg(
                "Could not find commit '12341234123412341234123412341234123412341234' in reference 'main'.")
            .function(
                s ->
                    s.transplant(
                        BranchName.of("main"),
                        Optional.of(Hash.of("12341234123412341234123412341234123412341234")),
                        singletonList(Hash.of("12341234123412341234123412341234123412341234")))),
        new ReferenceNotFoundFunction("transplant/empty/hash")
            .msg("Commit '12341234123412341234123412341234123412341234' not found")
            .function(
                s ->
                    s.transplant(
                        BranchName.of("main"),
                        Optional.empty(),
                        singletonList(Hash.of("12341234123412341234123412341234123412341234")))),
        // merge()
        new ReferenceNotFoundFunction("merge/hash/empty")
            .msg("Commit '12341234123412341234123412341234123412341234' not found")
            .function(
                s ->
                    s.merge(
                        Hash.of("12341234123412341234123412341234123412341234"),
                        BranchName.of("main"),
                        Optional.empty())),
        new ReferenceNotFoundFunction("merge/empty/hash")
            .msg(
                "Could not find commit '12341234123412341234123412341234123412341234' in reference 'main'.")
            .function(
                s ->
                    s.merge(
                        s.noAncestorHash(),
                        BranchName.of("main"),
                        Optional.of(Hash.of("12341234123412341234123412341234123412341234")))));
  }

  @ParameterizedTest
  @MethodSource("referenceNotFoundFunctions")
  void referenceNotFound(ReferenceNotFoundFunction f) throws Exception {
    if (f.setup != null) {
      f.setup.run(store());
    }
    assertThatThrownBy(() -> f.function.run(store()))
        .isInstanceOf(ReferenceNotFoundException.class)
        .hasMessage(f.msg);
  }

  /** Assigning a branch/tag to a fresh main without any commits didn't work in 0.9.2 */
  @Test
  public void assignReferenceToFreshMain()
      throws ReferenceNotFoundException, ReferenceAlreadyExistsException,
          ReferenceConflictException {
    BranchName main = BranchName.of("main");
    WithHash<Ref> mainRef = store.toRef(main.getName());
    assertThat(store().getCommits(main)).isEmpty();
    assertThat(store.getNamedRefs())
        .extracting(r -> r.getValue().getName())
        .containsExactly(main.getName());

    BranchName testBranch = BranchName.of("testBranch");
    Hash testBranchHash = store.create(testBranch, Optional.empty());
    store.assign(testBranch, Optional.of(testBranchHash), mainRef.getHash());
    assertThat(store.toRef(testBranch.getName()).getHash()).isEqualTo(mainRef.getHash());

    TagName testTag = TagName.of("testTag");
    Hash testTagHash = store.create(testTag, Optional.empty());
    store.assign(testTag, Optional.of(testTagHash), mainRef.getHash());
    assertThat(store.toRef(testTag.getName()).getHash()).isEqualTo(mainRef.getHash());
  }
}
