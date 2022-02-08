/*
 * Copyright (C) 2022 Dremio
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
import static org.projectnessie.versioned.testworker.CommitMessage.commitMessage;
import static org.projectnessie.versioned.testworker.OnRefOnly.newOnRef;
import static org.projectnessie.versioned.testworker.WithGlobalStateContent.withGlobal;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Optional;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.Operation;
import org.projectnessie.versioned.Put;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.tests.AbstractNestedVersionStore;
import org.projectnessie.versioned.testworker.BaseContent;
import org.projectnessie.versioned.testworker.CommitMessage;

public abstract class AbstractDuplicateTable extends AbstractNestedVersionStore {
  protected AbstractDuplicateTable(
      VersionStore<BaseContent, CommitMessage, BaseContent.Type> store) {
    super(store);
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
                commitMessage("initial commit"),
                ImmutableList.of(Put.of(Key.of("unrelated", "table"), newOnRef("value"))));

    // Create a table with the same name on two branches.
    // WITH global-states, that must fail
    // WITHOUT global-state, it is okay to work
    BranchName branch1 = BranchName.of("globalStateDuplicateTable-branch1");
    BranchName branch2 = BranchName.of("globalStateDuplicateTable-branch2");
    assertThat(store().create(branch1, Optional.of(ancestor))).isEqualTo(ancestor);
    assertThat(store().create(branch2, Optional.of(ancestor))).isEqualTo(ancestor);

    List<Operation<BaseContent>> putForBranch1;
    List<Operation<BaseContent>> putForBranch2;
    BaseContent valuebranch1;
    BaseContent valuebranch2;
    switch (mode) {
      case NO_GLOBAL:
        valuebranch1 = newOnRef("create table");
        valuebranch2 = newOnRef("create table");
        putForBranch1 = ImmutableList.of(Put.of(key, valuebranch1));
        putForBranch2 = ImmutableList.of(Put.of(key, valuebranch2));
        break;
      case STATEFUL_SAME_CONTENT_ID:
        valuebranch1 = withGlobal("state", "create table", "content-id-equal");
        valuebranch2 = withGlobal("state", "create table", "content-id-equal");
        putForBranch1 =
            singletonList(Put.of(key, withGlobal("state", "create table", "content-id-equal")));
        putForBranch2 =
            singletonList(Put.of(key, withGlobal("state", "create table", "content-id-equal")));
        break;
      case STATEFUL_DIFFERENT_CONTENT_ID:
        valuebranch1 = withGlobal("state", "create table", "content-id-1");
        valuebranch2 = withGlobal("state", "create table", "content-id-2");
        putForBranch1 =
            singletonList(Put.of(key, withGlobal("state", "create table", "content-id-1")));
        putForBranch2 =
            singletonList(Put.of(key, withGlobal("state", "create table", "content-id-2")));
        break;
      default:
        throw new IllegalStateException();
    }

    store().commit(branch1, Optional.empty(), commitMessage("create table"), putForBranch1);
    assertThat(store().getValue(branch1, key)).isEqualTo(valuebranch1);

    ThrowingCallable createTableOnOtherBranch =
        () ->
            store()
                .commit(
                    branch2,
                    Optional.empty(),
                    commitMessage("create table on other branch"),
                    putForBranch2);

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
