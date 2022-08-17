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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.projectnessie.versioned.testworker.OnRefOnly.newOnRef;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.IntFunction;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Commit;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.Operation;
import org.projectnessie.versioned.Put;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.tests.AbstractNestedVersionStore;

public abstract class AbstractSingleBranch extends AbstractNestedVersionStore {
  protected AbstractSingleBranch(VersionStore store) {
    super(store);
  }

  static class SingleBranchParam {
    final String branchName;
    final IntFunction<String> tableNameGen;
    final boolean allowInconsistentValueException;

    SingleBranchParam(
        String branchName,
        IntFunction<String> tableNameGen,
        boolean allowInconsistentValueException) {
      this.branchName = branchName;
      this.tableNameGen = tableNameGen;
      this.allowInconsistentValueException = allowInconsistentValueException;
    }

    @Override
    public String toString() {
      return "branchName='"
          + branchName
          + '\''
          + ", tableNameGen="
          + tableNameGen
          + ", allowInconsistentValueException="
          + allowInconsistentValueException;
    }
  }

  @SuppressWarnings("unused")
  static List<SingleBranchParam> singleBranchManyUsersCases() {
    return Arrays.asList(
        new SingleBranchParam("singleBranchManyUsersSingleTable", user -> "single-table", true),
        new SingleBranchParam(
            "singleBranchManyUsersDistinctTables",
            user -> String.format("user-table-%d", user),
            false));
  }

  /**
   * Use case simulation matrix: single branch, multiple users, each or all user updating a separate
   * or single table.
   */
  @ParameterizedTest
  @MethodSource("singleBranchManyUsersCases")
  void singleBranchManyUsers(SingleBranchParam param) throws Exception {
    BranchName branch = BranchName.of(param.branchName);

    int numUsers = 3;
    int numCommits = 20;

    Hash[] hashesKnownByUser = new Hash[numUsers];
    Hash createHash = store().create(branch, Optional.empty());
    Arrays.fill(hashesKnownByUser, createHash);

    List<CommitMeta> expectedValues = new ArrayList<>();
    for (int commitNum = 0; commitNum < numCommits; commitNum++) {
      for (int user = 0; user < numUsers; user++) {
        Hash hashKnownByUser = hashesKnownByUser[user];

        CommitMeta msg =
            CommitMeta.fromMessage(String.format("user %03d/commit %03d", user, commitNum));
        expectedValues.add(msg);

        Key key = Key.of(param.tableNameGen.apply(user));
        Content value = newOnRef(String.format("data_file_%03d_%03d", user, commitNum));
        Operation put = Put.of(key, value);

        Hash commitHash;
        List<Operation> ops = ImmutableList.of(put);
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
    List<CommitMeta> committedValues =
        commitsList(branch, s -> s.map(Commit::getCommitMeta), false);
    Collections.reverse(expectedValues);
    assertEquals(expectedValues, committedValues);
  }
}
