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
package org.projectnessie.versioned.tests;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.projectnessie.versioned.testworker.OnRefOnly.newOnRef;
import static org.projectnessie.versioned.testworker.OnRefOnly.onRef;

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
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.ImmutableCommitMeta;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Commit;
import org.projectnessie.versioned.CommitResult;
import org.projectnessie.versioned.ContentResult;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.VersionStore;

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
    Hash createHash = store().create(branch, Optional.empty()).getHash();
    Arrays.fill(hashesKnownByUser, createHash);

    List<CommitMeta> expectedValues = new ArrayList<>();
    Hash parent = createHash;
    for (int commitNum = 0; commitNum < numCommits; commitNum++) {
      for (int user = 0; user < numUsers; user++) {
        Hash hashKnownByUser = hashesKnownByUser[user];

        ImmutableCommitMeta.Builder msg =
            CommitMeta.builder()
                .message(String.format("user %03d/commit %03d", user, commitNum))
                .addParentCommitHashes(parent.asString());

        ContentKey key = ContentKey.of(param.tableNameGen.apply(user));
        List<Operation> ops =
            singleBranchManyUsersOps(branch, commitNum, user, hashKnownByUser, key);

        CommitResult commitHash;
        try {
          commitHash = store().commit(branch, Optional.of(hashKnownByUser), msg.build(), ops);
        } catch (ReferenceConflictException inconsistentValueException) {
          if (param.allowInconsistentValueException) {
            hashKnownByUser = store().hashOnReference(branch, Optional.empty(), emptyList());
            ops = singleBranchManyUsersOps(branch, commitNum, user, hashKnownByUser, key);
            commitHash = store().commit(branch, Optional.of(hashKnownByUser), msg.build(), ops);
          } else {
            throw inconsistentValueException;
          }
        }

        parent = commitHash.getCommitHash();

        expectedValues.add(msg.hash(parent.asString()).build());

        assertNotEquals(hashKnownByUser, commitHash);

        hashesKnownByUser[user] = parent;
      }
    }

    // Verify that all commits are there and that the order of the commits is correct
    List<CommitMeta> committedValues =
        commitsListMap(branch, Integer.MAX_VALUE, Commit::getCommitMeta);
    Collections.reverse(expectedValues);
    assertThat(committedValues).containsExactlyElementsOf(expectedValues);
  }

  private List<Operation> singleBranchManyUsersOps(
      BranchName branch, int commitNum, int user, Hash hashKnownByUser, ContentKey key)
      throws ReferenceNotFoundException {
    List<Operation> ops;
    ContentResult existing =
        store()
            .getValue(
                store.hashOnReference(branch, Optional.of(hashKnownByUser), emptyList()),
                key,
                false);
    Content value =
        existing != null
            ? onRef(
                String.format("data_file_%03d_%03d", user, commitNum),
                requireNonNull(existing.content()).getId())
            : newOnRef(String.format("data_file_%03d_%03d", user, commitNum));
    ops = ImmutableList.of(Put.of(key, value));
    return ops;
  }
}
