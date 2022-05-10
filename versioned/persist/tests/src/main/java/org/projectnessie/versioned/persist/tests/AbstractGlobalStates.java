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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.persist.adapter.ContentId;
import org.projectnessie.versioned.persist.adapter.ContentIdAndBytes;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitParams;
import org.projectnessie.versioned.persist.adapter.KeyWithBytes;

/**
 * Verifies handling of global-states in the database-adapters using various combinations of number
 * of keys/content-ids, number of branches, commits per branch, and a commit-probability, which is
 * necessary to keep the heap-pressure due to the tracked state within reasonable bounds.
 */
public abstract class AbstractGlobalStates {

  private final DatabaseAdapter databaseAdapter;

  protected AbstractGlobalStates(DatabaseAdapter databaseAdapter) {
    this.databaseAdapter = databaseAdapter;
  }

  @FunctionalInterface
  interface ThrowingFunction<R> {
    R run() throws Throwable;
  }

  static <R> R catchingFunction(ThrowingFunction<R> func) {
    try {
      return func.run();
    } catch (RuntimeException e) {
      throw e;
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }

  static class GlobalStateParam {
    int branches = 1;
    int commitsPerBranch = 1;
    int tables = 1;
    double tableCommitProbability = 1.0d;

    GlobalStateParam tableCommitProbability(double tableCommitProbability) {
      this.tableCommitProbability = tableCommitProbability;
      return this;
    }

    GlobalStateParam branches(int branches) {
      this.branches = branches;
      return this;
    }

    GlobalStateParam commitsPerBranch(int commitsPerBranch) {
      this.commitsPerBranch = commitsPerBranch;
      return this;
    }

    GlobalStateParam tables(int tables) {
      this.tables = tables;
      return this;
    }

    @Override
    public String toString() {
      return "branches="
          + branches
          + ", commitsPerBranch="
          + commitsPerBranch
          + ", tables="
          + tables
          + ", tableCommitProbability="
          + tableCommitProbability;
    }
  }

  @SuppressWarnings("unused")
  static List<GlobalStateParam> globalStatesParams() {
    return Arrays.asList(
        new GlobalStateParam().branches(1).tables(1).commitsPerBranch(1),
        new GlobalStateParam().branches(3).tables(3).commitsPerBranch(3),
        // Forces multiple global_log entries
        new GlobalStateParam().branches(1).tables(1).commitsPerBranch(25),
        new GlobalStateParam()
            .branches(3)
            .tables(3)
            .commitsPerBranch(25)
            .tableCommitProbability(.01d),
        new GlobalStateParam()
            .branches(1)
            .tables(25)
            .commitsPerBranch(25)
            .tableCommitProbability(.2d),
        new GlobalStateParam()
            .branches(3)
            .tables(25)
            .commitsPerBranch(25)
            .tableCommitProbability(.2d),
        new GlobalStateParam()
            .branches(3)
            .tables(25)
            .commitsPerBranch(25)
            .tableCommitProbability(.4d));
  }

  /**
   * Rudimentary test for Nessie-GC related basic operations to collect all globally known keys and
   * the global-state-logs.
   */
  @ParameterizedTest
  @MethodSource("globalStatesParams")
  void globalStates(GlobalStateParam param) throws Exception {
    List<BranchName> branches =
        IntStream.range(0, param.branches)
            .mapToObj(i -> BranchName.of("globalStates-" + i))
            .collect(Collectors.toList());

    Map<BranchName, Hash> heads =
        branches.stream()
            .collect(
                Collectors.toMap(
                    b -> b,
                    b ->
                        catchingFunction(
                            () ->
                                databaseAdapter.create(
                                    b,
                                    databaseAdapter.hashOnReference(
                                        BranchName.of("main"), Optional.empty())))));
    Map<ContentId, ByteString> currentStates = new HashMap<>();
    Set<Key> keys =
        IntStream.range(0, param.tables)
            .mapToObj(i -> Key.of("table", Integer.toString(i)))
            .collect(Collectors.toSet());
    Set<ContentId> usedContentIds = new HashSet<>();

    Map<ContentId, ByteString> expectedGlobalStates = new HashMap<>();

    for (int commit = 0;
        commit < param.commitsPerBranch || expectedGlobalStates.isEmpty();
        commit++) {
      for (BranchName branch : branches) {
        ImmutableCommitParams.Builder commitAttempt =
            ImmutableCommitParams.builder()
                .toBranch(branch)
                .expectedHead(Optional.of(heads.get(branch)))
                .commitMetaSerialized(
                    ByteString.copyFromUtf8(
                        "some commit#" + commit + " branch " + branch.getName()));

        for (Key key : keys) {
          if (param.tableCommitProbability == 1.0f
              || ThreadLocalRandom.current().nextDouble(0d, 1d) <= param.tableCommitProbability) {
            String state = "state-commit-" + commit + "+" + key;
            String value = "value-commit-" + commit + "+" + key;
            ContentId contentId = ContentId.of(key.toString() + "-" + branch.getName());
            ByteString put = ByteString.copyFromUtf8(value);
            ByteString global = ByteString.copyFromUtf8(state);

            commitAttempt
                .putExpectedStates(contentId, Optional.ofNullable(currentStates.get(contentId)))
                .putGlobal(contentId, global)
                .addPuts(KeyWithBytes.of(key, contentId, (byte) 0, put));

            expectedGlobalStates.put(contentId, global);

            usedContentIds.add(contentId);
            currentStates.put(contentId, global);
          }
        }

        ImmutableCommitParams attempt = commitAttempt.build();
        if (!attempt.getPuts().isEmpty()) {
          heads.put(branch, databaseAdapter.commit(attempt));
        }
      }
    }

    // verify that all global-state keys (== Key + content-id) are returned (in any order)
    try (Stream<ContentId> globalKeys = databaseAdapter.globalKeys()) {
      assertThat(globalKeys).containsExactlyInAnyOrderElementsOf(expectedGlobalStates.keySet());
    }

    try (Stream<ContentIdAndBytes> allStates =
        databaseAdapter.globalContent(expectedGlobalStates.keySet())) {
      List<ContentIdAndBytes> all = allStates.collect(Collectors.toList());

      // verify that the global-state-log returns all keys (in any order)
      assertThat(all.stream().map(ContentIdAndBytes::getContentId).distinct())
          .containsExactlyInAnyOrderElementsOf(usedContentIds);

      // verify that the global-state-log returns all content-ids (in any order)
      assertThat(all.stream().map(ContentIdAndBytes::getContentId).distinct())
          .containsExactlyInAnyOrderElementsOf(currentStates.keySet());

      Collection<ByteString> allExpected = expectedGlobalStates.values();

      // verify that the global-state-log returns all state-values
      assertThat(all.stream().map(ContentIdAndBytes::getValue))
          .containsExactlyInAnyOrderElementsOf(allExpected);
    }
  }

  @Test
  void commitCheckGlobalStateMismatches() throws Exception {
    BranchName branch = BranchName.of("main");

    Hash branchInitial = databaseAdapter.hashOnReference(branch, Optional.empty());

    Hash conflict =
        databaseAdapter.commit(
            ImmutableCommitParams.builder()
                .toBranch(branch)
                .commitMetaSerialized(ByteString.EMPTY)
                .addPuts(
                    KeyWithBytes.of(
                        Key.of("my", "table", "num0"),
                        ContentId.of("id-0"),
                        (byte) 0,
                        ByteString.copyFromUtf8("there")))
                .putGlobal(ContentId.of("id-0"), ByteString.copyFromUtf8("global"))
                .build());

    assertThatThrownBy(
            () ->
                databaseAdapter.commit(
                    ImmutableCommitParams.builder()
                        .toBranch(branch)
                        .expectedHead(Optional.of(branchInitial))
                        .commitMetaSerialized(ByteString.EMPTY)
                        .addPuts(
                            KeyWithBytes.of(
                                Key.of("my", "table", "num0"),
                                ContentId.of("id-0"),
                                (byte) 0,
                                ByteString.copyFromUtf8("no no")))
                        .putGlobal(ContentId.of("id-0"), ByteString.copyFromUtf8("no no"))
                        .putExpectedStates(ContentId.of("id-0"), Optional.empty())
                        .build()))
        .isInstanceOf(ReferenceConflictException.class)
        .hasMessageContaining(
            String.format(
                "Key 'my.table.num0' has conflicting put-operation from commit '%s'.",
                conflict.asString()));

    assertThatThrownBy(
            () ->
                databaseAdapter.commit(
                    ImmutableCommitParams.builder()
                        .toBranch(branch)
                        .commitMetaSerialized(ByteString.EMPTY)
                        .addPuts(
                            KeyWithBytes.of(
                                Key.of("my", "table", "num0"),
                                ContentId.of("id-0"),
                                (byte) 0,
                                ByteString.copyFromUtf8("no no")))
                        .putGlobal(ContentId.of("id-0"), ByteString.copyFromUtf8("DUPLICATE"))
                        .putExpectedStates(ContentId.of("id-0"), Optional.empty())
                        .build()))
        .isInstanceOf(ReferenceConflictException.class)
        .hasMessageContaining("Global-state for content-id 'id-0' already exists.");

    assertThatThrownBy(
            () ->
                databaseAdapter.commit(
                    ImmutableCommitParams.builder()
                        .toBranch(branch)
                        .commitMetaSerialized(ByteString.EMPTY)
                        .addPuts(
                            KeyWithBytes.of(
                                Key.of("my", "table", "num0"),
                                ContentId.of("id-0"),
                                (byte) 0,
                                ByteString.copyFromUtf8("no no")))
                        .putGlobal(ContentId.of("id-0"), ByteString.copyFromUtf8("DUPLICATE"))
                        .putExpectedStates(
                            ContentId.of("id-0"), Optional.of(ByteString.copyFromUtf8("NOT THIS")))
                        .build()))
        .isInstanceOf(ReferenceConflictException.class)
        .hasMessageContaining("Mismatch in global-state for content-id 'id-0'.");

    assertThatThrownBy(
            () ->
                databaseAdapter.commit(
                    ImmutableCommitParams.builder()
                        .toBranch(branch)
                        .expectedHead(Optional.of(branchInitial))
                        .commitMetaSerialized(ByteString.EMPTY)
                        .addPuts(
                            KeyWithBytes.of(
                                Key.of("my", "table", "num0"),
                                ContentId.of("id-NOPE"),
                                (byte) 0,
                                ByteString.copyFromUtf8("no no")))
                        .putGlobal(ContentId.of("id-NOPE"), ByteString.copyFromUtf8("DUPLICATE"))
                        .putExpectedStates(
                            ContentId.of("id-NOPE"),
                            Optional.of(ByteString.copyFromUtf8("NOT THIS")))
                        .build()))
        .isInstanceOf(ReferenceConflictException.class)
        .hasMessageContaining("No current global-state for content-id 'id-NOPE'.");
  }
}
