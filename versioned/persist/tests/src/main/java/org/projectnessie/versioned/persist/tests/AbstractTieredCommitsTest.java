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
import static org.junit.jupiter.api.Assertions.assertAll;

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Diff;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.ReferenceRetryFailureException;
import org.projectnessie.versioned.TagName;
import org.projectnessie.versioned.WithHash;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.ContentsAndState;
import org.projectnessie.versioned.persist.adapter.ContentsId;
import org.projectnessie.versioned.persist.adapter.ContentsIdAndBytes;
import org.projectnessie.versioned.persist.adapter.ContentsIdWithType;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapterConfig;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapterFactory;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitAttempt;
import org.projectnessie.versioned.persist.adapter.KeyFilterPredicate;
import org.projectnessie.versioned.persist.adapter.KeyWithBytes;
import org.projectnessie.versioned.persist.adapter.KeyWithType;

public abstract class AbstractTieredCommitsTest<CONFIG extends DatabaseAdapterConfig> {
  protected static DatabaseAdapter databaseAdapter;

  @BeforeEach
  void loadDatabaseAdapter() {
    if (databaseAdapter == null) {
      databaseAdapter =
          DatabaseAdapterFactory.<CONFIG>loadFactoryByName(adapterName())
              .newBuilder()
              .configure(SystemPropertiesConfigurer::configureFromSystemProperties)
              // default to a quite small max-size for the CommitLogEntry.keyList + KeyListEntity
              .configure(c -> c.withMaxKeyListSize(2048))
              .configure(this::configureDatabaseAdapter)
              .build();
    }
    databaseAdapter.reinitializeRepo("main");
  }

  protected abstract String adapterName();

  protected CONFIG configureDatabaseAdapter(CONFIG config) {
    return config;
  }

  @AfterAll
  static void closeDatabaseAdapter() throws Exception {
    try {
      if (databaseAdapter != null) {
        databaseAdapter.close();
      }
    } finally {
      databaseAdapter = null;
    }
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
      return "branches-" + branches + "-commitsPerBranch=" + commitsPerBranch + "-tables-" + tables;
    }
  }

  @SuppressWarnings("unused")
  static List<GlobalStateParam> globalStatesParams() {
    return Arrays.asList(
        new GlobalStateParam().branches(1).tables(1).commitsPerBranch(1),
        new GlobalStateParam().branches(3).tables(3).commitsPerBranch(3),
        // Forces multiple global_log entries
        new GlobalStateParam().branches(1).tables(1).commitsPerBranch(500),
        new GlobalStateParam()
            .branches(1)
            .tables(1000)
            .commitsPerBranch(100)
            .tableCommitProbability(.05d),
        new GlobalStateParam()
            .branches(3)
            .tables(1000)
            .commitsPerBranch(100)
            .tableCommitProbability(.01d),
        new GlobalStateParam()
            .branches(3)
            .tables(100)
            .commitsPerBranch(1000)
            .tableCommitProbability(.01d),
        new GlobalStateParam()
            .branches(1)
            .tables(100)
            .commitsPerBranch(100)
            .tableCommitProbability(.2d),
        new GlobalStateParam()
            .branches(3)
            .tables(100)
            .commitsPerBranch(100)
            .tableCommitProbability(.2d),
        new GlobalStateParam()
            .branches(3)
            .tables(30)
            .commitsPerBranch(30)
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
                                    b, databaseAdapter.toHash(BranchName.of("main"))))));
    Map<ContentsId, ByteString> currentStates = new HashMap<>();
    Set<Key> keys =
        IntStream.range(0, param.tables)
            .mapToObj(i -> Key.of("table", Integer.toString(i)))
            .collect(Collectors.toSet());
    Set<ContentsId> usedContentIds = new HashSet<>();

    Map<ContentsIdWithType, ByteString> expectedGlobalStates = new HashMap<>();
    Map<KeyWithType, List<ByteString>> expectedContents = new HashMap<>();

    for (int commit = 0; commit < param.commitsPerBranch; commit++) {
      for (BranchName branch : branches) {
        ImmutableCommitAttempt.Builder commitAttempt =
            ImmutableCommitAttempt.builder()
                .commitToBranch(branch)
                .expectedHead(Optional.of(heads.get(branch)))
                .commitMetaSerialized(
                    ByteString.copyFromUtf8(
                        "some commit#" + commit + " branch " + branch.getName()));

        for (Key key : keys) {
          if (param.tableCommitProbability == 1.0f
              || ThreadLocalRandom.current().nextDouble(0d, 1d) <= param.tableCommitProbability) {
            String state = "state-commit-" + commit + "+" + key;
            String value = "value-commit-" + commit + "+" + key;
            ContentsId contentsId = ContentsId.of(key.toString() + "-" + branch.getName());
            ByteString put = ByteString.copyFromUtf8(value);
            ByteString global = ByteString.copyFromUtf8(state);

            commitAttempt
                .putExpectedStates(contentsId, Optional.ofNullable(currentStates.get(contentsId)))
                .putGlobal(contentsId, global)
                .addPuts(KeyWithBytes.of(key, contentsId, (byte) 0, put));

            expectedGlobalStates.put(ContentsIdWithType.of(contentsId, (byte) 0), global);

            expectedContents
                .computeIfAbsent(KeyWithType.of(key, contentsId, (byte) 0), k -> new ArrayList<>())
                .add(put);

            usedContentIds.add(contentsId);
            currentStates.put(contentsId, global);
          }
        }

        ImmutableCommitAttempt attempt = commitAttempt.build();
        if (!attempt.getPuts().isEmpty()) {
          heads.put(branch, databaseAdapter.commit(attempt));
        }
      }
    }

    // verify that all global-state keys (== Key + contents-id) are returned (in any order)
    try (Stream<ContentsIdWithType> globalKeys = databaseAdapter.globalKeys(x -> 0)) {
      assertThat(globalKeys).containsExactlyInAnyOrderElementsOf(expectedGlobalStates.keySet());
    }

    try (Stream<ContentsIdAndBytes> allStates =
        databaseAdapter.globalLog(expectedGlobalStates.keySet(), s -> 0)) {
      List<ContentsIdAndBytes> all = allStates.collect(Collectors.toList());

      // verify that the global-state-log returns all keys (in any order)
      assertThat(all.stream().map(ContentsIdAndBytes::getContentsId).distinct())
          .containsExactlyInAnyOrderElementsOf(usedContentIds);

      // verify that the global-state-log returns all contents-ids (in any order)
      assertThat(all.stream().map(ContentsIdAndBytes::getContentsId).distinct())
          .containsExactlyInAnyOrderElementsOf(currentStates.keySet());

      Collection<ByteString> allExpected = expectedGlobalStates.values();

      // verify that the global-state-log returns all state-values
      assertThat(all.stream().map(ContentsIdAndBytes::getValue))
          .containsExactlyInAnyOrderElementsOf(allExpected);
    }

    try (Stream<KeyWithBytes> contents = databaseAdapter.allContents((ref, entry) -> true)) {
      List<KeyWithBytes> all = contents.collect(Collectors.toList());

      List<ByteString> allExpected =
          expectedContents.values().stream()
              .flatMap(Collection::stream)
              .collect(Collectors.toList());

      // verify that the all contents are returned
      assertThat(all.stream().map(KeyWithBytes::getValue))
          .containsExactlyInAnyOrderElementsOf(allExpected);
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 19, 20, 21, 39, 40, 41, 49, 50, 51, 255, 256, 257, 500})
  // Note: 1000 commits is quite the max that in-JVM H2 database can handle
  void manyCommits(int numCommits) throws Exception {
    BranchName branch = BranchName.of("manyCommits-" + numCommits);
    databaseAdapter.create(branch, databaseAdapter.toHash(BranchName.of("main")));

    Hash[] commits = new Hash[numCommits];

    ContentsId fixed = ContentsId.of("FIXED");

    for (int i = 0; i < numCommits; i++) {
      Key key = Key.of("many", "commits", Integer.toString(numCommits));
      ImmutableCommitAttempt.Builder commit =
          ImmutableCommitAttempt.builder()
              .commitToBranch(branch)
              .commitMetaSerialized(ByteString.copyFromUtf8("commit #" + i + " of " + numCommits))
              .addPuts(
                  KeyWithBytes.of(
                      key,
                      fixed,
                      (byte) 0,
                      ByteString.copyFromUtf8("value for #" + i + " of " + numCommits)))
              .putGlobal(fixed, ByteString.copyFromUtf8("state for #" + i + " of " + numCommits));
      if (i > 0) {
        commit.putExpectedStates(
            fixed,
            Optional.of(ByteString.copyFromUtf8("state for #" + (i - 1) + " of " + numCommits)));
      }
      Hash hash = databaseAdapter.commit(commit.build());
      commits[i] = hash;

      try (Stream<ContentsIdAndBytes> globals =
          databaseAdapter.globalLog(
              Collections.singleton(ContentsIdWithType.of(fixed, (byte) 0)), bs -> (byte) 0)) {
        assertThat(globals)
            .containsExactly(
                ContentsIdAndBytes.of(
                    fixed,
                    (byte) 0,
                    ByteString.copyFromUtf8("state for #" + i + " of " + numCommits)));
      }
    }

    try (Stream<CommitLogEntry> log = databaseAdapter.commitLog(databaseAdapter.toHash(branch))) {
      assertThat(log.count()).isEqualTo(numCommits);
    }

    for (int i = 0; i < numCommits; i++) {
      Key key = Key.of("many", "commits", Integer.toString(numCommits));

      try (Stream<Optional<ContentsAndState<ByteString>>> x =
          databaseAdapter.values(
              databaseAdapter.hashOnReference(branch, Optional.of(commits[i])),
              Collections.singletonList(key),
              KeyFilterPredicate.ALLOW_ALL)) {
        ByteString expect = ByteString.copyFromUtf8("value for #" + i + " of " + numCommits);
        assertThat(x.map(o -> o.map(ContentsAndState::getRefState)))
            .containsExactly(Optional.of(expect));
      }

      try (Stream<Optional<ContentsAndState<ByteString>>> x =
          databaseAdapter.values(
              databaseAdapter.hashOnReference(branch, Optional.of(commits[i])),
              Collections.singletonList(key),
              KeyFilterPredicate.ALLOW_ALL)) {
        ByteString expect =
            ByteString.copyFromUtf8("state for #" + (numCommits - 1) + " of " + numCommits);
        assertThat(x.map(o -> o.map(ContentsAndState::getGlobalState)))
            .containsExactly(Optional.of(expect));
      }

      try (Stream<KeyWithType> keys =
          databaseAdapter.keys(
              databaseAdapter.hashOnReference(branch, Optional.of(commits[i])),
              KeyFilterPredicate.ALLOW_ALL)) {
        assertThat(keys.map(KeyWithType::getKey)).containsExactly(key);
      }
    }

    databaseAdapter.delete(branch, Optional.empty());
  }

  @Test
  void commitCheckGlobalStateMismatches() throws Exception {
    BranchName branch = BranchName.of("main");

    Hash branchInitial = databaseAdapter.toHash(branch);

    databaseAdapter.commit(
        ImmutableCommitAttempt.builder()
            .commitToBranch(branch)
            .commitMetaSerialized(ByteString.EMPTY)
            .addPuts(
                KeyWithBytes.of(
                    Key.of("my", "table", "num0"),
                    ContentsId.of("id-0"),
                    (byte) 0,
                    ByteString.copyFromUtf8("there")))
            .putGlobal(ContentsId.of("id-0"), ByteString.copyFromUtf8("global"))
            .build());

    assertThatThrownBy(
            () ->
                databaseAdapter.commit(
                    ImmutableCommitAttempt.builder()
                        .commitToBranch(branch)
                        .expectedHead(Optional.of(branchInitial))
                        .commitMetaSerialized(ByteString.EMPTY)
                        .addPuts(
                            KeyWithBytes.of(
                                Key.of("my", "table", "num0"),
                                ContentsId.of("id-0"),
                                (byte) 0,
                                ByteString.copyFromUtf8("no no")))
                        .putGlobal(ContentsId.of("id-0"), ByteString.copyFromUtf8("no no"))
                        .putExpectedStates(ContentsId.of("id-0"), Optional.empty())
                        .build()))
        .isInstanceOf(ReferenceConflictException.class)
        .hasMessageContaining("Key 'my.table.num0' has put-operation.");

    assertThatThrownBy(
            () ->
                databaseAdapter.commit(
                    ImmutableCommitAttempt.builder()
                        .commitToBranch(branch)
                        .commitMetaSerialized(ByteString.EMPTY)
                        .addPuts(
                            KeyWithBytes.of(
                                Key.of("my", "table", "num0"),
                                ContentsId.of("id-0"),
                                (byte) 0,
                                ByteString.copyFromUtf8("no no")))
                        .putGlobal(ContentsId.of("id-0"), ByteString.copyFromUtf8("DUPLICATE"))
                        .putExpectedStates(ContentsId.of("id-0"), Optional.empty())
                        .build()))
        .isInstanceOf(ReferenceConflictException.class)
        .hasMessageContaining("Global-state for contents-id 'id-0' already exists.");

    assertThatThrownBy(
            () ->
                databaseAdapter.commit(
                    ImmutableCommitAttempt.builder()
                        .commitToBranch(branch)
                        .commitMetaSerialized(ByteString.EMPTY)
                        .addPuts(
                            KeyWithBytes.of(
                                Key.of("my", "table", "num0"),
                                ContentsId.of("id-0"),
                                (byte) 0,
                                ByteString.copyFromUtf8("no no")))
                        .putGlobal(ContentsId.of("id-0"), ByteString.copyFromUtf8("DUPLICATE"))
                        .putExpectedStates(
                            ContentsId.of("id-0"), Optional.of(ByteString.copyFromUtf8("NOT THIS")))
                        .build()))
        .isInstanceOf(ReferenceConflictException.class)
        .hasMessageContaining("Mismatch in global-state for contents-id 'id-0'.");

    assertThatThrownBy(
            () ->
                databaseAdapter.commit(
                    ImmutableCommitAttempt.builder()
                        .commitToBranch(branch)
                        .expectedHead(Optional.of(branchInitial))
                        .commitMetaSerialized(ByteString.EMPTY)
                        .addPuts(
                            KeyWithBytes.of(
                                Key.of("my", "table", "num0"),
                                ContentsId.of("id-NOPE"),
                                (byte) 0,
                                ByteString.copyFromUtf8("no no")))
                        .putGlobal(ContentsId.of("id-NOPE"), ByteString.copyFromUtf8("DUPLICATE"))
                        .putExpectedStates(
                            ContentsId.of("id-NOPE"),
                            Optional.of(ByteString.copyFromUtf8("NOT THIS")))
                        .build()))
        .isInstanceOf(ReferenceConflictException.class)
        .hasMessageContaining("No current global-state for contents-id 'id-NOPE'.");
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

  static class Variation {
    final int threads = Math.max(4, Runtime.getRuntime().availableProcessors());
    final boolean singleBranch;
    final boolean sharedKeys;
    final int tables;

    Variation(boolean singleBranch, boolean sharedKeys, int tables) {
      this.singleBranch = singleBranch;
      this.sharedKeys = sharedKeys;
      this.tables = tables;
    }

    @Override
    public String toString() {
      return "threads="
          + threads
          + ", singleBranch="
          + singleBranch
          + ", sharedKeys="
          + sharedKeys
          + ", tables="
          + tables;
    }
  }

  /** Cartesian product of all {@link Variation}s for {@link #concurrency(Variation)}. */
  @SuppressWarnings("unused")
  static Stream<Variation> concurrencyVariations() {
    return Stream.of(Boolean.FALSE, Boolean.TRUE)
        .flatMap(
            singleBranch ->
                Stream.of(Boolean.FALSE, Boolean.TRUE)
                    .flatMap(
                        sharedKeys ->
                            Stream.of(3)
                                .map(tables -> new Variation(singleBranch, sharedKeys, tables))));
  }

  @ParameterizedTest
  @MethodSource("concurrencyVariations")
  void concurrency(Variation variation) throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(variation.threads);
    AtomicInteger commitsOK = new AtomicInteger();
    AtomicInteger retryFailures = new AtomicInteger();
    AtomicBoolean stopFlag = new AtomicBoolean();
    List<Runnable> tasks = new ArrayList<>(variation.threads);
    Map<Key, ContentsId> keyToContentsId = new HashMap<>();
    try {
      CountDownLatch startLatch = new CountDownLatch(1);
      CountDownLatch stopLatch = new CountDownLatch(variation.threads);
      Map<BranchName, Set<Key>> keysPerBranch = new HashMap<>();
      for (int i = 0; i < variation.threads; i++) {
        BranchName branch =
            BranchName.of("concurrency-" + ((variation.singleBranch ? "shared" : i)));
        List<Key> keys = new ArrayList<>(variation.tables);

        for (int k = 0; k < variation.tables; k++) {
          Key key =
              Key.of(
                  "some",
                  "key",
                  variation.sharedKeys ? "shared" : Integer.toString(i),
                  "table-" + k);
          keys.add(key);
          keyToContentsId.put(
              key,
              ContentsId.of(
                  String.format(
                      "%s-table-%d", variation.sharedKeys ? "shared" : Integer.toString(i), k)));
          keysPerBranch.computeIfAbsent(branch, x -> new HashSet<>()).add(key);
        }

        tasks.add(
            () -> {
              try {
                assertThat(startLatch.await(2, TimeUnit.SECONDS)).isTrue();

                for (int commit = 0; ; commit++) {
                  if (stopFlag.get()) {
                    stopLatch.countDown();
                    break;
                  }

                  List<ByteString> currentStates;
                  try (Stream<ByteString> str =
                      databaseAdapter
                          .values(
                              databaseAdapter.toHash(branch), keys, KeyFilterPredicate.ALLOW_ALL)
                          .collect(Collectors.toList())
                          .stream()
                          .map(Optional::get)
                          .map(ContentsAndState::getGlobalState)) {
                    currentStates = str.collect(Collectors.toList());
                  }

                  ImmutableCommitAttempt.Builder commitAttempt = ImmutableCommitAttempt.builder();

                  for (int ki = 0; ki < keys.size(); ki++) {
                    Key key = keys.get(ki);
                    ContentsId contentsId = keyToContentsId.get(key);
                    commitAttempt.putGlobal(
                        contentsId,
                        ByteString.copyFromUtf8(
                            Integer.toString(
                                Integer.parseInt(currentStates.get(ki).toStringUtf8()) + 1)));
                    if (!variation.sharedKeys) {
                      commitAttempt.putExpectedStates(
                          contentsId, Optional.of(currentStates.get(ki)));
                    }
                    commitAttempt.addPuts(
                        KeyWithBytes.of(keys.get(ki), contentsId, (byte) 0, ByteString.EMPTY));
                  }

                  try {
                    commitAttempt
                        .commitToBranch(branch)
                        .commitMetaSerialized(
                            ByteString.copyFromUtf8(
                                "commit #"
                                    + commit
                                    + " to "
                                    + branch.getName()
                                    + " something "
                                    + ThreadLocalRandom.current().nextLong()));
                    databaseAdapter.commit(commitAttempt.build());
                    commitsOK.incrementAndGet();
                  } catch (ReferenceRetryFailureException retry) {
                    retryFailures.incrementAndGet();
                  }
                }
              } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
              }
            });
      }

      for (Entry<BranchName, Set<Key>> branchKeys : keysPerBranch.entrySet()) {
        BranchName branch = branchKeys.getKey();
        databaseAdapter.create(branch, databaseAdapter.toHash(BranchName.of("main")));
        ImmutableCommitAttempt.Builder commitAttempt =
            ImmutableCommitAttempt.builder()
                .commitToBranch(branchKeys.getKey())
                .commitMetaSerialized(
                    ByteString.copyFromUtf8("initial commit for " + branch.getName()));
        for (Key k : branchKeys.getValue()) {
          ContentsId contentsId = keyToContentsId.get(k);
          commitAttempt.addPuts(KeyWithBytes.of(k, contentsId, (byte) 0, ByteString.EMPTY));
          commitAttempt.putGlobal(contentsId, ByteString.copyFromUtf8("0"));
        }
        databaseAdapter.commit(commitAttempt.build());
      }

      tasks.forEach(executor::submit);

      startLatch.countDown();
      Thread.sleep(1_500);
      stopFlag.set(true);

      // 30 seconds is long, but necessary to let transactional databases detect deadlocks, which
      // cause Nessie-commit-retries.
      assertThat(stopLatch.await(30, TimeUnit.SECONDS)).isTrue();

    } finally {
      stopFlag.set(true);

      System.out.printf(
          "AbstractTieredCommitsTest.concurrency - %s : Commits OK: %s  Retry-Failures: %s%n",
          variation, commitsOK, retryFailures);

      executor.shutdownNow();

      // 30 seconds is long, but necessary to let transactional databases detect deadlocks, which
      // cause Nessie-commit-retries.
      assertThat(executor.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
    }
  }

  @Test
  void createBranch() throws Exception {
    BranchName create = BranchName.of("createBranch");
    createNamedRef(create, TagName.of(create.getName()));
  }

  @Test
  void createTag() throws Exception {
    TagName create = TagName.of("createTag");
    createNamedRef(create, BranchName.of(create.getName()));
  }

  private void createNamedRef(NamedRef create, NamedRef opposite) throws Exception {
    BranchName branch = BranchName.of("main");

    try (Stream<WithHash<NamedRef>> refs = databaseAdapter.namedRefs()) {
      assertThat(refs.map(WithHash::getValue)).containsExactlyInAnyOrder(branch);
    }

    Hash mainHash = databaseAdapter.toHash(branch);

    assertThatThrownBy(() -> databaseAdapter.toHash(create))
        .isInstanceOf(ReferenceNotFoundException.class);

    Hash createHash = databaseAdapter.create(create, databaseAdapter.toHash(branch));
    assertThat(createHash).isEqualTo(mainHash);

    try (Stream<WithHash<NamedRef>> refs = databaseAdapter.namedRefs()) {
      assertThat(refs.map(WithHash::getValue)).containsExactlyInAnyOrder(branch, create);
    }

    assertThatThrownBy(() -> databaseAdapter.create(create, databaseAdapter.toHash(branch)))
        .isInstanceOf(ReferenceAlreadyExistsException.class);

    assertThat(databaseAdapter.toHash(create)).isEqualTo(createHash);
    assertThatThrownBy(() -> databaseAdapter.toHash(opposite))
        .isInstanceOf(ReferenceNotFoundException.class);

    assertThatThrownBy(
            () ->
                databaseAdapter.create(
                    BranchName.of(create.getName()), databaseAdapter.toHash(branch)))
        .isInstanceOf(ReferenceAlreadyExistsException.class);

    assertThatThrownBy(
            () -> databaseAdapter.delete(create, Optional.of(Hash.of("dead00004242fee18eef"))))
        .isInstanceOf(ReferenceConflictException.class);

    assertThatThrownBy(() -> databaseAdapter.delete(opposite, Optional.of(createHash)))
        .isInstanceOf(ReferenceNotFoundException.class);

    databaseAdapter.delete(create, Optional.of(createHash));

    assertThatThrownBy(() -> databaseAdapter.toHash(create))
        .isInstanceOf(ReferenceNotFoundException.class);

    try (Stream<WithHash<NamedRef>> refs = databaseAdapter.namedRefs()) {
      assertThat(refs.map(WithHash::getValue)).containsExactlyInAnyOrder(branch);
    }
  }

  @Test
  void verifyNotFoundAndConflictExceptionsForUnreachableCommit() throws Exception {
    BranchName main = BranchName.of("main");
    BranchName unreachable = BranchName.of("unreachable");
    BranchName helper = BranchName.of("helper");

    databaseAdapter.create(unreachable, databaseAdapter.toHash(main));
    Hash helperHead = databaseAdapter.create(helper, databaseAdapter.toHash(main));

    Hash unreachableHead =
        databaseAdapter.commit(
            ImmutableCommitAttempt.builder()
                .commitToBranch(unreachable)
                .commitMetaSerialized(ByteString.copyFromUtf8("commit meta"))
                .addPuts(
                    KeyWithBytes.of(
                        Key.of("foo"),
                        ContentsId.of("contentsId"),
                        (byte) 0,
                        ByteString.copyFromUtf8("hello")))
                .build());

    assertAll(
        () ->
            assertThatThrownBy(
                    () -> databaseAdapter.hashOnReference(main, Optional.of(unreachableHead)))
                .isInstanceOf(ReferenceNotFoundException.class)
                .hasMessage(
                    String.format(
                        "Could not find commit '%s' in reference '%s'.",
                        unreachableHead.asString(), main.getName())),
        () ->
            assertThatThrownBy(
                    () ->
                        databaseAdapter.commit(
                            ImmutableCommitAttempt.builder()
                                .commitToBranch(helper)
                                .expectedHead(Optional.of(unreachableHead))
                                .commitMetaSerialized(ByteString.copyFromUtf8("commit meta"))
                                .addPuts(
                                    KeyWithBytes.of(
                                        Key.of("bar"),
                                        ContentsId.of("contentsId-no-no"),
                                        (byte) 0,
                                        ByteString.copyFromUtf8("hello")))
                                .build()))
                .isInstanceOf(ReferenceNotFoundException.class)
                .hasMessage(
                    String.format(
                        "Could not find commit '%s' in reference '%s'.",
                        unreachableHead.asString(), helper.getName())),
        () ->
            assertThatThrownBy(
                    () ->
                        databaseAdapter.assign(
                            helper, Optional.of(unreachableHead), databaseAdapter.toHash(main)))
                .isInstanceOf(ReferenceConflictException.class)
                .hasMessage(
                    String.format(
                        "Named-reference '%s' is not at expected hash '%s', but at '%s'.",
                        helper.getName(), unreachableHead.asString(), helperHead.asString())));
  }

  @Test
  void assign() throws Exception {
    BranchName main = BranchName.of("main");
    TagName tag = TagName.of("tag");
    TagName branch = TagName.of("branch");

    databaseAdapter.create(branch, databaseAdapter.toHash(main));
    databaseAdapter.create(tag, databaseAdapter.toHash(main));

    Hash beginning = databaseAdapter.toHash(main);

    Hash[] commits = new Hash[3];
    for (int i = 0; i < commits.length; i++) {
      commits[i] =
          databaseAdapter.commit(
              ImmutableCommitAttempt.builder()
                  .commitToBranch(main)
                  .commitMetaSerialized(ByteString.copyFromUtf8("commit meta " + i))
                  .addPuts(
                      KeyWithBytes.of(
                          Key.of("bar", Integer.toString(i)),
                          ContentsId.of("contentsId-" + i),
                          (byte) 0,
                          ByteString.copyFromUtf8("hello " + i)))
                  .build());
    }

    Hash expect = beginning;
    for (Hash commit : commits) {
      assertThat(Arrays.asList(databaseAdapter.toHash(branch), databaseAdapter.toHash(tag)))
          .containsExactly(expect, expect);

      databaseAdapter.assign(tag, Optional.of(expect), commit);

      databaseAdapter.assign(branch, Optional.of(expect), commit);

      expect = commit;
    }

    assertThat(Arrays.asList(databaseAdapter.toHash(branch), databaseAdapter.toHash(tag)))
        .containsExactly(commits[commits.length - 1], commits[commits.length - 1]);
  }

  @Test
  void diff() throws Exception {
    BranchName main = BranchName.of("main");
    BranchName branch = BranchName.of("branch");

    Hash initialHash = databaseAdapter.create(branch, databaseAdapter.toHash(main));

    Hash[] commits = new Hash[3];
    for (int i = 0; i < commits.length; i++) {
      ImmutableCommitAttempt.Builder commit =
          ImmutableCommitAttempt.builder()
              .commitToBranch(branch)
              .commitMetaSerialized(ByteString.copyFromUtf8("commit " + i));
      for (int k = 0; k < 3; k++) {
        commit.addPuts(
            KeyWithBytes.of(
                Key.of("key", Integer.toString(k)),
                ContentsId.of("C" + k),
                (byte) 0,
                ByteString.copyFromUtf8("value " + i + " for " + k)));
      }
      commits[i] = databaseAdapter.commit(commit.build());
    }

    try (Stream<Diff<ByteString>> diff =
        databaseAdapter.diff(
            databaseAdapter.toHash(main),
            databaseAdapter.hashOnReference(branch, Optional.of(initialHash)),
            KeyFilterPredicate.ALLOW_ALL)) {
      assertThat(diff).isEmpty();
    }

    for (int i = 0; i < commits.length; i++) {
      try (Stream<Diff<ByteString>> diff =
          databaseAdapter.diff(
              databaseAdapter.toHash(main),
              databaseAdapter.hashOnReference(branch, Optional.of(commits[i])),
              KeyFilterPredicate.ALLOW_ALL)) {
        int c = i;
        assertThat(diff)
            .containsExactlyInAnyOrderElementsOf(
                IntStream.range(0, 3)
                    .mapToObj(
                        k ->
                            Diff.of(
                                Key.of("key", Integer.toString(k)),
                                Optional.empty(),
                                Optional.of(ByteString.copyFromUtf8("value " + c + " for " + k))))
                    .collect(Collectors.toList()));
      }
    }

    for (int i = 0; i < commits.length; i++) {
      try (Stream<Diff<ByteString>> diff =
          databaseAdapter.diff(
              databaseAdapter.hashOnReference(branch, Optional.of(commits[i])),
              databaseAdapter.toHash(main),
              KeyFilterPredicate.ALLOW_ALL)) {
        int c = i;
        assertThat(diff)
            .containsExactlyInAnyOrderElementsOf(
                IntStream.range(0, 3)
                    .mapToObj(
                        k ->
                            Diff.of(
                                Key.of("key", Integer.toString(k)),
                                Optional.of(ByteString.copyFromUtf8("value " + c + " for " + k)),
                                Optional.empty()))
                    .collect(Collectors.toList()));
      }
    }

    for (int i = 1; i < commits.length; i++) {
      try (Stream<Diff<ByteString>> diff =
          databaseAdapter.diff(
              databaseAdapter.hashOnReference(branch, Optional.of(commits[i - 1])),
              databaseAdapter.hashOnReference(branch, Optional.of(commits[i])),
              KeyFilterPredicate.ALLOW_ALL)) {
        int c = i;
        assertThat(diff)
            .containsExactlyInAnyOrderElementsOf(
                IntStream.range(0, 3)
                    .mapToObj(
                        k ->
                            Diff.of(
                                Key.of("key", Integer.toString(k)),
                                Optional.of(
                                    ByteString.copyFromUtf8("value " + (c - 1) + " for " + k)),
                                Optional.of(ByteString.copyFromUtf8("value " + c + " for " + k))))
                    .collect(Collectors.toList()));
      }
    }
  }

  @Test
  void merge() throws Exception {
    mergeTransplant(
        (target, expectedHead, branch, commitHashes, i) ->
            databaseAdapter.merge(commitHashes[i], target, expectedHead));

    BranchName branch = BranchName.of("branch");
    BranchName branch2 = BranchName.of("branch2");
    databaseAdapter.create(branch2, databaseAdapter.toHash(branch));
    assertThatThrownBy(
            () -> databaseAdapter.merge(databaseAdapter.toHash(branch), branch2, Optional.empty()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("No hashes to merge from '");
  }

  @Test
  void transplant() throws Exception {
    Hash[] commits =
        mergeTransplant(
            (target, expectedHead, branch, commitHashes, i) ->
                databaseAdapter.transplant(
                    target, expectedHead, Arrays.asList(commitHashes).subList(0, i + 1)));

    BranchName conflict = BranchName.of("conflict");

    // no conflict, when transplanting the commits from against the current HEAD of the
    // conflict-branch
    Hash noConflictHead = databaseAdapter.toHash(conflict);
    databaseAdapter.transplant(conflict, Optional.of(noConflictHead), Arrays.asList(commits));

    // again, no conflict (same as above, just again)
    databaseAdapter.transplant(conflict, Optional.empty(), Arrays.asList(commits));

    assertThatThrownBy(
            () -> databaseAdapter.transplant(conflict, Optional.empty(), Collections.emptyList()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("No hashes to transplant given.");
  }

  @FunctionalInterface
  interface MergeOrTransplant {
    void apply(
        BranchName target,
        Optional<Hash> expectedHead,
        BranchName branch,
        Hash[] commitHashes,
        int i)
        throws Exception;
  }

  private Hash[] mergeTransplant(MergeOrTransplant mergeOrTransplant) throws Exception {
    BranchName main = BranchName.of("main");
    BranchName branch = BranchName.of("branch");
    BranchName conflict = BranchName.of("conflict");

    databaseAdapter.create(branch, databaseAdapter.toHash(main));

    Hash[] commits = new Hash[3];
    for (int i = 0; i < commits.length; i++) {
      ImmutableCommitAttempt.Builder commit =
          ImmutableCommitAttempt.builder()
              .commitToBranch(branch)
              .commitMetaSerialized(ByteString.copyFromUtf8("commit " + i));
      for (int k = 0; k < 3; k++) {
        commit.addPuts(
            KeyWithBytes.of(
                Key.of("key", Integer.toString(k)),
                ContentsId.of("C" + k),
                (byte) 0,
                ByteString.copyFromUtf8("value " + i + " for " + k)));
      }
      commits[i] = databaseAdapter.commit(commit.build());
    }

    for (int i = 0; i < commits.length; i++) {
      BranchName target = BranchName.of("transplant-" + i);
      databaseAdapter.create(target, databaseAdapter.toHash(main));

      mergeOrTransplant.apply(target, Optional.empty(), branch, commits, i);

      try (Stream<CommitLogEntry> targetLog =
          databaseAdapter.commitLog(databaseAdapter.toHash(target))) {
        assertThat(targetLog).hasSize(i + 1);
      }
    }

    // prepare conflict for keys 0 + 1

    Hash conflictBase = databaseAdapter.create(conflict, databaseAdapter.toHash(main));
    ImmutableCommitAttempt.Builder commit =
        ImmutableCommitAttempt.builder()
            .commitToBranch(conflict)
            .commitMetaSerialized(ByteString.copyFromUtf8("commit conflict"));
    for (int k = 0; k < 2; k++) {
      commit.addPuts(
          KeyWithBytes.of(
              Key.of("key", Integer.toString(k)),
              ContentsId.of("C" + k),
              (byte) 0,
              ByteString.copyFromUtf8("conflict value for " + k)));
    }
    databaseAdapter.commit(commit.build());

    assertThatThrownBy(
            () -> mergeOrTransplant.apply(conflict, Optional.of(conflictBase), branch, commits, 2))
        .isInstanceOf(ReferenceConflictException.class)
        .hasMessage("The following keys have been changed in conflict: 'key.0', 'key.1'");

    return commits;
  }

  @Test
  void recreateDefaultBranch() throws Exception {
    BranchName main = BranchName.of("main");
    Hash mainHead = databaseAdapter.toHash(main);
    databaseAdapter.delete(main, Optional.of(mainHead));

    assertThatThrownBy(() -> databaseAdapter.toHash(main))
        .isInstanceOf(ReferenceNotFoundException.class);

    databaseAdapter.create(main, null);
    databaseAdapter.toHash(main);
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
        // quite slow for a unit-test
        // new ManyKeysParams(20000, 25),
        // new ManyKeysParams(20000, 100),
        new ManyKeysParams(250, 25),
        new ManyKeysParams(1000, 25),
        new ManyKeysParams(1000, 100),
        new ManyKeysParams(5000, 25),
        new ManyKeysParams(5000, 100));
  }

  @ParameterizedTest
  @MethodSource("manyKeysParams")
  void manyKeys(ManyKeysParams params) throws Exception {
    BranchName main = BranchName.of("main");

    List<ImmutableCommitAttempt.Builder> commits =
        IntStream.range(0, params.commits)
            .mapToObj(
                i ->
                    ImmutableCommitAttempt.builder()
                        .commitMetaSerialized(ByteString.copyFromUtf8("commit #" + i))
                        .commitToBranch(main))
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
                  key, ContentsId.of("cid-" + i), (byte) 0, ByteString.copyFromUtf8("value " + i));
            })
        .forEach(kb -> commits.get(commitDist.incrementAndGet() % params.commits).addPuts(kb));

    for (ImmutableCommitAttempt.Builder commit : commits) {
      databaseAdapter.commit(commit.build());
    }

    Hash mainHead = databaseAdapter.toHash(main);
    try (Stream<KeyWithType> keys = databaseAdapter.keys(mainHead, KeyFilterPredicate.ALLOW_ALL)) {
      List<Key> fetchedKeys = keys.map(KeyWithType::getKey).collect(Collectors.toList());

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
}
