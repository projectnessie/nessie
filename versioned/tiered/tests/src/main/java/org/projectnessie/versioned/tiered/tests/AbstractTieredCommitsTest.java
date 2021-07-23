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
package org.projectnessie.versioned.tiered.tests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.ContentsAndState;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.Operation;
import org.projectnessie.versioned.Put;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.StoreWorker;
import org.projectnessie.versioned.StringSerializer;
import org.projectnessie.versioned.StringSerializer.TestEnum;
import org.projectnessie.versioned.WithHash;
import org.projectnessie.versioned.tiered.adapter.DatabaseAdapter;
import org.projectnessie.versioned.tiered.adapter.DatabaseAdapterConfig;
import org.projectnessie.versioned.tiered.adapter.DatabaseAdapterFactory;
import org.projectnessie.versioned.tiered.adapter.KeyWithBytes;
import org.projectnessie.versioned.tiered.adapter.SystemPropertiesConfigurer;
import org.projectnessie.versioned.tiered.impl.TieredVersionStore;

public class AbstractTieredCommitsTest<CONFIG extends DatabaseAdapterConfig> {
  protected static DatabaseAdapter databaseAdapter;
  protected TieredVersionStore<String, String, String, TestEnum> versionStore;

  @BeforeEach
  void loadDatabaseAdapter() throws Exception {
    if (databaseAdapter == null) {
      @SuppressWarnings("unchecked")
      DatabaseAdapterFactory<CONFIG> factory =
          ServiceLoader.load(DatabaseAdapterFactory.class).iterator().next();

      databaseAdapter =
          factory
              .newBuilder()
              .configure(SystemPropertiesConfigurer::configureFromSystemProperties)
              .configure(this::configureDatabaseAdapter)
              .build();
    }
    databaseAdapter.reinitializeRepo();

    StoreWorker<String, String, String, TestEnum> storeWorker =
        StoreWorker.of(
            StringSerializer.getInstanceNoSpy(),
            StringSerializer.getInstanceNoSpy(),
            StringSerializer.getInstanceNoSpy(),
            StringSerializer::mergeGlobalState,
            StringSerializer::extractGlobalState);

    versionStore = new TieredVersionStore<>(databaseAdapter, storeWorker);
  }

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

  @Test
  void createBranch() throws Exception {
    BranchName branch = BranchName.of("main");
    BranchName create = BranchName.of("createBranch");

    Hash mainHash = databaseAdapter.toHash(branch);

    assertThatThrownBy(() -> databaseAdapter.toHash(create))
        .isInstanceOf(ReferenceNotFoundException.class);

    Hash createHash = databaseAdapter.create(create, Optional.empty(), Optional.empty());
    assertThat(createHash).isEqualTo(mainHash);

    assertThatThrownBy(() -> databaseAdapter.create(create, Optional.empty(), Optional.empty()))
        .isInstanceOf(ReferenceAlreadyExistsException.class);

    assertThatThrownBy(
            () -> databaseAdapter.delete(create, Optional.of(Hash.of("dead00004242fee18eef"))))
        .isInstanceOf(ReferenceConflictException.class);

    databaseAdapter.delete(create, Optional.of(createHash));
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 19, 20, 21, 39, 40, 41, 49, 50, 51, 255, 256, 257, 1000})
  // Note: 1000 commits is quite the max that in-JVM H2 database can handle
  void manyCommits(int numCommits) throws Exception {
    BranchName branch = BranchName.of("manyCommits-" + numCommits);

    Hash head = databaseAdapter.create(branch, Optional.empty(), Optional.empty());

    for (int i = 0; i < numCommits; i++) {
      String newState = "state for #" + i + " of " + numCommits;
      String value = newState + "|value for #" + i + " of " + numCommits;
      List<Operation<String, String>> ops =
          Collections.singletonList(
              Put.of(
                  Key.of("many", "commits", Integer.toString(numCommits)),
                  value,
                  i > 0 ? "state for #" + (i - 1) + " of " + numCommits : null));
      versionStore.commit(branch, Optional.empty(), "commit #" + i + " of " + numCommits, ops);
    }

    try (Stream<WithHash<String>> s =
        versionStore.getCommits(branch, Optional.empty(), Optional.of(head))) {
      assertThat(s.count()).isEqualTo(numCommits);
    }

    databaseAdapter.delete(branch, Optional.empty());
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 3, 5})
  void commit(int tablesPerCommit) throws Exception {
    BranchName branch = BranchName.of("main");

    ArrayList<Key> keys = new ArrayList<>(tablesPerCommit);
    List<Operation<String, String>> operations = new ArrayList<>(tablesPerCommit);
    for (int i = 0; i < tablesPerCommit; i++) {
      Key key = Key.of("my", "table", "num" + i);
      keys.add(key);

      operations.add(Put.of(key, "0|initial commit contents"));
    }

    Hash head = versionStore.commit(branch, Optional.empty(), "initial commit meta", operations);
    for (int commitNum = 0; commitNum < 3; commitNum++) {
      List<Optional<String>> contents = versionStore.getValues(branch, Optional.empty(), keys);

      operations = new ArrayList<>(tablesPerCommit);
      for (int i = 0; i < tablesPerCommit; i++) {
        String value = contents.get(i).orElseThrow(RuntimeException::new);
        String currentState = value.split("\\|")[0];
        String newGlobalState = Integer.toString(Integer.parseInt(currentState) + 1);
        operations.add(Put.of(keys.get(i), newGlobalState + "|commit value", currentState));
      }

      Hash newHead = versionStore.commit(branch, Optional.empty(), "commit meta data", operations);
      assertThat(newHead).isNotEqualTo(head);
      head = newHead;
    }
  }

  static class Variation {
    final int threads = 4;
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
    try {
      CountDownLatch startLatch = new CountDownLatch(1);
      AtomicBoolean stopFlag = new AtomicBoolean();
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
          keysPerBranch.computeIfAbsent(branch, x -> new HashSet<>()).add(key);
        }

        executor.submit(
            () -> {
              try {
                assertThat(startLatch.await(2, TimeUnit.SECONDS)).isTrue();

                for (int commit = 0; ; commit++) {

                  List<ByteString> currentStates;
                  try (Stream<ByteString> str =
                      databaseAdapter
                          .values(branch, Optional.empty(), keys)
                          .collect(Collectors.toList())
                          .stream()
                          .map(Optional::get)
                          .map(ContentsAndState::getState)) {
                    currentStates = str.collect(Collectors.toList());
                  }

                  List<KeyWithBytes> puts = new ArrayList<>();
                  Map<Key, ByteString> global = new HashMap<>();
                  Map<Key, ByteString> expected = new HashMap<>();
                  for (int ki = 0; ki < keys.size(); ki++) {
                    global.put(
                        keys.get(ki),
                        ByteString.copyFromUtf8(
                            Integer.toString(
                                Integer.parseInt(currentStates.get(ki).toStringUtf8()) + 1)));
                    if (!variation.sharedKeys) {
                      expected.put(keys.get(ki), currentStates.get(ki));
                    }
                    puts.add(KeyWithBytes.of(keys.get(ki), (byte) 0, ByteString.EMPTY));
                  }

                  try {
                    databaseAdapter.commit(
                        branch,
                        Optional.empty(),
                        expected,
                        puts,
                        global,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new HashSet<>(keys),
                        ByteString.copyFromUtf8(
                            "commit #"
                                + commit
                                + " to "
                                + branch.getName()
                                + " something "
                                + ThreadLocalRandom.current().nextLong()));
                    commitsOK.incrementAndGet();
                  } catch (ReferenceConflictException conf) {
                    if (conf.getMessage().startsWith("Retry")) {
                      retryFailures.incrementAndGet();
                    } else {
                      throw conf;
                    }
                  }

                  if (stopFlag.get()) {
                    stopLatch.countDown();
                    break;
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
        databaseAdapter.create(branch, Optional.empty(), Optional.empty());
        databaseAdapter.commit(
            branchKeys.getKey(),
            Optional.empty(),
            Collections.emptyMap(),
            branchKeys.getValue().stream()
                .map(k -> KeyWithBytes.of(k, (byte) 0, ByteString.EMPTY))
                .collect(Collectors.toList()),
            branchKeys.getValue().stream()
                .collect(Collectors.toMap(k -> k, k -> ByteString.copyFromUtf8("0"))),
            Collections.emptyList(),
            Collections.emptyList(),
            branchKeys.getValue(),
            ByteString.copyFromUtf8("initial commit for " + branch.getName()));
      }

      startLatch.countDown();
      Thread.sleep(1_500);
      stopFlag.set(true);
      assertThat(stopLatch.await(2, TimeUnit.SECONDS)).isTrue();

    } finally {
      System.out.printf(
          "AbstractTieredCommitsTest.concurrency - %s : Commits OK: %s  Retry-Failures: %s%n",
          variation, commitsOK, retryFailures);

      executor.shutdown();
      assertThat(executor.awaitTermination(2, TimeUnit.SECONDS)).isTrue();
    }
  }
}
