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
package org.projectnessie.versioned.tiered.benchmarks;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.Operation;
import org.projectnessie.versioned.Put;
import org.projectnessie.versioned.ReferenceRetryFailureException;
import org.projectnessie.versioned.StoreWorker;
import org.projectnessie.versioned.StringSerializer;
import org.projectnessie.versioned.StringSerializer.TestEnum;
import org.projectnessie.versioned.tiered.adapter.DatabaseAdapter;
import org.projectnessie.versioned.tiered.adapter.DatabaseAdapterConfig;
import org.projectnessie.versioned.tiered.adapter.DatabaseAdapterFactory;
import org.projectnessie.versioned.tiered.adapter.SystemPropertiesConfigurer;
import org.projectnessie.versioned.tiered.impl.TieredVersionStore;

@Warmup(iterations = 2, time = 2000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 3, time = 5000, timeUnit = TimeUnit.MILLISECONDS)
@Fork(1)
@Threads(4)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class CommitBench {

  @State(Scope.Benchmark)
  public static class BenchmarkParam {

    @Param({"1", "3", "5"})
    public int tablesPerCommit;

    @Param({"H2", "HSQL", "In-Memory", "RocksDB"})
    public String adapter;

    final AtomicInteger retryFailures = new AtomicInteger();
    DatabaseAdapter databaseAdapter;
    TieredVersionStore<String, String, String, TestEnum> versionStore;
    List<Key> keys;
    BranchName branch = BranchName.of("main");

    @Setup
    public void init() throws Exception {
      databaseAdapter = adapterByName();

      databaseAdapter.reinitializeRepo();

      StoreWorker<String, String, String, TestEnum> storeWorker =
          StoreWorker.of(
              StringSerializer.getInstanceNoSpy(),
              StringSerializer.getInstanceNoSpy(),
              StringSerializer.getInstanceNoSpy(),
              StringSerializer::mergeGlobalState,
              StringSerializer::extractGlobalState);

      versionStore = new TieredVersionStore<>(databaseAdapter, storeWorker);

      keys = new ArrayList<>(tablesPerCommit);
      for (int i = 0; i < tablesPerCommit; i++) {
        Key key = Key.of("my", "table", "num" + i);
        keys.add(key);
      }

      versionStore.commit(
          branch, Optional.empty(), "initial commit meta", initialOperations(this, keys));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private DatabaseAdapter adapterByName() {
      DatabaseAdapterFactory<DatabaseAdapterConfig> factory = null;
      for (DatabaseAdapterFactory f : ServiceLoader.load(DatabaseAdapterFactory.class)) {
        if (f.getName().equalsIgnoreCase(adapter)) {
          factory = f;
          break;
        }
      }
      if (factory == null) {
        throw new IllegalArgumentException(
            String.format("No database-adapter named '%s' found.", adapter));
      }
      return factory
          .newBuilder()
          .configure(SystemPropertiesConfigurer::configureFromSystemProperties)
          .build();
    }

    @TearDown
    public void close() throws Exception {
      if (retryFailures.get() > 0) {
        System.out.printf("(Retry-failures over all iterations: %d) ", retryFailures.get());
      }
      databaseAdapter.close();
    }
  }

  @State(Scope.Thread)
  public static class ThreadParam {
    BranchName branch;
    List<Key> keys;

    @Setup
    public void createBranch(BenchmarkParam bp) throws Exception {
      branch = BranchName.of("thread-" + Thread.currentThread().getId());

      keys = new ArrayList<>(bp.tablesPerCommit);
      for (int i = 0; i < bp.tablesPerCommit; i++) {
        Key key = Key.of("per-thread", Long.toString(Thread.currentThread().getId()), "num" + i);
        keys.add(key);
      }

      bp.versionStore.commit(
          bp.branch,
          Optional.empty(),
          "initial commit meta " + Thread.currentThread().getId(),
          initialOperations(bp, keys));

      bp.versionStore.create(branch, Optional.empty(), Optional.empty());
      bp.versionStore.commit(
          branch,
          Optional.empty(),
          "initial commit meta " + Thread.currentThread().getId(),
          initialOperations(
              bp, Stream.concat(keys.stream(), bp.keys.stream()).collect(Collectors.toList())));
    }
  }

  @Benchmark
  public void singleBranchSharedKeys(BenchmarkParam bp) throws Exception {
    doCommit(bp, bp.branch, bp.keys, false);
  }

  @Benchmark
  public void branchPerThreadSharedKeys(BenchmarkParam bp, ThreadParam tp) throws Exception {
    doCommit(bp, tp.branch, bp.keys, false);
  }

  @Benchmark
  public void singleBranchUnsharedKeys(BenchmarkParam bp, ThreadParam tp) throws Exception {
    doCommit(bp, bp.branch, tp.keys, true);
  }

  @Benchmark
  public void branchPerThreadUnsharedKeys(BenchmarkParam bp, ThreadParam tp) throws Exception {
    doCommit(bp, tp.branch, tp.keys, true);
  }

  private void doCommit(BenchmarkParam bp, BranchName branch, List<Key> keys, boolean expectState)
      throws Exception {
    List<Optional<String>> contents = bp.versionStore.getValues(branch, Optional.empty(), keys);

    try {
      List<Operation<String, String>> operations = new ArrayList<>(bp.tablesPerCommit);
      for (int i = 0; i < bp.tablesPerCommit; i++) {
        Key key = keys.get(i);
        String value =
            contents
                .get(i)
                .orElseThrow(
                    () -> new RuntimeException("no value for key " + key + " " + " in " + branch));
        String currentState = value.split("\\|")[0];
        String newGlobalState = Integer.toString(Integer.parseInt(currentState) + 1);
        operations.add(
            Put.of(
                key,
                // Must add randomness here, otherwise concurrent threads will compute the same
                // hashes, because parent, "contents", key are a all the same.
                newGlobalState + "|commit value " + ThreadLocalRandom.current().nextLong(),
                // Hashes for global-state have non-deterministic keys, so having these kind of
                // otherwise "collision-prone" state values is fine.
                // TODO verifying the global-state won't work, because this benchmark shall just
                //  measure the commit, but NOT client-side retries necessary due to concurrent
                //  changes to "our tables".
                expectState ? currentState : null));
      }

      bp.versionStore.commit(branch, Optional.empty(), "commit meta data", operations);
    } catch (ReferenceRetryFailureException e) {
      bp.retryFailures.incrementAndGet();
    }
  }

  static List<Operation<String, String>> initialOperations(BenchmarkParam bp, List<Key> keys) {
    List<Operation<String, String>> operations = new ArrayList<>(bp.tablesPerCommit);
    for (Key key : keys) {
      operations.add(Put.of(key, "0|initial commit contents"));
    }
    return operations;
  }
}
