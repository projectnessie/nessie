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
package org.projectnessie.versioned.persist.benchmarks;

import static java.util.Collections.emptyList;
import static org.projectnessie.versioned.testworker.OnRefOnly.onRef;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
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
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.ContentResult;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Operation;
import org.projectnessie.versioned.Put;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceRetryFailureException;
import org.projectnessie.versioned.persist.adapter.AdjustableDatabaseAdapterConfig;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapterConfig;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapterFactory;
import org.projectnessie.versioned.persist.adapter.DatabaseConnectionConfig;
import org.projectnessie.versioned.persist.adapter.DatabaseConnectionProvider;
import org.projectnessie.versioned.persist.store.PersistVersionStore;
import org.projectnessie.versioned.persist.tests.SystemPropertiesConfigurer;
import org.projectnessie.versioned.persist.tests.extension.TestConnectionProviderSource;

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

    @Param({"H2:h2", "In-Memory", "RocksDB"})
    public String adapter;

    final AtomicInteger retryFailures = new AtomicInteger();
    final AtomicInteger conflictsFailures = new AtomicInteger();
    final AtomicInteger success = new AtomicInteger();
    TestConnectionProviderSource<DatabaseConnectionConfig> providerSource;
    DatabaseAdapter databaseAdapter;
    PersistVersionStore versionStore;
    List<ContentKey> keys;
    Map<ContentKey, String> contentIds;
    BranchName branch = BranchName.of("main");

    @Setup
    public void init() throws Exception {
      databaseAdapter = adapterByName();

      databaseAdapter.eraseRepo();
      databaseAdapter.initializeRepo(branch.getName());

      versionStore = new PersistVersionStore(databaseAdapter);

      keys = new ArrayList<>(tablesPerCommit);

      for (int i = 0; i < tablesPerCommit; i++) {
        ContentKey key = ContentKey.of("my", "table", "num" + i);
        keys.add(key);
      }

      contentIds =
          keys.stream().collect(Collectors.toMap(k -> k, k -> UUID.randomUUID().toString()));

      versionStore.commit(
          branch,
          Optional.empty(),
          CommitMeta.fromMessage("initial commit meta"),
          initialOperations(this, keys, contentIds));
    }

    private DatabaseAdapter adapterByName() {
      String adapterName =
          (adapter.indexOf(':') <= 0) ? adapter : adapter.substring(0, adapter.indexOf(':'));
      DatabaseAdapterFactory<
              ? extends DatabaseAdapter,
              ? extends DatabaseAdapterConfig,
              ? extends AdjustableDatabaseAdapterConfig,
              DatabaseConnectionProvider<DatabaseConnectionConfig>>
          factory =
              DatabaseAdapterFactory.loadFactory(f -> f.getName().equalsIgnoreCase(adapterName));

      DatabaseAdapterFactory.Builder<
              ? extends DatabaseAdapter,
              ? extends DatabaseAdapterConfig,
              ? extends AdjustableDatabaseAdapterConfig,
              DatabaseConnectionProvider<DatabaseConnectionConfig>>
          builder =
              factory
                  .newBuilder()
                  .configure(SystemPropertiesConfigurer::configureAdapterFromSystemProperties);

      String providerSpec =
          adapter.indexOf(':') == -1
              ? null
              : adapter.substring(adapter.indexOf(':') + 1).toLowerCase(Locale.ROOT);
      providerSource =
          TestConnectionProviderSource.findCompatibleProviderSource(
              builder.getConfig(), factory, providerSpec);
      providerSource.configureConnectionProviderConfigFromDefaults(
          SystemPropertiesConfigurer::configureConnectionFromSystemProperties);
      try {
        providerSource.start();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

      return builder.withConnector(providerSource.getConnectionProvider()).build();
    }

    @TearDown
    public void close() throws Exception {
      int retries = retryFailures.get();
      int conflicts = conflictsFailures.get();
      int successes = success.get();
      int total = retries + conflicts + successes;
      double retryRate = retries;
      retryRate /= total;
      retryRate *= 100;
      double conflictRate = conflicts;
      conflictRate /= total;
      conflictRate *= 100;
      double successRate = successes;
      successRate /= total;
      successRate *= 100;
      System.out.printf(
          "(%.02f%% retries (%d), %.02f%% conflicts (%d), %.02f%% success (%d)) ",
          retryRate, retries, conflictRate, conflicts, successRate, successes);
      providerSource.stop();
    }
  }

  @State(Scope.Thread)
  public static class ThreadParam {
    BranchName branch;
    List<ContentKey> keys;
    Map<ContentKey, String> contentIds;

    @Setup
    @SuppressWarnings("deprecation") // Thread.getId() deprecated since 19
    public void createBranch(BenchmarkParam bp) throws Exception {
      branch = BranchName.of("thread-" + Thread.currentThread().getId());

      keys = new ArrayList<>(bp.tablesPerCommit);
      for (int i = 0; i < bp.tablesPerCommit; i++) {
        ContentKey key =
            ContentKey.of("per-thread", Long.toString(Thread.currentThread().getId()), "num" + i);
        keys.add(key);
      }

      contentIds = new HashMap<>(bp.contentIds);
      keys.forEach(k -> contentIds.put(k, UUID.randomUUID().toString()));

      bp.versionStore.commit(
          bp.branch,
          Optional.empty(),
          CommitMeta.fromMessage("initial commit meta " + Thread.currentThread().getId()),
          initialOperations(bp, keys, contentIds));

      Hash hash = bp.versionStore.hashOnReference(bp.branch, Optional.empty(), emptyList());

      bp.versionStore.create(branch, Optional.of(hash));
    }
  }

  @Benchmark
  public void singleBranchSharedKeys(BenchmarkParam bp) throws Exception {
    doCommit(bp, bp.branch, bp.keys, bp.contentIds);
  }

  @Benchmark
  public void branchPerThreadSharedKeys(BenchmarkParam bp, ThreadParam tp) throws Exception {
    doCommit(bp, tp.branch, bp.keys, bp.contentIds);
  }

  @Benchmark
  public void singleBranchUnsharedKeys(BenchmarkParam bp, ThreadParam tp) throws Exception {
    doCommit(bp, bp.branch, tp.keys, tp.contentIds);
  }

  @Benchmark
  public void branchPerThreadUnsharedKeys(BenchmarkParam bp, ThreadParam tp) throws Exception {
    doCommit(bp, tp.branch, tp.keys, tp.contentIds);
  }

  private void doCommit(
      BenchmarkParam bp,
      BranchName branch,
      List<ContentKey> keys,
      Map<ContentKey, String> contentIds)
      throws Exception {
    Map<ContentKey, ContentResult> contentByKey = bp.versionStore.getValues(branch, keys);

    try {
      List<Operation> operations = new ArrayList<>(bp.tablesPerCommit);
      for (int i = 0; i < bp.tablesPerCommit; i++) {
        ContentKey key = keys.get(i);
        ContentResult value = contentByKey.get(key);
        if (value == null) {
          throw new RuntimeException("no value for key " + key + " in " + branch);
        }
        String contentId = contentIds.get(key);
        operations.add(
            Put.of(
                key,
                // Must add randomness here, otherwise concurrent threads will compute the same
                // hashes, because parent, "content", key are all the same.
                onRef("commit value " + ThreadLocalRandom.current().nextLong(), contentId)));
      }

      bp.versionStore.commit(
          branch, Optional.empty(), CommitMeta.fromMessage("commit meta data"), operations);

      bp.success.incrementAndGet();
    } catch (ReferenceRetryFailureException e) {
      bp.retryFailures.incrementAndGet();
    } catch (ReferenceConflictException e) {
      bp.conflictsFailures.incrementAndGet();
    }
  }

  static List<Operation> initialOperations(
      BenchmarkParam bp, List<ContentKey> keys, Map<ContentKey, String> contentIds) {
    List<Operation> operations = new ArrayList<>(bp.tablesPerCommit);
    for (ContentKey key : keys) {
      String contentId = contentIds.get(key);
      operations.add(Put.of(key, onRef("initial commit content", contentId)));
    }
    return operations;
  }
}
