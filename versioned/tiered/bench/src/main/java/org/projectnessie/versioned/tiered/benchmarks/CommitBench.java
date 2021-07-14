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
import org.projectnessie.versioned.StoreWorker;
import org.projectnessie.versioned.StringSerializer;
import org.projectnessie.versioned.StringSerializer.TestEnum;
import org.projectnessie.versioned.tiered.adapter.DatabaseAdapter;
import org.projectnessie.versioned.tiered.adapter.DatabaseAdapterFactory;
import org.projectnessie.versioned.tiered.adapter.DatabaseAdapterFactory.Builder;
import org.projectnessie.versioned.tiered.impl.TieredVersionStore;

@Warmup(iterations = 2, time = 2000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 3, time = 5000, timeUnit = TimeUnit.MILLISECONDS)
@Fork(1)
@Threads(4)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class CommitBench {

  @State(Scope.Benchmark)
  public static class Adapter {

    @Param({"1", "3", "5"})
    public int tablesPerCommit;

    DatabaseAdapter databaseAdapter;
    TieredVersionStore<String, String, String, TestEnum> versionStore;
    List<Key> keys;
    BranchName branch = BranchName.of("main");

    @SuppressWarnings("unchecked")
    @Setup
    public void init() throws Exception {
      DatabaseAdapterFactory factory =
          ServiceLoader.load(DatabaseAdapterFactory.class).iterator().next();

      StoreWorker<String, String, String, TestEnum> storeWorker =
          StoreWorker.of(
              StringSerializer.getInstanceNoSpy(),
              StringSerializer.getInstanceNoSpy(),
              StringSerializer.getInstanceNoSpy(),
              (value, state) -> state + '|' + value.substring(value.indexOf('|') + 1));

      Builder builder = factory.newBuilder();

      databaseAdapter = builder.build();
      databaseAdapter.initializeRepo();

      versionStore = new TieredVersionStore<>(databaseAdapter, storeWorker);

      keys = new ArrayList<>(tablesPerCommit);
      List<Operation<String, String>> operations = new ArrayList<>(tablesPerCommit);
      for (int i = 0; i < tablesPerCommit; i++) {
        Key key = Key.of("my", "table", "num" + i);
        keys.add(key);
        operations.add(Put.of(key, "0|initial commit contents", "0", null));
      }

      versionStore.commit(branch, Optional.empty(), "initial commit meta", operations);
    }

    @TearDown
    public void close() throws Exception {
      databaseAdapter.close();
    }
  }

  @Benchmark
  public void commit(Adapter adapter) throws Exception {

    List<Optional<String>> contents = adapter.versionStore.getValues(adapter.branch, adapter.keys);

    List<Operation<String, String>> operations = new ArrayList<>(adapter.tablesPerCommit);
    for (int i = 0; i < adapter.tablesPerCommit; i++) {
      String value = contents.get(i).orElseThrow(RuntimeException::new);
      String currentState = value.split("\\|")[0];
      operations.add(
          Put.of(
              adapter.keys.get(i),
              // Must add randomness here, otherwise concurrent threads will compute the same
              // hashes, because parent, "contents", key are a all the same.
              "commit value " + ThreadLocalRandom.current().nextLong(),
              // Hashes for global-state have non-deterministic keys, so having these kind of
              // otherwise "collision-prone" state values is fine.
              Integer.toString(Integer.parseInt(currentState) + 1),
              // TODO verifying the global-state won't work, because this benchmark shall just
              //  measure the commit, but NOT client-side retries necessary due to concurrent
              //  changes to "our tables".
              null)); // expectedState.getState()));
    }

    adapter.versionStore.commit(adapter.branch, Optional.empty(), "commit meta data", operations);
  }
}
