/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.services;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.projectnessie.model.CommitMeta.fromMessage;
import static org.projectnessie.versioned.VersionStore.KeyRestrictions.NO_KEY_RESTRICTIONS;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
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
import org.openjdk.jmh.infra.Blackhole;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.ContentResult;
import org.projectnessie.versioned.KeyEntry;
import org.projectnessie.versioned.ReferenceCreatedResult;
import org.projectnessie.versioned.paging.PaginationIterator;

@Warmup(iterations = 2, time = 2000, timeUnit = MILLISECONDS)
@Measurement(iterations = 3, time = 1000, timeUnit = MILLISECONDS)
@Fork(
    value = 1,
    jvmArgs = {"-Xms8g", "-Xmx8g"})
@Threads(1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(MICROSECONDS)
public class ContentOpsBench {

  @State(Scope.Benchmark)
  public static class BenchmarkParam extends BaseParams {

    @Param({"100", "1000", "10000"})
    public int contents;

    @Param({"In-Memory"})
    public String backendName;

    ReferenceCreatedResult ref;
    List<ContentKey> keys = new ArrayList<>();

    @Setup
    public void setup() throws Exception {
      super.init(backendName);

      Namespace ns = Namespace.of("my-namespace");

      BranchName branchName = BranchName.of("branch");
      ref = versionStore.create(branchName, Optional.empty());

      versionStore.commit(
          branchName,
          Optional.empty(),
          fromMessage("initial"),
          Collections.singletonList(Put.of(ns.toContentKey(), ns)));
      List<Operation> commitOps = new ArrayList<>();
      for (int j = 0; j < contents; j++) {
        ContentKey key = ContentKey.of(ns, "table-" + j);
        keys.add(key);
        commitOps.add(Put.of(key, IcebergTable.of("meta-" + j, j, j, j, j)));
        if (commitOps.size() == 500) {
          versionStore.commit(branchName, Optional.empty(), fromMessage("x"), commitOps);
          commitOps.clear();
        }
      }
      if (!commitOps.isEmpty()) {
        versionStore.commit(branchName, Optional.empty(), fromMessage("x"), commitOps);
      }
    }

    @Override
    @TearDown
    public void tearDown() throws Exception {
      super.tearDown();
    }

    public ContentKey randomKey() {
      return keys.get(ThreadLocalRandom.current().nextInt(contents));
    }
  }

  @Benchmark
  public void getKeys(BenchmarkParam param, Blackhole bh) throws Exception {
    PaginationIterator<KeyEntry> iter =
        param.versionStore.getKeys(param.ref.getNamedRef(), null, false, NO_KEY_RESTRICTIONS);
    while (iter.hasNext()) {
      bh.consume(iter.next());
    }
  }

  @Benchmark
  public ContentResult getValue(BenchmarkParam param) throws Exception {
    ContentKey key = param.randomKey();
    return param.versionStore.getValue(param.ref.getNamedRef(), key, false);
  }

  @Benchmark
  public Map<ContentKey, ContentResult> getTenValues(BenchmarkParam param) throws Exception {
    Set<ContentKey> k = new HashSet<>();
    while (k.size() < 10) {
      k.add(param.randomKey());
    }
    return param.versionStore.getValues(param.ref.getNamedRef(), k, false);
  }
}
