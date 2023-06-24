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
package org.projectnessie.versioned.storage.common.indexes;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.projectnessie.versioned.storage.common.indexes.StoreIndexElement.indexElement;
import static org.projectnessie.versioned.storage.common.indexes.StoreKey.key;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.Action.ADD;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.commitOp;
import static org.projectnessie.versioned.storage.common.persist.ObjId.randomObjId;

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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.projectnessie.versioned.storage.common.objtypes.CommitOp;
import org.projectnessie.versioned.storage.common.objtypes.CommitOp.Action;
import org.projectnessie.versioned.storage.commontests.ImmutableRealisticKeySet;
import org.projectnessie.versioned.storage.commontests.KeyIndexTestSet;
import org.projectnessie.versioned.storage.commontests.KeyIndexTestSet.IndexTestSetGenerator;

/** Benchmark that uses {@link RealisticKeySet} to generate keys. */
@Warmup(iterations = 2, time = 2000, timeUnit = MILLISECONDS)
@Measurement(iterations = 3, time = 1000, timeUnit = MILLISECONDS)
@Fork(1)
@Threads(1) // Do NOT use multiple threads StoreIndex is NOT thread safe!
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(MICROSECONDS)
public class RealisticKeyIndexImplBench {
  @State(Scope.Benchmark)
  public static class BenchmarkParam {

    @Param({"3", "5"})
    public int namespaceLevels;

    @Param({"2", "5"})
    public int foldersPerLevel;

    @Param({"25", "50"})
    public int tablesPerNamespace;

    @Param({"false", "true"})
    public boolean deterministic;

    private KeyIndexTestSet<CommitOp> keyIndexTestSet;

    @Setup
    public void init() {
      IndexTestSetGenerator<CommitOp> builder =
          KeyIndexTestSet.<CommitOp>newGenerator()
              .keySet(
                  ImmutableRealisticKeySet.builder()
                      .namespaceLevels(namespaceLevels)
                      .foldersPerLevel(foldersPerLevel)
                      .tablesPerNamespace(tablesPerNamespace)
                      .deterministic(deterministic)
                      .build())
              .elementSupplier(key -> indexElement(key, commitOp(Action.ADD, 1, randomObjId())))
              .elementSerializer(CommitOp.COMMIT_OP_SERIALIZER)
              .build();

      this.keyIndexTestSet = builder.generateIndexTestSet();

      System.err.printf(
          "%nNumber of tables: %d%nSerialized size: %d%n",
          keyIndexTestSet.keys().size(), keyIndexTestSet.serialized().size());
    }
  }

  @Benchmark
  public Object serializeUnmodifiedIndex(BenchmarkParam param) {
    return param.keyIndexTestSet.serialize();
  }

  @Benchmark
  public Object serializeModifiedIndex(BenchmarkParam param) {
    StoreIndex<CommitOp> deserialized = param.keyIndexTestSet.deserialize();
    return deserialized.serialize();
  }

  @Benchmark
  public Object deserializeAdd(BenchmarkParam param) {
    StoreIndex<CommitOp> deserialized = param.keyIndexTestSet.deserialize();
    for (char c = 'a'; c <= 'z'; c++) {
      deserialized.add(indexElement(key(c + "x", "key"), commitOp(ADD, 1, randomObjId())));
    }
    return deserialized;
  }

  @Benchmark
  public Object deserializeAddSerialize(BenchmarkParam param) {
    StoreIndex<CommitOp> deserialized = param.keyIndexTestSet.deserialize();
    for (char c = 'a'; c <= 'z'; c++) {
      deserialized.add(indexElement(key(c + "x", "key"), commitOp(ADD, 1, randomObjId())));
    }
    return deserialized.serialize();
  }

  @Benchmark
  public Object deserialize(BenchmarkParam param) {
    return param.keyIndexTestSet.deserialize();
  }

  @Benchmark
  public Object deserializeGetRandomKey(BenchmarkParam param) {
    StoreIndex<CommitOp> deserialized = param.keyIndexTestSet.deserialize();
    return deserialized.get(param.keyIndexTestSet.randomKey());
  }
}
