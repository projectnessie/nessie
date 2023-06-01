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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
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
import org.projectnessie.model.CommitMeta;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.ReferenceCreatedResult;
import org.projectnessie.versioned.ReferenceInfo;

@Warmup(iterations = 2, time = 2000, timeUnit = MILLISECONDS)
@Measurement(iterations = 3, time = 1000, timeUnit = MILLISECONDS)
@Fork(
    value = 1,
    jvmArgs = {"-Xms8g", "-Xmx8g"})
@Threads(1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(MICROSECONDS)
public class ReferencesOpsBench {

  @State(Scope.Benchmark)
  public static class BenchmarkParam extends BaseParams {

    @Param({"10", "100", "1000"})
    public int references;

    @Param({"In-Memory"})
    public String backendName;

    List<ReferenceCreatedResult> refs = new ArrayList<>();

    @Setup
    public void setup() throws Exception {
      super.init(backendName);

      for (int i = 0; i < references; i++) {
        BranchName branchName = BranchName.of("branch-" + i);
        ReferenceCreatedResult ref = versionStore.create(branchName, Optional.empty());
        refs.add(ref);
      }
    }

    @Override
    @TearDown
    public void tearDown() throws Exception {
      super.tearDown();
    }

    public ReferenceCreatedResult randomRef() {
      return refs.get(ThreadLocalRandom.current().nextInt(references));
    }
  }

  @Benchmark
  public ReferenceInfo<CommitMeta> getNamedRefDefault(BenchmarkParam param) throws Exception {
    ReferenceCreatedResult ref = param.randomRef();
    return param.versionStore.getNamedRef(ref.getNamedRef().getName(), GetNamedRefsParams.DEFAULT);
  }
}
