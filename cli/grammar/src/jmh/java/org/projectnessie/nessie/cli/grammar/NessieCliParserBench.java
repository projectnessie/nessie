/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.nessie.cli.grammar;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

@Warmup(iterations = 2, time = 2000, timeUnit = MILLISECONDS)
@Measurement(iterations = 3, time = 1000, timeUnit = MILLISECONDS)
@Fork(1)
@Threads(4)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(MICROSECONDS)
public class NessieCliParserBench {

  @State(Scope.Benchmark)
  public static class Args {
    @Param({
      "CREATE branch if not exists foo;",
      "DROP TAG main;",
      "CONNECT TO http://127.0.0.1:19120/api/v2",
      "CONNECT TO http://127.0.0.1:19120/api/v2 USING a=b",
      "CONNECT TO http://127.0.0.1:19120/api/v2 USING a=b AND c=d",
      "CONNECT TO http://127.0.0.1:19120/api/v2 USING a=b AND c=d"
          + " AND \"foo.bar.foo.bar.foo.bar.foo.bar.foo.bar\"=\"foo.bar.foo.bar.foo.bar.foo.bar.foo.bar\""
          + " AND \"foo.bar.foo.bar.foo.bar.foo.bar.foo.bar\"=\"foo.bar.foo.bar.foo.bar.foo.bar.foo.bar\""
          + " AND \"foo.bar.foo.bar.foo.bar.foo.bar.foo.bar\"=\"foo.bar.foo.bar.foo.bar.foo.bar.foo.bar\""
          + " AND \"foo.bar.foo.bar.foo.bar.foo.bar.foo.bar\"=\"foo.bar.foo.bar.foo.bar.foo.bar.foo.bar\""
          + " AND \"foo.bar.foo.bar.foo.bar.foo.bar.foo.bar\"=\"foo.bar.foo.bar.foo.bar.foo.bar.foo.bar\""
          + " AND \"foo.bar.foo.bar.foo.bar.foo.bar.foo.bar\"=\"foo.bar.foo.bar.foo.bar.foo.bar.foo.bar\"",
    })
    String input;
  }

  @Benchmark
  public Node singleStatement(Args params) {
    NessieCliLexer lexer = new NessieCliLexer(params.input);
    NessieCliParser parser = new NessieCliParser(lexer);
    parser.SingleStatement();
    return parser.rootNode();
  }
}
