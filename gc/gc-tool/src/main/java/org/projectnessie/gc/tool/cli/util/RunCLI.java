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
package org.projectnessie.gc.tool.cli.util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;
import org.projectnessie.gc.tool.cli.CLI;

/** Helper class for tests. */
public final class RunCLI {

  private final String[] args;
  private final int exitCode;
  private final String out;
  private final String err;

  public RunCLI(int exitCode, String out, String err, String[] args) {
    this.args = args;
    this.exitCode = exitCode;
    this.out = out;
    this.err = err;
  }

  public int getExitCode() {
    return exitCode;
  }

  public String getOut() {
    return out;
  }

  public String getErr() {
    return err;
  }

  @Override
  public String toString() {
    return "RunCLI{"
        + "args="
        + Arrays.toString(args)
        + "\nexitCode="
        + exitCode
        + "\n\nstdout:\n"
        + out
        + "\n\nstderr:\n"
        + err;
  }

  public static RunCLI run(List<String> args) throws Exception {
    return run(args.toArray(new String[0]));
  }

  public static RunCLI run(String... args) throws Exception {
    try (StringWriter out = new StringWriter();
        PrintWriter outWriter = new PrintWriter(out);
        StringWriter err = new StringWriter();
        PrintWriter errWriter = new PrintWriter(err)) {
      int exitCode = CLI.runMain(outWriter, errWriter, args);
      return new RunCLI(exitCode, out.toString(), err.toString(), args);
    }
  }
}
