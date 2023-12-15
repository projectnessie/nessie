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
package org.projectnessie.tools.contentgenerator;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.projectnessie.tools.contentgenerator.cli.NessieContentGenerator;

public final class RunContentGenerator {
  private RunContentGenerator() {}

  public static ProcessResult runGeneratorCmd(String... params) {
    return runGeneratorCmd(s -> {}, params);
  }

  public static ProcessResult runGeneratorCmd(Consumer<String> stdoutWatcher, String... params) {
    try (StringWriter stringOut = new StringWriter();
        StringWriter stringErr = new StringWriter();
        PrintWriter out =
            new PrintWriter(stringOut) {
              @Override
              public void write(String s, int off, int len) {
                stdoutWatcher.accept(s.substring(off, off + len));
                super.write(s, off, len);
              }
            };
        PrintWriter err = new PrintWriter(stringErr)) {
      int exitCode = NessieContentGenerator.runMain(out, err, params);
      String output = stringOut.toString();
      String errors = stringErr.toString();
      return new ProcessResult(exitCode, output, errors);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static final class ProcessResult {

    private final int exitCode;
    private final String stdOut;
    private final String stdErr;

    ProcessResult(int exitCode, String stdOut, String stdErr) {
      this.exitCode = exitCode;
      this.stdOut = stdOut;
      this.stdErr = stdErr;
    }

    public int getExitCode() {
      return exitCode;
    }

    public List<String> lines(String out) {
      return Arrays.stream(out.split("\n"))
          .map(s -> s.replaceAll("\\r", ""))
          .collect(Collectors.toList());
    }

    public List<String> getStdOutLines() {
      return lines(stdOut);
    }

    public List<String> getStdErrLines() {
      return lines(stdErr);
    }

    @Override
    public String toString() {
      return "ProcessResult{"
          + "exitCode="
          + exitCode
          + ", stdOut='"
          + stdOut
          + '\''
          + ", stdErr='"
          + stdErr
          + '\''
          + '}';
    }
  }
}
