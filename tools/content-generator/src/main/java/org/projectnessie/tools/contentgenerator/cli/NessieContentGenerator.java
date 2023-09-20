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
package org.projectnessie.tools.contentgenerator.cli;

import com.google.common.annotations.VisibleForTesting;
import java.io.PrintWriter;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.client.http.HttpClientException;
import org.projectnessie.error.BaseNessieClientServerException;
import picocli.CommandLine;

public class NessieContentGenerator extends ContentGenerator<NessieApiV2> {

  public static void main(String[] arguments) {
    System.exit(runMain(arguments));
  }

  @VisibleForTesting
  public static int runMain(String[] arguments) {
    return runMain(null, arguments);
  }

  @VisibleForTesting
  public static int runMain(PrintWriter out, String[] arguments) {
    return runMain(out, null, arguments);
  }

  @VisibleForTesting
  public static int runMain(PrintWriter out, PrintWriter err, String[] arguments) {
    NessieContentGenerator command = new NessieContentGenerator();
    return runMain(command, out, err, arguments);
  }

  @VisibleForTesting
  public static int runMain(
      NessieContentGenerator command, PrintWriter out, PrintWriter err, String[] arguments) {
    CommandLine commandLine =
        new CommandLine(command)
            .setExecutionStrategy(command::execute)
            .setExecutionExceptionHandler(
                (ex, cmd, parseResult) -> {
                  if (ex instanceof BaseNessieClientServerException
                      || ex instanceof HttpClientException) {
                    // Just print the exception w/o the stack trace for Nessie related exceptions
                    cmd.getErr().println(cmd.getColorScheme().errorText(ex.toString()));
                  } else {
                    // Print the full stack trace in all other cases.
                    cmd.getErr().println(cmd.getColorScheme().richStackTraceString(ex));
                  }
                  return cmd.getExitCodeExceptionMapper() != null
                      ? cmd.getExitCodeExceptionMapper().getExitCode(ex)
                      : cmd.getCommandSpec().exitCodeOnExecutionException();
                });
    if (null != out) {
      commandLine = commandLine.setOut(out);
    }
    if (null != err) {
      commandLine = commandLine.setErr(err);
    }
    try {
      return commandLine.execute(arguments);
    } finally {
      commandLine.getOut().flush();
      commandLine.getErr().flush();
    }
  }

  protected int execute(CommandLine.ParseResult parseResult) {
    return new CommandLine.RunLast().execute(parseResult);
  }
}
