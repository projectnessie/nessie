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
import org.projectnessie.client.http.HttpClientException;
import org.projectnessie.error.BaseNessieClientServerException;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;

@Command(
    name = "nessie-content-generator",
    mixinStandardHelpOptions = true,
    versionProvider = NessieVersionProvider.class,
    subcommands = {GenerateContent.class, ReadCommits.class, HelpCommand.class})
public class NessieContentGenerator {

  public static void main(String[] arguments) {
    System.exit(runMain(arguments));
  }

  @SuppressWarnings("InstantiationOfUtilityClass")
  @VisibleForTesting
  public static int runMain(String[] arguments) {
    return new CommandLine(new NessieContentGenerator())
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
            })
        .execute(arguments);
  }
}
