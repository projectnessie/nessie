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
package org.projectnessie.gc.tool.cli;

import com.google.common.annotations.VisibleForTesting;
import java.io.PrintWriter;
import org.projectnessie.client.http.HttpClientException;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.gc.expire.PerContentDeleteExpired;
import org.projectnessie.gc.identify.IdentifyLiveContents;
import org.projectnessie.gc.tool.cli.commands.CompletionScript;
import org.projectnessie.gc.tool.cli.commands.DeferredDeleteFiles;
import org.projectnessie.gc.tool.cli.commands.DeleteLiveSets;
import org.projectnessie.gc.tool.cli.commands.JdbcCreateSchema;
import org.projectnessie.gc.tool.cli.commands.JdbcDumpSchema;
import org.projectnessie.gc.tool.cli.commands.ListDeferredDeletions;
import org.projectnessie.gc.tool.cli.commands.ListLiveSets;
import org.projectnessie.gc.tool.cli.commands.MarkAndSweep;
import org.projectnessie.gc.tool.cli.commands.MarkLive;
import org.projectnessie.gc.tool.cli.commands.ShowLiveSet;
import org.projectnessie.gc.tool.cli.commands.Sweep;
import org.projectnessie.gc.tool.cli.commands.ThirdPartyLicenses;
import picocli.CommandLine;
import picocli.CommandLine.HelpCommand;

/**
 * Standalone tool to perform a full {@link IdentifyLiveContents mark}-and-{@link
 * PerContentDeleteExpired sweep} Nessie GC cycle.
 */
@CommandLine.Command(
    name = "nessie-gc.jar",
    mixinStandardHelpOptions = true,
    versionProvider = NessieVersionProvider.class,
    subcommands = {
      HelpCommand.class,
      MarkLive.class,
      Sweep.class,
      MarkAndSweep.class,
      ListLiveSets.class,
      DeleteLiveSets.class,
      ListDeferredDeletions.class,
      DeferredDeleteFiles.class,
      ShowLiveSet.class,
      JdbcDumpSchema.class,
      JdbcCreateSchema.class,
      CompletionScript.class,
      ThirdPartyLicenses.class
    })
public class CLI {

  public static void main(String... arguments) {

    // There's no easy, better way :(
    // Setting the usage-width to 100 chars so that URLs are not line-wrapped.
    System.setProperty("picocli.usage.width", "100");

    System.exit(runMain(arguments));
  }

  @VisibleForTesting
  public static int runMain(String... arguments) {
    return runMain(null, null, arguments);
  }

  @VisibleForTesting
  public static int runMain(PrintWriter out, PrintWriter err, String... arguments) {
    @SuppressWarnings("InstantiationOfUtilityClass")
    CommandLine commandLine =
        new CommandLine(new CLI())
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
}
