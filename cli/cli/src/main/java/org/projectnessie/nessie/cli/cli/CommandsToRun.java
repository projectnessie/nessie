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
package org.projectnessie.nessie.cli.cli;

import static org.projectnessie.nessie.cli.cli.NessieCliImpl.OPTION_COMMAND;
import static org.projectnessie.nessie.cli.cli.NessieCliImpl.OPTION_CONTINUE_ON_ERROR;
import static org.projectnessie.nessie.cli.cli.NessieCliImpl.OPTION_KEEP_RUNNING;
import static org.projectnessie.nessie.cli.cli.NessieCliImpl.OPTION_SCRIPT;

import java.util.List;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

class CommandsToRun {

  @ArgGroup CommandsSource commandsSource;

  @Option(
      names = {"-K", OPTION_KEEP_RUNNING},
      description = {
        "When running commands via the "
            + OPTION_COMMAND
            + " or "
            + OPTION_SCRIPT
            + " option the process will exit once the commands have been executed.",
        "To keep the REPL running, specify this option."
            + "See the "
            + OPTION_CONTINUE_ON_ERROR
            + " option."
      })
  boolean keepRunning;

  @Option(
      names = {"-E", OPTION_CONTINUE_ON_ERROR},
      description = {
        "When running commands via the "
            + OPTION_COMMAND
            + " or "
            + OPTION_SCRIPT
            + " option the process will stop/exit when a command could not be parsed or ran into an error.",
        "Specifying this option lets the REPL continue executing the remaining commands after parse or runtime errors."
      })
  boolean continueOnError;

  @Override
  public String toString() {
    return "CommandsToRun{"
        + "commandsSource="
        + commandsSource
        + ", keepRunning="
        + keepRunning
        + ", continueOnError="
        + continueOnError
        + '}';
  }

  static class CommandsSource {
    @Option(
        names = {"-s", OPTION_SCRIPT},
        description = {
          "Run the commands in the Nessie CLI script referenced by this option.",
          "Possible values are either a file path or use the minus character ('-') to read the script from stdin."
        })
    String scriptFile;

    @Option(
        names = {"-c", OPTION_COMMAND},
        arity = "*",
        description = {
          "Nessie CLI commands to run. Each value represents one command.",
          "The process will exit once all specified commands have been executed. "
              + "To keep the REPL running in case of errors, specify the "
              + OPTION_KEEP_RUNNING
              + " option."
        })
    List<String> commands = List.of();

    @Override
    public String toString() {
      return "CommandsSource{" + "runScript='" + scriptFile + '\'' + ", commands=" + commands + '}';
    }
  }
}
