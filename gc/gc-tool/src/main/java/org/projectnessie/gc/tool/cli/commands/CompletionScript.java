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
package org.projectnessie.gc.tool.cli.commands;

import static java.lang.String.format;

import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import org.projectnessie.gc.tool.cli.options.EnvironmentDefaultProvider;
import picocli.CommandLine;
import picocli.CommandLine.ExecutionException;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Model.CommandSpec;

@CommandLine.Command(
    name = "completion-script",
    mixinStandardHelpOptions = true,
    defaultValueProvider = EnvironmentDefaultProvider.class,
    description = "Extracts the command-line completion script.")
public class CompletionScript implements Callable<Integer> {
  @CommandLine.Option(
      names = {"-O", "--output-file"},
      required = true,
      description = "Completion script file name.")
  Path outputFile;

  @CommandLine.Spec CommandSpec commandSpec;

  @Override
  public Integer call() throws Exception {
    URL scriptUrl =
        CompletionScript.class
            .getClassLoader()
            .getResource("META-INF/completion/nessie-gc-completion");
    if (scriptUrl == null) {
      throw new IllegalStateException("Sorry, no completion script available :(");
    }
    if (Files.exists(outputFile)) {
      throw new ExecutionException(commandSpec.commandLine(), "File already exists.");
    }
    try (InputStream in = scriptUrl.openConnection().getInputStream()) {
      Files.copy(in, outputFile);
    }
    commandSpec
        .commandLine()
        .getOut()
        .println(
            Ansi.AUTO.string(
                format("Completion script written to @|bold,underline %s|@%n", outputFile)));
    return 0;
  }
}
