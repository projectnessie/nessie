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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import org.projectnessie.gc.contents.jdbc.JdbcHelper;
import org.projectnessie.gc.tool.cli.options.EnvironmentDefaultProvider;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;

@CommandLine.Command(
    name = "show-sql-create-schema-script",
    mixinStandardHelpOptions = true,
    defaultValueProvider = EnvironmentDefaultProvider.class,
    description = "Print DDL statements to create the schema.")
public class JdbcDumpSchema implements Callable<Integer> {
  @CommandLine.Option(names = "--output-file")
  Path outputFile;

  @CommandLine.Spec CommandSpec commandSpec;

  @Override
  public Integer call() throws Exception {
    if (outputFile != null) {
      try (BufferedWriter writer = Files.newBufferedWriter(outputFile)) {
        dumpStatements(writer);
      }
    } else {
      dumpStatements(commandSpec.commandLine().getOut());
    }
    return 0;
  }

  private void dumpStatements(Writer out) throws IOException {
    for (String statement : JdbcHelper.getCreateTableStatements().values()) {
      out.write(statement);
      out.write(";\n\n");
    }
  }
}
