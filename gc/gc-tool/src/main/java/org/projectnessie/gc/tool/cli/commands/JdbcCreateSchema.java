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

import java.sql.Connection;
import javax.sql.DataSource;
import org.projectnessie.gc.tool.cli.Closeables;
import org.projectnessie.gc.tool.cli.options.EnvironmentDefaultProvider;
import org.projectnessie.gc.tool.cli.options.JdbcOptions;
import org.projectnessie.gc.tool.cli.options.SchemaCreateStrategy;
import picocli.CommandLine;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Model.CommandSpec;

@CommandLine.Command(
    name = "create-sql-schema",
    mixinStandardHelpOptions = true,
    defaultValueProvider = EnvironmentDefaultProvider.class,
    description = "JDBC schema creation.")
public class JdbcCreateSchema extends BaseCommand {
  @CommandLine.ArgGroup(multiplicity = "1", exclusive = false)
  JdbcOptions jdbc;

  @CommandLine.Spec CommandSpec commandSpec;

  @Override
  protected Integer call(Closeables closeables) throws Exception {
    DataSource dataSource = closeables.maybeAdd(jdbc.createDataSource());
    SchemaCreateStrategy schemaCreateStrategy = jdbc.getSchemaCreateStrategy();
    if (schemaCreateStrategy == null) {
      schemaCreateStrategy = SchemaCreateStrategy.CREATE;
    }
    try (Connection conn = dataSource.getConnection()) {
      schemaCreateStrategy.apply(conn);
      // by default autocommit is set to false, so we commit here
      conn.commit();
    }
    commandSpec
        .commandLine()
        .getOut()
        .println(
            Ansi.AUTO.text(
                "@|bold,green Nessie GC tables for live-content-set storage created.|@"));
    return 0;
  }
}
