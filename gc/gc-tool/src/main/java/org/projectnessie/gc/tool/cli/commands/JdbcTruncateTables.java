/*
 * Copyright (C) 2025 Dremio
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

import static java.lang.System.out;

import java.sql.Connection;
import javax.sql.DataSource;
import org.projectnessie.gc.contents.jdbc.JdbcHelper;
import org.projectnessie.gc.tool.cli.Closeables;
import org.projectnessie.gc.tool.cli.options.EnvironmentDefaultProvider;
import org.projectnessie.gc.tool.cli.options.JdbcOptions;
import picocli.CommandLine;

@CommandLine.Command(
    name = "truncate",
    aliases = {"clean-tables"},
    mixinStandardHelpOptions = true,
    defaultValueProvider = EnvironmentDefaultProvider.class,
    description =
        "Truncate nessie-gc tables to avoid increasing storage of DB, "
            + "must not be used with the in-memory contents-storage.")
public class JdbcTruncateTables extends BaseCommand {
  @CommandLine.ArgGroup(multiplicity = "1", exclusive = false)
  JdbcOptions jdbc;

  @Override
  protected Integer call(Closeables closeables) throws Exception {
    DataSource dataSource = closeables.maybeAdd(jdbc.createDataSource());
    int truncateTableCount;
    try (Connection conn = dataSource.getConnection()) {
      truncateTableCount = JdbcHelper.truncateTable(conn);
    }
    out.printf("number of `%s` tables truncated", truncateTableCount);
    return truncateTableCount > 0 ? 0 : 1;
  }
}
