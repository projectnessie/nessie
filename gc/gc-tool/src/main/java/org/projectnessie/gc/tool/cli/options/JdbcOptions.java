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
package org.projectnessie.gc.tool.cli.options;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import javax.sql.DataSource;
import org.projectnessie.gc.contents.jdbc.AgroalJdbcDataSourceProvider;
import picocli.CommandLine;

public class JdbcOptions {
  @CommandLine.Option(
      names = "--jdbc",
      description = "Flag whether to use the JDBC contents storage.")
  boolean jdbc;

  @CommandLine.Option(
      names = "--jdbc-url",
      description = "JDBC URL of the database to connect to.",
      required = true)
  String url;

  @CommandLine.Option(
      names = "--jdbc-properties",
      description = "JDBC parameters.",
      arity = "0..*",
      split = ",")
  Map<String, String> properties = new HashMap<>();

  @CommandLine.Option(
      names = "--jdbc-user",
      description = "JDBC user name used to authenticate the database access.")
  String user;

  @CommandLine.Option(
      names = "--jdbc-password",
      description = "JDBC password used to authenticate the database access.")
  String password;

  @CommandLine.Option(
      names = "--jdbc-fetch-size",
      arity = "0..1",
      defaultValue = "100",
      description = "JDBC fetch size, defaults to 100.")
  int fetchSize = 100;

  @CommandLine.Option(
      names = "--jdbc-schema",
      description =
          "How to create the database schema. "
              + "Possible values: CREATE, DROP_AND_CREATE, CREATE_IF_NOT_EXISTS.")
  SchemaCreateStrategy schemaCreateStrategy;

  public DataSource createDataSource() throws SQLException {
    AgroalJdbcDataSourceProvider.Builder jdbcDsBuilder =
        AgroalJdbcDataSourceProvider.builder()
            .jdbcUrl(url)
            .usernamePasswordCredentials(user, password);
    properties.forEach(jdbcDsBuilder::putJdbcProperties);
    AgroalJdbcDataSourceProvider dataSourceProvider = jdbcDsBuilder.build();
    return dataSourceProvider.dataSource();
  }

  public SchemaCreateStrategy getSchemaCreateStrategy() {
    return schemaCreateStrategy;
  }
}
