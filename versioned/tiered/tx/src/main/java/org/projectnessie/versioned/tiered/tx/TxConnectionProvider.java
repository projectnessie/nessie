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
package org.projectnessie.versioned.tiered.tx;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.AgroalConnectionFactoryConfiguration.TransactionIsolation;
import io.agroal.api.configuration.supplier.AgroalConnectionFactoryConfigurationSupplier;
import io.agroal.api.configuration.supplier.AgroalConnectionPoolConfigurationSupplier;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.api.security.NamePrincipal;
import io.agroal.api.security.SimplePassword;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.sql.DataSource;

public class TxConnectionProvider implements AutoCloseable {
  public void configure(TxDatabaseAdapterConfig config) {}

  public void setupDatabase(
      Map<String, List<String>> perTableDDLs,
      List<String> formatParams,
      boolean metadataUpperCase,
      boolean batchDDL,
      String catalog,
      String schema) {
    try (Connection c = this.borrowConnection()) {
      try (Statement st = c.createStatement()) {
        Object[] formatParamsArray = formatParams.toArray();
        Stream<String> ddls =
            perTableDDLs.entrySet().stream()
                .filter(e -> !tableExists(c, metadataUpperCase, catalog, schema, e.getKey()))
                .flatMap(e -> e.getValue().stream())
                .map(s -> MessageFormat.format(s, formatParamsArray));

        executeDDLs(batchDDL, ddls, st);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void executeDDLs(boolean batchDDL, Stream<String> ddls, Statement st)
      throws SQLException {
    if (batchDDL) {
      String ddl = ddls.map(s -> s + ";").collect(Collectors.joining("\n\n"));
      if (!ddl.isEmpty()) {
        ddl = "BEGIN;\n" + ddl + "\nEND TRANSACTION;\n";
        st.execute(ddl);
      }
    } else {
      List<String> ddlStatements = ddls.collect(Collectors.toList());
      for (String ddl : ddlStatements) {
        st.execute(ddl);
      }
    }
  }

  public void dropTables(Set<String> tableNames, boolean batchDDL) {
    try (Connection c = this.borrowConnection()) {
      try (Statement st = c.createStatement()) {
        Stream<String> ddls =
            tableNames.stream().map(tableName -> String.format("DROP TABLE %s", tableName));
        executeDDLs(batchDDL, ddls, st);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static boolean tableExists(
      Connection conn, boolean metadataUpperCase, String catalog, String schema, String table) {
    if (metadataUpperCase) {
      catalog = catalog != null ? catalog.toUpperCase(Locale.ROOT) : null;
      schema = schema != null ? schema.toUpperCase(Locale.ROOT) : null;
      table = table != null ? table.toUpperCase(Locale.ROOT) : null;
    }

    try (ResultSet tables = conn.getMetaData().getTables(catalog, schema, table, null)) {
      return tables.next();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws Exception {}

  public Connection borrowConnection() throws SQLException {
    throw new UnsupportedOperationException();
  }

  public static class LocalConnectionProvider extends TxConnectionProvider {
    private DataSource dataSource;

    @Override
    public void configure(TxDatabaseAdapterConfig config) {
      AgroalDataSourceConfigurationSupplier dataSourceConfiguration =
          new AgroalDataSourceConfigurationSupplier();
      AgroalConnectionPoolConfigurationSupplier poolConfiguration =
          dataSourceConfiguration.connectionPoolConfiguration();
      AgroalConnectionFactoryConfigurationSupplier connectionFactoryConfiguration =
          poolConfiguration.connectionFactoryConfiguration();

      // configure pool
      poolConfiguration
          .initialSize(10)
          .maxSize(10)
          .minSize(10)
          .maxLifetime(Duration.of(5, ChronoUnit.MINUTES))
          .acquisitionTimeout(Duration.of(30, ChronoUnit.SECONDS));

      // configure supplier
      connectionFactoryConfiguration.jdbcUrl(config.getJdbcUrl());
      if (config.getJdbcUser() != null) {
        connectionFactoryConfiguration.credential(new NamePrincipal(config.getJdbcUser()));
        connectionFactoryConfiguration.credential(new SimplePassword(config.getJdbcPass()));
      }
      connectionFactoryConfiguration.jdbcTransactionIsolation(TransactionIsolation.READ_COMMITTED);
      connectionFactoryConfiguration.autoCommit(false);

      try {
        dataSource = AgroalDataSource.from(dataSourceConfiguration.get());
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void close() throws Exception {
      if (dataSource instanceof AutoCloseable) {
        ((AutoCloseable) dataSource).close();
      }
    }

    @Override
    public Connection borrowConnection() throws SQLException {
      return dataSource.getConnection();
    }
  }
}
