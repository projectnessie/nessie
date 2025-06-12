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
package org.projectnessie.gc.contents.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Spliterators.AbstractSpliterator;
import java.util.function.Consumer;
import java.util.function.Supplier;

public final class JdbcHelper {
  private JdbcHelper() {}

  @SuppressWarnings("SqlSourceToSinkFlow")
  public static void createTables(Connection connection, boolean ifNotExists) throws SQLException {
    try (Statement st = connection.createStatement()) {
      Map<String, String> createTableStatements = getCreateTableStatements();
      for (String tableName : createTableStatements.keySet()) {
        if (!ifNotExists || !tableExists(connection, tableName)) {
          st.execute(createTableStatements.get(tableName));
        }
      }
    }
  }

  @SuppressWarnings({"SqlSourceToSinkFlow", "SqlDialectInspection", "SqlNoDataSourceInspection"})
  public static void dropTables(Connection connection) throws SQLException {
    try (Statement st = connection.createStatement()) {
      for (String tableName : SqlDmlDdl.ALL_CREATES.keySet()) {
        if (tableExists(connection, tableName)) {
          st.execute("DROP TABLE " + tableName);
        }
      }
    }
  }

  private static boolean tableExists(Connection connection, String tableName) throws SQLException {
    DatabaseMetaData meta = connection.getMetaData();
    if (meta.storesUpperCaseIdentifiers()) {
      tableName = tableName.toUpperCase(Locale.ROOT);
    }
    String catalog = connection.getCatalog();
    String schema = connection.getSchema();
    try (ResultSet rs = meta.getTables(catalog, schema, tableName, new String[] {"TABLE"})) {
      return rs.next();
    }
  }

  public static Map<String, String> getCreateTableStatements() {
    return SqlDmlDdl.ALL_CREATES;
  }

  static Exception forClose(List<AutoCloseable> closeables) {
    Exception e = null;
    for (int i = closeables.size() - 1; i >= 0; i--) {
      try {
        closeables.get(i).close();
      } catch (Exception ex) {
        if (e != null) {
          e.addSuppressed(ex);
        } else {
          e = ex;
        }
      }
    }
    return e;
  }

  @FunctionalInterface
  interface WithStatement<R> {
    R withStatement(Connection connection, PreparedStatement preparedStatement) throws SQLException;
  }

  @FunctionalInterface
  interface Prepare {
    void prepare(PreparedStatement preparedStatement) throws SQLException;
  }

  @FunctionalInterface
  interface FromRow<R> {
    R fromRow(ResultSet resultSet) throws SQLException;
  }

  static final class ResultSetSplit<R> extends AbstractSpliterator<R> {
    private final Supplier<Connection> connectionSupplier;
    private final int fetchSize;
    private final Consumer<AutoCloseable> closeables;
    private final String sql;
    private final Prepare prepare;
    private final FromRow<R> fromRow;
    private ResultSet resultSet;

    ResultSetSplit(
        Supplier<Connection> connectionSupplier,
        int fetchSize,
        Consumer<AutoCloseable> closeables,
        String sql,
        Prepare prepare,
        FromRow<R> fromRow) {
      super(Long.MAX_VALUE, 0);
      this.connectionSupplier = connectionSupplier;
      this.fetchSize = fetchSize;
      this.closeables = closeables;
      this.sql = sql;
      this.prepare = prepare;
      this.fromRow = fromRow;
    }

    @Override
    public boolean tryAdvance(Consumer<? super R> action) {
      try {
        if (resultSet == null) {
          Connection conn = connectionSupplier.get();
          closeables.accept(conn);
          PreparedStatement stmt = conn.prepareStatement(sql);
          stmt.setFetchSize(fetchSize);
          closeables.accept(stmt);
          prepare.prepare(stmt);
          resultSet = stmt.executeQuery();
          resultSet.setFetchSize(fetchSize);
          closeables.accept(resultSet);
        }

        if (!resultSet.next()) {
          return false;
        }

        R value = fromRow.fromRow(resultSet);

        action.accept(value);

        return true;
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Check whether the given {@link Throwable} represents an exception that indicates an
   * integrity-constraint-violation.
   */
  static boolean isIntegrityConstraintViolation(Throwable e) {
    if (e instanceof SQLException) {
      SQLException sqlException = (SQLException) e;
      return sqlException instanceof SQLIntegrityConstraintViolationException
          // e.g. H2
          || CONSTRAINT_VIOLATION_SQL_CODE == sqlException.getErrorCode()
          // e.g. Postgres & Cockroach
          || CONSTRAINT_VIOLATION_SQL_STATE.equals(sqlException.getSQLState());
    }
    return false;
  }

  /** Postgres &amp; Cockroach integrity constraint violation. */
  static final String CONSTRAINT_VIOLATION_SQL_STATE = "23505";

  /** H2 integrity constraint violation. */
  static final int CONSTRAINT_VIOLATION_SQL_CODE = 23505;
}
