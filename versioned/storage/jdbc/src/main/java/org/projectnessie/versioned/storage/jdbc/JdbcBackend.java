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
package org.projectnessie.versioned.storage.jdbc;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static org.projectnessie.versioned.storage.jdbc.AbstractJdbcPersist.sqlSelectMultiple;
import static org.projectnessie.versioned.storage.jdbc.JdbcColumnType.BIGINT;
import static org.projectnessie.versioned.storage.jdbc.JdbcColumnType.BOOL;
import static org.projectnessie.versioned.storage.jdbc.JdbcColumnType.NAME;
import static org.projectnessie.versioned.storage.jdbc.JdbcColumnType.OBJ_ID;
import static org.projectnessie.versioned.storage.jdbc.JdbcColumnType.VARBINARY;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.COLS_OBJS_ALL;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.COL_OBJ_ID;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.COL_REFS_CREATED_AT;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.COL_REFS_DELETED;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.COL_REFS_EXTENDED_INFO;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.COL_REFS_NAME;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.COL_REFS_POINTER;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.COL_REFS_PREVIOUS;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.COL_REPO_ID;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.ERASE_OBJS;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.ERASE_REFS;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.TABLE_OBJS;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.TABLE_REFS;

import com.google.common.collect.ImmutableMap;
import jakarta.annotation.Nonnull;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.sql.DataSource;
import org.projectnessie.versioned.storage.common.exceptions.UnknownOperationResultException;
import org.projectnessie.versioned.storage.common.persist.Backend;
import org.projectnessie.versioned.storage.common.persist.PersistFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JdbcBackend implements Backend {

  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcBackend.class);

  private static final int MAX_CREATE_TABLE_RECURSION_DEPTH = 3;

  private final DatabaseSpecific databaseSpecific;
  private final DataSource dataSource;
  private final boolean closeDataSource;
  private final String createTableRefsSql;
  private final String createTableObjsSql;
  private final int fetchSize;

  public JdbcBackend(
      @Nonnull JdbcBackendConfig config,
      @Nonnull DatabaseSpecific databaseSpecific,
      boolean closeDataSource) {
    this.dataSource = config.dataSource();
    this.fetchSize = config.fetchSize().orElse(JdbcBackendBaseConfig.DEFAULT_FETCH_SIZE);
    this.databaseSpecific = databaseSpecific;
    this.closeDataSource = closeDataSource;
    createTableRefsSql = buildCreateTableRefsSql(databaseSpecific);
    createTableObjsSql = buildCreateTableObjsSql(databaseSpecific);
  }

  private String buildCreateTableRefsSql(DatabaseSpecific databaseSpecific) {
    Map<JdbcColumnType, String> columnTypes = databaseSpecific.columnTypes();
    return "CREATE TABLE "
        + TABLE_REFS
        + "\n  (\n    "
        + COL_REPO_ID
        + " "
        + columnTypes.get(NAME)
        + ",\n    "
        + COL_REFS_NAME
        + " "
        + columnTypes.get(NAME)
        + ",\n    "
        + COL_REFS_POINTER
        + " "
        + columnTypes.get(OBJ_ID)
        + ",\n    "
        + COL_REFS_DELETED
        + " "
        + columnTypes.get(BOOL)
        + ",\n    "
        + COL_REFS_CREATED_AT
        + " "
        + columnTypes.get(BIGINT)
        + " DEFAULT 0,\n    "
        + COL_REFS_EXTENDED_INFO
        + " "
        + columnTypes.get(OBJ_ID)
        + ",\n    "
        + COL_REFS_PREVIOUS
        + " "
        + columnTypes.get(VARBINARY)
        + ",\n    PRIMARY KEY ("
        + COL_REPO_ID
        + ", "
        + COL_REFS_NAME
        + ")\n  )";
  }

  private String buildCreateTableObjsSql(DatabaseSpecific databaseSpecific) {
    Map<JdbcColumnType, String> columnTypes = databaseSpecific.columnTypes();
    StringBuilder sb =
        new StringBuilder()
            .append("CREATE TABLE ")
            .append(TABLE_OBJS)
            .append(" (\n    ")
            .append(COL_REPO_ID)
            .append(" ")
            .append(columnTypes.get(NAME));
    for (Entry<String, JdbcColumnType> entry : COLS_OBJS_ALL.entrySet()) {
      String colName = entry.getKey();
      String colType = columnTypes.get(entry.getValue());
      sb.append(",\n    ").append(colName).append(" ").append(colType);
    }
    sb.append(",\n    PRIMARY KEY (")
        .append(COL_REPO_ID)
        .append(", ")
        .append(COL_OBJ_ID)
        .append(")\n  )");
    return sb.toString();
  }

  DatabaseSpecific databaseSpecific() {
    return databaseSpecific;
  }

  @Override
  public void close() {
    if (closeDataSource) {
      try {
        if (dataSource instanceof AutoCloseable) {
          ((AutoCloseable) dataSource).close();
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Nonnull
  @Override
  public PersistFactory createFactory() {
    return new JdbcPersistFactory(this);
  }

  int fetchSize() {
    return fetchSize;
  }

  Connection borrowConnection() throws SQLException {
    Connection c = dataSource.getConnection();
    c.setAutoCommit(false);
    return c;
  }

  @Override
  public Optional<String> setupSchema() {
    try (Connection conn = borrowConnection()) {
      Integer nameTypeId = databaseSpecific.columnTypeIds().get(NAME);
      Integer objIdTypeId = databaseSpecific.columnTypeIds().get(OBJ_ID);
      createTableIfNotExists(
          0,
          conn,
          TABLE_REFS,
          createTableRefsSql,
          Stream.of(
                  COL_REPO_ID,
                  COL_REFS_NAME,
                  COL_REFS_POINTER,
                  COL_REFS_DELETED,
                  COL_REFS_CREATED_AT,
                  COL_REFS_EXTENDED_INFO,
                  COL_REFS_PREVIOUS)
              .collect(Collectors.toSet()),
          ImmutableMap.of(COL_REPO_ID, nameTypeId, COL_REFS_NAME, nameTypeId));
      createTableIfNotExists(
          0,
          conn,
          TABLE_OBJS,
          createTableObjsSql,
          Stream.concat(Stream.of(COL_REPO_ID), COLS_OBJS_ALL.keySet().stream())
              .collect(Collectors.toSet()),
          ImmutableMap.of(COL_REPO_ID, nameTypeId, COL_OBJ_ID, objIdTypeId));
      StringBuilder info = new StringBuilder();
      String s = conn.getCatalog();
      if (s != null && !s.isEmpty()) {
        info.append("catalog: ").append(s);
      }
      s = conn.getSchema();
      if (s != null && !s.isEmpty()) {
        if (info.length() > 0) {
          info.append(", ");
        }
        info.append("schema: ").append(s);
      }
      return Optional.of(info.toString());
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void createTableIfNotExists(
      int depth,
      Connection conn,
      String tableName,
      String createTable,
      Set<String> expectedColumns,
      Map<String, Integer> expectedPrimaryKey)
      throws SQLException {

    try (Statement st = conn.createStatement()) {
      if (conn.getMetaData().storesLowerCaseIdentifiers()) {
        tableName = tableName.toLowerCase(Locale.ROOT);
      } else if (conn.getMetaData().storesUpperCaseIdentifiers()) {
        tableName = tableName.toUpperCase(Locale.ROOT);
      }

      String catalog = conn.getCatalog();
      String schema = conn.getSchema();

      try (ResultSet rs = conn.getMetaData().getTables(catalog, schema, tableName, null)) {
        if (rs.next()) {
          // table already exists

          Map<String, Integer> primaryKey = new LinkedHashMap<>();
          Map<String, Integer> columns = new LinkedHashMap<>();

          try (ResultSet cols = conn.getMetaData().getColumns(catalog, schema, tableName, null)) {
            while (cols.next()) {
              String colName = cols.getString("COLUMN_NAME").toLowerCase(Locale.ROOT);
              columns.put(colName, cols.getInt("DATA_TYPE"));
            }
          }
          try (ResultSet cols = conn.getMetaData().getPrimaryKeys(catalog, schema, tableName)) {
            while (cols.next()) {
              String colName = cols.getString("COLUMN_NAME").toLowerCase(Locale.ROOT);
              int colType = columns.get(colName);
              primaryKey.put(colName.toLowerCase(Locale.ROOT), colType);
            }
          }

          checkState(
              primaryKey.equals(expectedPrimaryKey),
              "Expected primary key columns %s do not match existing primary key columns %s for table '%s'. DDL template:\n%s",
              expectedPrimaryKey.keySet(),
              primaryKey.keySet(),
              tableName,
              createTable);
          Set<String> missingColumns = new HashSet<>(expectedColumns);
          missingColumns.removeAll(columns.keySet());
          if (!missingColumns.isEmpty()) {
            throw new IllegalStateException(
                format(
                    "The database table %s is missing mandatory columns %s.%nFound columns : %s%nExpected columns : %s%nDDL template:\n%s",
                    tableName,
                    sortedColumnNames(missingColumns),
                    sortedColumnNames(columns.keySet()),
                    sortedColumnNames(expectedColumns),
                    createTable));
          }

          // Existing table looks compatible
          return;
        }
      }

      try {
        st.executeUpdate(createTable);
      } catch (SQLException e) {
        if (!databaseSpecific.isAlreadyExists(e)) {
          throw e;
        }

        if (depth >= MAX_CREATE_TABLE_RECURSION_DEPTH) {
          throw e;
        }

        // table was created by another process, try again to check the schema
        createTableIfNotExists(
            depth + 1, conn, tableName, createTable, expectedColumns, expectedPrimaryKey);
      }
    }
  }

  private static String sortedColumnNames(Collection<?> input) {
    return input.stream().map(Object::toString).sorted().collect(Collectors.joining(","));
  }

  @Override
  public void eraseRepositories(Set<String> repositoryIds) {
    if (repositoryIds == null || repositoryIds.isEmpty()) {
      return;
    }

    try (Connection conn = borrowConnection()) {

      try (PreparedStatement ps =
          conn.prepareStatement(sqlSelectMultiple(ERASE_REFS, repositoryIds.size()))) {
        int i = 1;
        for (String repositoryId : repositoryIds) {
          ps.setString(i++, repositoryId);
        }
        ps.executeUpdate();
      }
      try (PreparedStatement ps =
          conn.prepareStatement(sqlSelectMultiple(ERASE_OBJS, repositoryIds.size()))) {
        int i = 1;
        for (String repositoryId : repositoryIds) {
          ps.setString(i++, repositoryId);
        }
        ps.executeUpdate();
      }
      conn.commit();
    } catch (SQLException e) {
      throw unhandledSQLException(databaseSpecific, e);
    }
  }

  static RuntimeException unhandledSQLException(DatabaseSpecific databaseSpecific, SQLException e) {
    if (databaseSpecific.isRetryTransaction(e)) {
      return new UnknownOperationResultException("Unhandled SQL exception", e);
    }
    return new RuntimeException("Unhandled SQL exception", e);
  }
}
