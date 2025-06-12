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
package org.projectnessie.versioned.storage.jdbc2;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static org.projectnessie.versioned.storage.jdbc2.AbstractJdbc2Persist.sqlSelectMultiple;
import static org.projectnessie.versioned.storage.jdbc2.Jdbc2ColumnType.BIGINT;
import static org.projectnessie.versioned.storage.jdbc2.Jdbc2ColumnType.BOOL;
import static org.projectnessie.versioned.storage.jdbc2.Jdbc2ColumnType.NAME;
import static org.projectnessie.versioned.storage.jdbc2.Jdbc2ColumnType.OBJ_ID;
import static org.projectnessie.versioned.storage.jdbc2.Jdbc2ColumnType.VARBINARY;
import static org.projectnessie.versioned.storage.jdbc2.Jdbc2ColumnType.VARCHAR;
import static org.projectnessie.versioned.storage.jdbc2.SqlConstants.COL_OBJ_ID;
import static org.projectnessie.versioned.storage.jdbc2.SqlConstants.COL_OBJ_REFERENCED;
import static org.projectnessie.versioned.storage.jdbc2.SqlConstants.COL_OBJ_TYPE;
import static org.projectnessie.versioned.storage.jdbc2.SqlConstants.COL_OBJ_VALUE;
import static org.projectnessie.versioned.storage.jdbc2.SqlConstants.COL_OBJ_VERS;
import static org.projectnessie.versioned.storage.jdbc2.SqlConstants.COL_REFS_CREATED_AT;
import static org.projectnessie.versioned.storage.jdbc2.SqlConstants.COL_REFS_DELETED;
import static org.projectnessie.versioned.storage.jdbc2.SqlConstants.COL_REFS_EXTENDED_INFO;
import static org.projectnessie.versioned.storage.jdbc2.SqlConstants.COL_REFS_NAME;
import static org.projectnessie.versioned.storage.jdbc2.SqlConstants.COL_REFS_POINTER;
import static org.projectnessie.versioned.storage.jdbc2.SqlConstants.COL_REFS_PREVIOUS;
import static org.projectnessie.versioned.storage.jdbc2.SqlConstants.COL_REPO_ID;
import static org.projectnessie.versioned.storage.jdbc2.SqlConstants.ERASE_OBJS;
import static org.projectnessie.versioned.storage.jdbc2.SqlConstants.ERASE_REFS;
import static org.projectnessie.versioned.storage.jdbc2.SqlConstants.TABLE_OBJS;
import static org.projectnessie.versioned.storage.jdbc2.SqlConstants.TABLE_REFS;

import com.google.common.collect.ImmutableMap;
import jakarta.annotation.Nonnull;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.sql.DataSource;
import org.projectnessie.versioned.storage.common.exceptions.UnknownOperationResultException;
import org.projectnessie.versioned.storage.common.persist.Backend;
import org.projectnessie.versioned.storage.common.persist.PersistFactory;

public final class Jdbc2Backend implements Backend {

  private static final int MAX_CREATE_TABLE_RECURSION_DEPTH = 3;

  private final DatabaseSpecific databaseSpecific;
  private final DataSource dataSource;
  private final boolean closeDataSource;
  private final String createTableRefsSql;
  private final String createTableObjsSql;
  private final int fetchSize;

  public Jdbc2Backend(
      @Nonnull Jdbc2BackendConfig config,
      @Nonnull DatabaseSpecific databaseSpecific,
      boolean closeDataSource) {
    this.dataSource = config.dataSource();
    this.fetchSize = config.fetchSize().orElse(Jdbc2BackendBaseConfig.DEFAULT_FETCH_SIZE);
    this.databaseSpecific = databaseSpecific;
    this.closeDataSource = closeDataSource;
    createTableRefsSql = buildCreateTableRefsSql(databaseSpecific);
    createTableObjsSql = buildCreateTableObjsSql(databaseSpecific);
  }

  // Columns ordered to minimize column padding in PostgreSQL. A column is padded to "have the same
  // length" as the _next_ column. "Bigger" columns should appear before "smaller" columns.
  private String buildCreateTableRefsSql(DatabaseSpecific databaseSpecific) {
    Map<Jdbc2ColumnType, String> columnTypes = databaseSpecific.columnTypes();
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
        + COL_REFS_EXTENDED_INFO
        + " "
        + columnTypes.get(OBJ_ID)
        + ",\n    "
        + COL_REFS_PREVIOUS
        + " "
        + columnTypes.get(VARBINARY)
        + ",\n    "
        + COL_REFS_CREATED_AT
        + " "
        + columnTypes.get(BIGINT)
        + " DEFAULT 0,\n    "
        + COL_REFS_DELETED
        + " "
        + columnTypes.get(BOOL)
        + ",\n    PRIMARY KEY ("
        + COL_REPO_ID
        + ", "
        + COL_REFS_NAME
        + ")\n  )";
  }

  private String buildCreateTableObjsSql(DatabaseSpecific databaseSpecific) {
    Map<Jdbc2ColumnType, String> columnTypes = databaseSpecific.columnTypes();
    return "CREATE TABLE "
        + TABLE_OBJS
        + "\n  (\n    "
        + COL_REPO_ID
        + " "
        + columnTypes.get(NAME)
        + ",\n    "
        + COL_OBJ_ID
        + " "
        + columnTypes.get(OBJ_ID)
        + ",\n    "
        + COL_OBJ_TYPE
        + " "
        + columnTypes.get(NAME)
        + ",\n    "
        + COL_OBJ_VERS
        + " "
        + columnTypes.get(VARCHAR)
        + ",\n    "
        + COL_OBJ_VALUE
        + " "
        + columnTypes.get(VARBINARY)
        + ",\n    "
        + COL_OBJ_REFERENCED
        + " "
        + columnTypes.get(BIGINT)
        + ",\n    PRIMARY KEY ("
        + COL_REPO_ID
        + ", "
        + databaseSpecific.primaryKeyCol(COL_OBJ_ID, OBJ_ID)
        + ")\n  )";
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
    return new Jdbc2PersistFactory(this);
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

      info.append(", ")
          .append(
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
                  ImmutableMap.of(COL_REPO_ID, nameTypeId, COL_REFS_NAME, nameTypeId)));
      info.append(", ")
          .append(
              createTableIfNotExists(
                  0,
                  conn,
                  TABLE_OBJS,
                  createTableObjsSql,
                  Stream.of(COL_REPO_ID, COL_OBJ_ID, COL_OBJ_TYPE, COL_OBJ_VERS, COL_OBJ_VALUE)
                      .collect(Collectors.toSet()),
                  ImmutableMap.of(COL_REPO_ID, nameTypeId, COL_OBJ_ID, objIdTypeId)));

      // Need to commit to get DDL changes into some databases (Postgres for example)
      conn.commit();

      return Optional.of(info.toString());
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private String createTableIfNotExists(
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
              "Expected primary key columns %s do not match existing primary key columns %s for table '%s' (type names and ordinals from java.sql.Types). DDL template:\n%s",
              withSqlTypesNames(expectedPrimaryKey),
              withSqlTypesNames(primaryKey),
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
          return format("table '%s' looks compatible", tableName);
        }
      }

      try {
        st.executeUpdate(createTable);

        return format("table '%s' created", tableName);
      } catch (SQLException e) {
        if (!databaseSpecific.isAlreadyExists(e)) {
          throw e;
        }

        if (depth >= MAX_CREATE_TABLE_RECURSION_DEPTH) {
          throw e;
        }

        // table was created by another process, try again to check the schema
        return createTableIfNotExists(
            depth + 1, conn, tableName, createTable, expectedColumns, expectedPrimaryKey);
      }
    }
  }

  private String withSqlTypesNames(Map<String, Integer> primaryKey) {
    StringBuilder sb = new StringBuilder().append('[');
    primaryKey.forEach(
        (col, type) -> {
          String typeName = sqlTypeName(type);
          if (sb.length() > 1) {
            sb.append(", ");
          }
          sb.append(col).append(" ").append(typeName).append("(JDBC id:").append(type).append(')');
        });
    return sb.append(']').toString();
  }

  private String sqlTypeName(int type) {
    return Arrays.stream(Types.class.getDeclaredFields())
        .filter(
            f ->
                Modifier.isPublic(f.getModifiers())
                    && Modifier.isStatic(f.getModifiers())
                    && f.getType() == int.class)
        .filter(
            f -> {
              try {
                return f.getInt(null) == type;
              } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
              }
            })
        .map(Field::getName)
        .findFirst()
        .orElseThrow();
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
