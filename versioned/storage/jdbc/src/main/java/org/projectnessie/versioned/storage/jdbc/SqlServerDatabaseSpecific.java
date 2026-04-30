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

import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** SQL Server–specific dialect for the legacy JDBC backend. */
final class SqlServerDatabaseSpecific implements DatabaseSpecific {

  private static final String VARCHAR = "NVARCHAR(255)";
  private static final String VARCHAR_MAX = "NVARCHAR(MAX)";
  private static final String VARBINARY_MAX = "VARBINARY(MAX)";

  /**
   * Constraint violation / duplicate key (23000 = integrity constraint, 2714 = object already
   * exists).
   */
  private static final String MSSQL_CONSTRAINT_VIOLATION_SQL_STATE = "23000";

  private static final int MSSQL_ALREADY_EXISTS_ERROR = 2714;
  private static final String MSSQL_ALREADY_EXISTS_STATE_S0001 = "S0001";
  private static final String MSSQL_ALREADY_EXISTS_STATE_42S01 = "42S01";
  private static final String MSSQL_DEADLOCK_SQL_STATE = "40001";

  private static final Pattern INSERT_COLUMNS_PATTERN =
      Pattern.compile(
          "INSERT\\s+INTO\\s+\\w+\\s*\\(\\s*(.+?)\\s*\\)\\s+VALUES",
          Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  private final Map<JdbcColumnType, String> typeMap;
  private final Map<JdbcColumnType, Integer> typeIdMap;

  SqlServerDatabaseSpecific() {
    typeMap = new EnumMap<>(JdbcColumnType.class);
    typeIdMap = new EnumMap<>(JdbcColumnType.class);
    typeMap.put(JdbcColumnType.NAME, VARCHAR);
    typeIdMap.put(JdbcColumnType.NAME, Types.NVARCHAR);
    typeMap.put(JdbcColumnType.OBJ_ID, VARCHAR);
    typeIdMap.put(JdbcColumnType.OBJ_ID, Types.NVARCHAR);
    typeMap.put(JdbcColumnType.OBJ_ID_LIST, VARCHAR_MAX);
    typeIdMap.put(JdbcColumnType.OBJ_ID_LIST, Types.NVARCHAR);
    typeMap.put(JdbcColumnType.BOOL, "BIT");
    typeIdMap.put(JdbcColumnType.BOOL, Types.BIT);
    typeMap.put(JdbcColumnType.VARBINARY, VARBINARY_MAX);
    typeIdMap.put(JdbcColumnType.VARBINARY, Types.VARBINARY);
    typeMap.put(JdbcColumnType.BIGINT, "BIGINT");
    typeIdMap.put(JdbcColumnType.BIGINT, Types.BIGINT);
    typeMap.put(JdbcColumnType.VARCHAR, VARCHAR);
    typeIdMap.put(JdbcColumnType.VARCHAR, Types.NVARCHAR);
  }

  @Override
  public Map<JdbcColumnType, String> columnTypes() {
    return typeMap;
  }

  @Override
  public Map<JdbcColumnType, Integer> columnTypeIds() {
    return typeIdMap;
  }

  @Override
  public boolean isConstraintViolation(SQLException e) {
    return MSSQL_CONSTRAINT_VIOLATION_SQL_STATE.equals(e.getSQLState())
        || (e.getErrorCode() == MSSQL_ALREADY_EXISTS_ERROR);
  }

  @Override
  public boolean isRetryTransaction(SQLException e) {
    return MSSQL_DEADLOCK_SQL_STATE.equals(e.getSQLState());
  }

  @Override
  public boolean isAlreadyExists(SQLException e) {
    if (e.getErrorCode() == MSSQL_ALREADY_EXISTS_ERROR) {
      return true;
    }
    String state = e.getSQLState();
    return MSSQL_ALREADY_EXISTS_STATE_S0001.equals(state)
        || MSSQL_ALREADY_EXISTS_STATE_42S01.equals(state);
  }

  @Override
  public String wrapInsert(String sql) {
    // SQL Server has no ON CONFLICT DO NOTHING; use MERGE for insert-ignore semantics.
    if (sql.contains(SqlConstants.TABLE_REFS)) {
      return "MERGE INTO "
          + SqlConstants.TABLE_REFS
          + " AS t USING (SELECT ? AS "
          + SqlConstants.COL_REPO_ID
          + ", ? AS "
          + SqlConstants.COL_REFS_NAME
          + ", ? AS "
          + SqlConstants.COL_REFS_POINTER
          + ", ? AS "
          + SqlConstants.COL_REFS_DELETED
          + ", ? AS "
          + SqlConstants.COL_REFS_CREATED_AT
          + ", ? AS "
          + SqlConstants.COL_REFS_EXTENDED_INFO
          + ", ? AS "
          + SqlConstants.COL_REFS_PREVIOUS
          + ") AS s ON t."
          + SqlConstants.COL_REPO_ID
          + " = s."
          + SqlConstants.COL_REPO_ID
          + " AND t."
          + SqlConstants.COL_REFS_NAME
          + " = s."
          + SqlConstants.COL_REFS_NAME
          + " WHEN NOT MATCHED BY TARGET THEN INSERT ("
          + SqlConstants.COL_REPO_ID
          + ", "
          + SqlConstants.COL_REFS_NAME
          + ", "
          + SqlConstants.COL_REFS_POINTER
          + ", "
          + SqlConstants.COL_REFS_DELETED
          + ", "
          + SqlConstants.COL_REFS_CREATED_AT
          + ", "
          + SqlConstants.COL_REFS_EXTENDED_INFO
          + ", "
          + SqlConstants.COL_REFS_PREVIOUS
          + ") VALUES (s."
          + SqlConstants.COL_REPO_ID
          + ", s."
          + SqlConstants.COL_REFS_NAME
          + ", s."
          + SqlConstants.COL_REFS_POINTER
          + ", s."
          + SqlConstants.COL_REFS_DELETED
          + ", s."
          + SqlConstants.COL_REFS_CREATED_AT
          + ", s."
          + SqlConstants.COL_REFS_EXTENDED_INFO
          + ", s."
          + SqlConstants.COL_REFS_PREVIOUS
          + ");";
    }
    if (sql.contains(SqlConstants.TABLE_OBJS)) {
      Matcher m = INSERT_COLUMNS_PATTERN.matcher(sql);
      if (m.find()) {
        String cols = m.group(1);
        String[] columnNames =
            Arrays.stream(cols.split(",")).map(String::trim).toArray(String[]::new);
        String colList = String.join(", ", columnNames);
        String selectAliasList =
            String.join(", ", Arrays.stream(columnNames).map(c -> "? AS " + c).toList());
        String valuesList =
            String.join(", ", Arrays.stream(columnNames).map(c -> "s." + c).toList());
        return "MERGE INTO "
            + SqlConstants.TABLE_OBJS
            + " AS t USING (SELECT "
            + selectAliasList
            + ") AS s ON t."
            + SqlConstants.COL_REPO_ID
            + " = s."
            + SqlConstants.COL_REPO_ID
            + " AND t."
            + SqlConstants.COL_OBJ_ID
            + " = s."
            + SqlConstants.COL_OBJ_ID
            + " WHEN NOT MATCHED BY TARGET THEN INSERT ("
            + colList
            + ") VALUES ("
            + valuesList
            + ");";
      }
    }
    return sql;
  }
}
