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

import java.sql.SQLException;
import java.sql.Types;
import java.util.EnumMap;
import java.util.Map;

/** SQL Server–specific dialect for the JDBC2 backend. */
final class SqlServerDatabaseSpecific implements DatabaseSpecific {

  private static final String VARCHAR = "NVARCHAR(255)";
  private static final String VARBINARY_MAX = "VARBINARY(MAX)";

  /** Max key length for nonclustered index; OBJ_ID is used in PK. */
  private static final String OBJ_ID = "VARBINARY(900)";

  private static final String MSSQL_CONSTRAINT_VIOLATION_SQL_STATE = "23000";
  private static final int MSSQL_ALREADY_EXISTS_ERROR = 2714;
  private static final String MSSQL_ALREADY_EXISTS_STATE_S0001 = "S0001";
  private static final String MSSQL_ALREADY_EXISTS_STATE_42S01 = "42S01";
  private static final String MSSQL_DEADLOCK_SQL_STATE = "40001";

  private final Map<Jdbc2ColumnType, String> typeMap;
  private final Map<Jdbc2ColumnType, Integer> typeIdMap;

  SqlServerDatabaseSpecific() {
    typeMap = new EnumMap<>(Jdbc2ColumnType.class);
    typeIdMap = new EnumMap<>(Jdbc2ColumnType.class);
    typeMap.put(Jdbc2ColumnType.NAME, VARCHAR);
    typeIdMap.put(Jdbc2ColumnType.NAME, Types.NVARCHAR);
    typeMap.put(Jdbc2ColumnType.OBJ_ID, OBJ_ID);
    typeIdMap.put(Jdbc2ColumnType.OBJ_ID, Types.VARBINARY);
    typeMap.put(Jdbc2ColumnType.BOOL, "BIT");
    typeIdMap.put(Jdbc2ColumnType.BOOL, Types.BIT);
    typeMap.put(Jdbc2ColumnType.VARBINARY, VARBINARY_MAX);
    typeIdMap.put(Jdbc2ColumnType.VARBINARY, Types.VARBINARY);
    typeMap.put(Jdbc2ColumnType.BIGINT, "BIGINT");
    typeIdMap.put(Jdbc2ColumnType.BIGINT, Types.BIGINT);
    typeMap.put(Jdbc2ColumnType.VARCHAR, VARCHAR);
    typeIdMap.put(Jdbc2ColumnType.VARCHAR, Types.NVARCHAR);
  }

  @Override
  public Map<Jdbc2ColumnType, String> columnTypes() {
    return typeMap;
  }

  @Override
  public Map<Jdbc2ColumnType, Integer> columnTypeIds() {
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
      return "MERGE INTO "
          + SqlConstants.TABLE_OBJS
          + " AS t USING (SELECT ? AS "
          + SqlConstants.COL_REPO_ID
          + ", ? AS "
          + SqlConstants.COL_OBJ_ID
          + ", ? AS "
          + SqlConstants.COL_OBJ_TYPE
          + ", ? AS "
          + SqlConstants.COL_OBJ_VERS
          + ", ? AS "
          + SqlConstants.COL_OBJ_VALUE
          + ", ? AS "
          + SqlConstants.COL_OBJ_REFERENCED
          + ") AS s ON t."
          + SqlConstants.COL_REPO_ID
          + " = s."
          + SqlConstants.COL_REPO_ID
          + " AND t."
          + SqlConstants.COL_OBJ_ID
          + " = s."
          + SqlConstants.COL_OBJ_ID
          + " WHEN NOT MATCHED BY TARGET THEN INSERT ("
          + SqlConstants.COL_REPO_ID
          + ", "
          + SqlConstants.COLS_OBJS_ALL_NAMES
          + ") VALUES (s."
          + SqlConstants.COL_REPO_ID
          + ", s."
          + SqlConstants.COL_OBJ_ID
          + ", s."
          + SqlConstants.COL_OBJ_TYPE
          + ", s."
          + SqlConstants.COL_OBJ_VERS
          + ", s."
          + SqlConstants.COL_OBJ_VALUE
          + ", s."
          + SqlConstants.COL_OBJ_REFERENCED
          + ");";
    }
    return sql;
  }

  @Override
  public String primaryKeyCol(String col, Jdbc2ColumnType columnType) {
    return col;
  }
}
