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

import static java.util.stream.Collectors.joining;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.ADD_REFERENCE;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.COLS_OBJS_ALL;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.COL_OBJ_ID;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.COL_REPO_ID;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.TABLE_OBJS;

import com.google.common.collect.ImmutableMap;
import java.sql.SQLException;
import java.sql.Types;
import org.junit.jupiter.api.Test;

class TestSqlServerDatabaseSpecific {

  private final SqlServerDatabaseSpecific db = new SqlServerDatabaseSpecific();

  @Test
  void columnTypesUseNvarcharAndBit() {
    assertEquals("NVARCHAR(255)", db.columnTypes().get(JdbcColumnType.NAME));
    assertEquals("BIT", db.columnTypes().get(JdbcColumnType.BOOL));
    assertEquals("VARBINARY(MAX)", db.columnTypes().get(JdbcColumnType.VARBINARY));
  }

  @Test
  void columnTypeIdsMatchJdbcTypes() {
    assertEquals(Types.NVARCHAR, db.columnTypeIds().get(JdbcColumnType.NAME));
    assertEquals(Types.BIT, db.columnTypeIds().get(JdbcColumnType.BOOL));
    assertEquals(Types.VARBINARY, db.columnTypeIds().get(JdbcColumnType.VARBINARY));
  }

  @Test
  void isConstraintViolation_sqlState23000() {
    assertTrue(db.isConstraintViolation(new SQLException("x", "23000", 0)));
  }

  @Test
  void isConstraintViolation_error2714() {
    assertTrue(db.isConstraintViolation(new SQLException("x", "01000", 2714)));
  }

  @Test
  void isConstraintViolation_other() {
    assertFalse(db.isConstraintViolation(new SQLException("x", "01000", 0)));
  }

  @Test
  void isRetryTransaction_deadlock() {
    assertTrue(db.isRetryTransaction(new SQLException("deadlock", "40001", 0)));
    assertFalse(db.isRetryTransaction(new SQLException("x", "01000", 0)));
  }

  @Test
  void isAlreadyExists() {
    assertTrue(db.isAlreadyExists(new SQLException("x", "S0001", 0)));
    assertTrue(db.isAlreadyExists(new SQLException("x", "42S01", 0)));
    assertTrue(db.isAlreadyExists(new SQLException("x", "01000", 2714)));
    assertFalse(db.isAlreadyExists(new SQLException("x", "01000", 0)));
  }

  @Test
  void wrapInsert_refsBecomesMerge() {
    String merged = db.wrapInsert(ADD_REFERENCE);
    assertTrue(merged.startsWith("MERGE INTO " + SqlConstants.TABLE_REFS));
    assertTrue(merged.contains("WHEN NOT MATCHED BY TARGET THEN INSERT"));
    assertTrue(merged.contains("ON t." + COL_REPO_ID + " = s." + COL_REPO_ID));
    assertTrue(
        merged.contains(
            "AND t." + SqlConstants.COL_REFS_NAME + " = s." + SqlConstants.COL_REFS_NAME));
  }

  @Test
  void wrapInsert_objsBecomesMergeWithDynamicColumns() {
    ImmutableMap.Builder<String, Integer> params = ImmutableMap.builder();
    int i = 1;
    params.put(COL_REPO_ID, i++);
    for (String col : COLS_OBJS_ALL.keySet()) {
      params.put(col, i++);
    }
    ImmutableMap<String, Integer> storeObjSqlParams = params.build();
    String insert =
        "INSERT INTO "
            + TABLE_OBJS
            + " ("
            + String.join(", ", storeObjSqlParams.keySet())
            + ") VALUES ("
            + storeObjSqlParams.keySet().stream().map(c -> "?").collect(joining(", "))
            + ")";

    String merged = db.wrapInsert(insert);
    assertTrue(merged.startsWith("MERGE INTO " + TABLE_OBJS));
    assertTrue(merged.contains("WHEN NOT MATCHED BY TARGET THEN INSERT"));
    assertTrue(merged.contains("ON t." + COL_REPO_ID + " = s." + COL_REPO_ID));
    assertTrue(merged.contains("AND t." + COL_OBJ_ID + " = s." + COL_OBJ_ID));
    for (String col : storeObjSqlParams.keySet()) {
      assertTrue(merged.contains("? AS " + col), () -> "missing alias for column " + col);
    }
  }

  @Test
  void wrapInsert_unrelatedSqlUnchanged() {
    assertEquals("SELECT 1", db.wrapInsert("SELECT 1"));
  }
}
