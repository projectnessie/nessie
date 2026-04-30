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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.projectnessie.versioned.storage.jdbc2.SqlConstants.ADD_REFERENCE;
import static org.projectnessie.versioned.storage.jdbc2.SqlConstants.COL_OBJ_ID;
import static org.projectnessie.versioned.storage.jdbc2.SqlConstants.COL_REPO_ID;
import static org.projectnessie.versioned.storage.jdbc2.SqlConstants.STORE_OBJ;
import static org.projectnessie.versioned.storage.jdbc2.SqlConstants.TABLE_OBJS;

import java.sql.SQLException;
import java.sql.Types;
import org.junit.jupiter.api.Test;

class TestSqlServerDatabaseSpecific {

  private final SqlServerDatabaseSpecific db = new SqlServerDatabaseSpecific();

  @Test
  void columnTypesUseNvarcharVarbinary900AndBit() {
    assertEquals("NVARCHAR(255)", db.columnTypes().get(Jdbc2ColumnType.NAME));
    assertEquals("VARBINARY(900)", db.columnTypes().get(Jdbc2ColumnType.OBJ_ID));
    assertEquals("BIT", db.columnTypes().get(Jdbc2ColumnType.BOOL));
    assertEquals("VARBINARY(MAX)", db.columnTypes().get(Jdbc2ColumnType.VARBINARY));
  }

  @Test
  void columnTypeIdsMatchJdbcTypes() {
    assertEquals(Types.NVARCHAR, db.columnTypeIds().get(Jdbc2ColumnType.NAME));
    assertEquals(Types.VARBINARY, db.columnTypeIds().get(Jdbc2ColumnType.OBJ_ID));
    assertEquals(Types.BIT, db.columnTypeIds().get(Jdbc2ColumnType.BOOL));
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
  void wrapInsert_storeObjBecomesMerge() {
    String merged = db.wrapInsert(STORE_OBJ);
    assertTrue(merged.startsWith("MERGE INTO " + TABLE_OBJS));
    assertTrue(merged.contains("WHEN NOT MATCHED BY TARGET THEN INSERT"));
    assertTrue(merged.contains("ON t." + COL_REPO_ID + " = s." + COL_REPO_ID));
    assertTrue(merged.contains("AND t." + COL_OBJ_ID + " = s." + COL_OBJ_ID));
    assertTrue(merged.contains("? AS " + SqlConstants.COL_OBJ_TYPE));
    assertTrue(merged.contains("? AS " + SqlConstants.COL_OBJ_VALUE));
  }

  @Test
  void primaryKeyColUnmodified() {
    assertEquals("obj_id", db.primaryKeyCol("obj_id", Jdbc2ColumnType.OBJ_ID));
  }

  @Test
  void wrapInsert_unrelatedSqlUnchanged() {
    assertEquals("SELECT 1", db.wrapInsert("SELECT 1"));
  }
}
