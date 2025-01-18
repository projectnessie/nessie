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

final class SqlConstants {

  static final int MAX_BATCH_SIZE = 50;

  static final String TABLE_REFS = "refs2";
  static final String TABLE_OBJS = "objs2";

  static final String COL_REPO_ID = "repo";
  static final String COL_OBJ_TYPE = "obj_type";
  static final String COL_OBJ_ID = "obj_id";
  static final String COL_OBJ_VERS = "obj_vers";
  static final String COL_OBJ_VALUE = "obj_value";
  static final String COL_OBJ_REFERENCED = "obj_ref";

  static final String ERASE_OBJS =
      "DELETE FROM " + TABLE_OBJS + " WHERE " + COL_REPO_ID + " IN (?)";
  static final String ERASE_REFS =
      "DELETE FROM " + TABLE_REFS + " WHERE " + COL_REPO_ID + " IN (?)";
  static final String DELETE_OBJ =
      "DELETE FROM " + TABLE_OBJS + " WHERE " + COL_REPO_ID + "=? AND " + COL_OBJ_ID + "=?";
  static final String DELETE_OBJ_CONDITIONAL =
      "DELETE FROM "
          + TABLE_OBJS
          + " WHERE "
          + COL_REPO_ID
          + "=? AND "
          + COL_OBJ_ID
          + "=? AND "
          + COL_OBJ_TYPE
          + "=? AND "
          + COL_OBJ_VERS
          + "=?";
  static final String UPDATE_OBJS_REFERENCED =
      "UPDATE "
          + TABLE_OBJS
          + " SET "
          + COL_OBJ_REFERENCED
          + "=? WHERE "
          + COL_REPO_ID
          + "=? AND "
          + COL_OBJ_ID
          + "=?";
  static final String DELETE_OBJ_REFERENCED =
      "DELETE FROM "
          + TABLE_OBJS
          + " WHERE "
          + COL_REPO_ID
          + "=? AND "
          + COL_OBJ_ID
          + "=? AND "
          + COL_OBJ_REFERENCED
          + "=?";
  static final String DELETE_OBJ_REFERENCED_NULL =
      "DELETE FROM "
          + TABLE_OBJS
          + " WHERE "
          + COL_REPO_ID
          + "=? AND "
          + COL_OBJ_ID
          + "=? AND ("
          + COL_OBJ_REFERENCED
          + " IS NULL OR "
          + COL_OBJ_REFERENCED
          + "=0)";

  static final String COL_REFS_NAME = "ref_name";
  static final String COL_REFS_POINTER = "pointer";
  static final String COL_REFS_DELETED = "deleted";
  static final String COL_REFS_CREATED_AT = "created_at";
  static final String COL_REFS_EXTENDED_INFO = "ext_info";
  static final String COL_REFS_PREVIOUS = "prev_ptr";
  static final String REFS_CREATED_AT_COND = "_REFS_CREATED_AT_";
  static final String REFS_EXTENDED_INFO_COND = "_REFS_EXTENDED_INFO_";
  static final String UPDATE_REFERENCE_POINTER =
      "UPDATE "
          + TABLE_REFS
          + " SET "
          + COL_REFS_POINTER
          + "=?, "
          + COL_REFS_PREVIOUS
          + "=? WHERE "
          + COL_REPO_ID
          + "=? AND "
          + COL_REFS_NAME
          + "=? AND "
          + COL_REFS_POINTER
          + "=? AND "
          + COL_REFS_DELETED
          + "=? AND "
          + COL_REFS_CREATED_AT
          + REFS_CREATED_AT_COND
          + " AND "
          + COL_REFS_EXTENDED_INFO
          + REFS_EXTENDED_INFO_COND;
  static final String PURGE_REFERENCE =
      "DELETE FROM "
          + TABLE_REFS
          + " WHERE "
          + COL_REPO_ID
          + "=? AND "
          + COL_REFS_NAME
          + "=? AND "
          + COL_REFS_POINTER
          + "=? AND "
          + COL_REFS_DELETED
          + "=? AND "
          + COL_REFS_CREATED_AT
          + REFS_CREATED_AT_COND
          + " AND "
          + COL_REFS_EXTENDED_INFO
          + REFS_EXTENDED_INFO_COND;
  static final String MARK_REFERENCE_AS_DELETED =
      "UPDATE "
          + TABLE_REFS
          + " SET "
          + COL_REFS_DELETED
          + "=? WHERE "
          + COL_REPO_ID
          + "=? AND "
          + COL_REFS_NAME
          + "=? AND "
          + COL_REFS_POINTER
          + "=? AND "
          + COL_REFS_DELETED
          + "=? AND "
          + COL_REFS_CREATED_AT
          + REFS_CREATED_AT_COND
          + " AND "
          + COL_REFS_EXTENDED_INFO
          + REFS_EXTENDED_INFO_COND;
  static final String ADD_REFERENCE =
      "INSERT INTO "
          + TABLE_REFS
          + " ("
          + COL_REPO_ID
          + ", "
          + COL_REFS_NAME
          + ", "
          + COL_REFS_POINTER
          + ", "
          + COL_REFS_DELETED
          + ", "
          + COL_REFS_CREATED_AT
          + ", "
          + COL_REFS_EXTENDED_INFO
          + ", "
          + COL_REFS_PREVIOUS
          + ") VALUES (?, ?, ?, ?, ?, ?, ?)";
  static final String FIND_REFERENCES =
      "SELECT "
          + COL_REFS_NAME
          + ", "
          + COL_REFS_POINTER
          + ", "
          + COL_REFS_DELETED
          + ", "
          + COL_REFS_CREATED_AT
          + ", "
          + COL_REFS_EXTENDED_INFO
          + ", "
          + COL_REFS_PREVIOUS
          + " FROM "
          + TABLE_REFS
          + " WHERE "
          + COL_REPO_ID
          + "=? AND "
          + COL_REFS_NAME
          + " IN (?)";

  static final String COLS_OBJS_ALL_NAMES =
      COL_OBJ_ID
          + ", "
          + COL_OBJ_TYPE
          + ", "
          + COL_OBJ_VERS
          + ", "
          + COL_OBJ_VALUE
          + ", "
          + COL_OBJ_REFERENCED;

  static final String FETCH_OBJ_TYPE =
      "SELECT "
          + COL_OBJ_TYPE
          + " FROM "
          + TABLE_OBJS
          + " WHERE "
          + COL_REPO_ID
          + "=? AND "
          + COL_OBJ_ID
          + " IN (?)";

  static final String FIND_OBJS =
      "SELECT "
          + COLS_OBJS_ALL_NAMES
          + " FROM "
          + TABLE_OBJS
          + " WHERE "
          + COL_REPO_ID
          + "=? AND "
          + COL_OBJ_ID
          + " IN (?)";

  static final String STORE_OBJ =
      "INSERT INTO "
          + TABLE_OBJS
          + " ("
          + COL_REPO_ID
          + ", "
          + COLS_OBJS_ALL_NAMES
          + ") VALUES (?, ?, ?, ?, ?, ?)";

  static final String FIND_OBJS_TYPED = FIND_OBJS + " AND " + COL_OBJ_TYPE + "=?";

  static final String SCAN_OBJS_ALL =
      "SELECT " + COLS_OBJS_ALL_NAMES + " FROM " + TABLE_OBJS + " WHERE " + COL_REPO_ID + "=?";

  static final String SCAN_OBJS = SCAN_OBJS_ALL + " AND " + COL_OBJ_TYPE + " IN (?)";

  private SqlConstants() {}
}
