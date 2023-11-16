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
package org.projectnessie.versioned.storage.cassandra;

final class CassandraConstants {

  static final int SELECT_BATCH_SIZE = 20;
  static final int MAX_CONCURRENT_BATCH_READS = 20;
  static final int MAX_CONCURRENT_DELETES = 20;
  static final int MAX_CONCURRENT_STORES = 20;

  static final String TABLE_REFS = "refs";
  static final String TABLE_OBJS = "objs";
  static final String COL_REPO_ID = "repo";

  static final String COL_OBJ_ID = "obj_id";
  static final String DELETE_OBJ =
      "DELETE FROM %s." + TABLE_OBJS + " WHERE " + COL_REPO_ID + "=? AND " + COL_OBJ_ID + "=?";
  static final String COL_OBJ_TYPE = "obj_type";

  static final String COLS_COMMIT =
      "c_created, c_seq, c_message, c_headers, c_reference_index, c_reference_index_stripes, c_tail, c_secondary_parents, c_incremental_index, c_incomplete_index, c_commit_type";
  static final String COLS_REF = "r_name, r_initial_pointer, r_created_at, r_extended_info";
  static final String COLS_VALUE = "v_content_id, v_payload, v_data";
  static final String COLS_SEGMENTS = "i_stripes";
  static final String COLS_INDEX = "i_index";
  static final String COLS_TAG = "t_message, t_headers, t_signature";
  static final String COLS_STRING =
      "s_content_type, s_compression, s_filename, s_predecessors, s_text";

  static final String INSERT_OBJ_PREFIX =
      "INSERT INTO %s."
          + TABLE_OBJS
          + " ("
          + COL_REPO_ID
          + ", "
          + COL_OBJ_ID
          + ", "
          + COL_OBJ_TYPE
          + ", ";
  static final String INSERT_OBJ_STRING =
      INSERT_OBJ_PREFIX + COLS_STRING + ") VALUES (?,?,?, ?,?,?,?,?)";
  static final String INSERT_OBJ_TAG = INSERT_OBJ_PREFIX + COLS_TAG + ") VALUES (?,?,?, ?,?,?)";
  static final String INSERT_OBJ_INDEX = INSERT_OBJ_PREFIX + COLS_INDEX + ") VALUES (?,?,?, ?)";
  static final String INSERT_OBJ_SEGMENTS =
      INSERT_OBJ_PREFIX + COLS_SEGMENTS + ") VALUES (?,?,?, ?)";
  static final String INSERT_OBJ_VALUE = INSERT_OBJ_PREFIX + COLS_VALUE + ") VALUES (?,?,?, ?,?,?)";
  static final String INSERT_OBJ_REF = INSERT_OBJ_PREFIX + COLS_REF + ") VALUES (?,?,?, ?,?,?,?)";
  static final String INSERT_OBJ_COMMIT =
      INSERT_OBJ_PREFIX + COLS_COMMIT + ") VALUES (?,?,?, ?,?,?,?,?,?,?,?,?,?,?)";

  static final String CREATE_TABLE_OBJS =
      "CREATE TABLE %s."
          + TABLE_OBJS
          + " (\n    "
          + COL_REPO_ID
          + " {0}, "
          + COL_OBJ_ID
          + " {1}, "
          + COL_OBJ_TYPE
          + " {0}"
          + ",\n    c_created {5}, c_seq {5}, c_message {6}, c_headers {4}, c_reference_index {1}, c_reference_index_stripes {4}, c_tail {2}, c_secondary_parents {2}, c_incremental_index {4}, c_incomplete_index {3}, c_commit_type {0}"
          + ",\n    r_name {0}, r_initial_pointer {1}, r_created_at {5}, r_extended_info {1}"
          + ",\n    v_content_id {0}, v_payload {7}, v_data {4}"
          + ",\n    i_stripes {4}"
          + ",\n    i_index {4}"
          + ",\n    t_message {6}, t_headers {4}, t_signature {4}"
          + ",\n    s_content_type {0}, s_compression {0}, s_filename {0}, s_predecessors {2}, s_text {4}"
          + ",\n    PRIMARY KEY (("
          + COL_REPO_ID
          + ", "
          + COL_OBJ_ID
          + "))\n  )";
  static final String COL_REFS_NAME = "ref_name";
  static final String COL_REFS_POINTER = "pointer";
  static final String COL_REFS_DELETED = "deleted";
  static final String COL_REFS_CREATED_AT = "created_at";
  static final String COL_REFS_EXTENDED_INFO = "ext_info";
  static final String COL_REFS_PREVIOUS = "prev_ptr";
  static final String UPDATE_REFERENCE_POINTER =
      "UPDATE %s."
          + TABLE_REFS
          + " SET "
          + COL_REFS_POINTER
          + "=?, "
          + COL_REFS_PREVIOUS
          + "=? WHERE "
          + COL_REPO_ID
          + "=? AND "
          + COL_REFS_NAME
          + "=? IF "
          + COL_REFS_POINTER
          + "=? AND "
          + COL_REFS_DELETED
          + "=? AND "
          + COL_REFS_CREATED_AT
          + "=? AND "
          + COL_REFS_EXTENDED_INFO
          + "=?";
  static final String PURGE_REFERENCE =
      "DELETE FROM %s."
          + TABLE_REFS
          + " WHERE "
          + COL_REPO_ID
          + "=? AND "
          + COL_REFS_NAME
          + "=? IF "
          + COL_REFS_POINTER
          + "=? AND "
          + COL_REFS_DELETED
          + "=? AND "
          + COL_REFS_CREATED_AT
          + "=? AND "
          + COL_REFS_EXTENDED_INFO
          + "=?";
  static final String MARK_REFERENCE_AS_DELETED =
      "UPDATE %s."
          + TABLE_REFS
          + " SET "
          + COL_REFS_DELETED
          + "=? WHERE "
          + COL_REPO_ID
          + "=? AND "
          + COL_REFS_NAME
          + "=? IF "
          + COL_REFS_POINTER
          + "=? AND "
          + COL_REFS_DELETED
          + "=? AND "
          + COL_REFS_CREATED_AT
          + "=? AND "
          + COL_REFS_EXTENDED_INFO
          + "=?";
  static final String ADD_REFERENCE =
      "INSERT INTO %s."
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
          + ") VALUES (?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS";
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
          + " FROM %s."
          + TABLE_REFS
          + " WHERE "
          + COL_REPO_ID
          + "=? AND "
          + COL_REFS_NAME
          + " IN ?";
  static final String CREATE_TABLE_REFS =
      "CREATE TABLE %s."
          + TABLE_REFS
          + "\n  (\n    "
          + COL_REPO_ID
          + " {0}, "
          + COL_REFS_NAME
          + " {0}, "
          + COL_REFS_POINTER
          + " {1}, "
          + COL_REFS_DELETED
          + " {3}, "
          + COL_REFS_CREATED_AT
          + " {5}, "
          + COL_REFS_EXTENDED_INFO
          + " {1}, "
          + COL_REFS_PREVIOUS
          + " {4}, "
          + "\n    PRIMARY KEY (("
          + COL_REPO_ID
          + ", "
          + COL_REFS_NAME
          + "))\n  )";
  static final String COLS_OBJS_ALL =
      COL_OBJ_ID
          + ", "
          + COL_OBJ_TYPE
          + ", "
          + COLS_COMMIT
          + ", "
          + COLS_REF
          + ", "
          + COLS_VALUE
          + ", "
          + COLS_SEGMENTS
          + ", "
          + COLS_INDEX
          + ", "
          + COLS_TAG
          + ", "
          + COLS_STRING;
  static final int COL_COMMIT_CREATED = 2; // obj_id + obj_type before this column
  static final int COL_COMMIT_SEQ = COL_COMMIT_CREATED + 1;
  static final int COL_COMMIT_MESSAGE = COL_COMMIT_SEQ + 1;
  static final int COL_COMMIT_HEADERS = COL_COMMIT_MESSAGE + 1;
  static final int COL_COMMIT_REFERENCE_INDEX = COL_COMMIT_HEADERS + 1;
  static final int COL_COMMIT_REFERENCE_INDEX_STRIPES = COL_COMMIT_REFERENCE_INDEX + 1;
  static final int COL_COMMIT_TAIL = COL_COMMIT_REFERENCE_INDEX_STRIPES + 1;
  static final int COL_COMMIT_SECONDARY_PARENTS = COL_COMMIT_TAIL + 1;
  static final int COL_COMMIT_INCREMENTAL_INDEX = COL_COMMIT_SECONDARY_PARENTS + 1;
  static final int COL_COMMIT_INCOMPLETE_INDEX = COL_COMMIT_INCREMENTAL_INDEX + 1;
  static final int COL_COMMIT_TYPE = COL_COMMIT_INCOMPLETE_INDEX + 1;
  static final int COL_REF_NAME = COL_COMMIT_TYPE + 1;
  static final int COL_REF_INITIAL_POINTER = COL_REF_NAME + 1;
  static final int COL_REF_CREATED_AT = COL_REF_INITIAL_POINTER + 1;
  static final int COL_REF_EXTENDED_INFO = COL_REF_CREATED_AT + 1;
  static final int COL_VALUE_CONTENT_ID = COL_REF_EXTENDED_INFO + 1;
  static final int COL_VALUE_PAYLOAD = COL_VALUE_CONTENT_ID + 1;
  static final int COL_VALUE_DATA = COL_VALUE_PAYLOAD + 1;
  static final int COL_SEGMENTS_STRIPES = COL_VALUE_DATA + 1;
  static final int COL_INDEX_INDEX = COL_SEGMENTS_STRIPES + 1;
  static final int COL_TAG_MESSAGE = COL_INDEX_INDEX + 1;
  static final int COL_TAG_HEADERS = COL_TAG_MESSAGE + 1;
  static final int COL_TAG_SIGNATURE = COL_TAG_HEADERS + 1;
  static final int COL_STRING_CONTENT_TYPE = COL_TAG_SIGNATURE + 1;
  static final int COL_STRING_COMPRESSION = COL_STRING_CONTENT_TYPE + 1;
  static final int COL_STRING_FILENAME = COL_STRING_COMPRESSION + 1;
  static final int COL_STRING_PREDECESSORS = COL_STRING_FILENAME + 1;
  static final int COL_STRING_TEXT = COL_STRING_PREDECESSORS + 1;

  static final String FETCH_OBJ_TYPE =
      "SELECT "
          + COL_OBJ_TYPE
          + " FROM %s."
          + TABLE_OBJS
          + " WHERE "
          + COL_REPO_ID
          + "=? AND "
          + COL_OBJ_ID
          + " IN ?";

  static final String FIND_OBJS =
      "SELECT "
          + COLS_OBJS_ALL
          + " FROM %s."
          + TABLE_OBJS
          + " WHERE "
          + COL_REPO_ID
          + "=? AND "
          + COL_OBJ_ID
          + " IN ?";

  static final String SCAN_OBJS =
      "SELECT "
          + COLS_OBJS_ALL
          + " FROM %s."
          + TABLE_OBJS
          + " WHERE "
          + COL_REPO_ID
          + "=? ALLOW FILTERING";

  static final String ERASE_OBJS_SCAN =
      "SELECT "
          + COL_REPO_ID
          + ", "
          + COL_OBJ_ID
          + " FROM %s."
          + TABLE_OBJS
          + " WHERE "
          + COL_REPO_ID
          + " IN ? ALLOW FILTERING";
  static final String ERASE_REFS_SCAN =
      "SELECT "
          + COL_REPO_ID
          + ", "
          + COL_REFS_NAME
          + " FROM %s."
          + TABLE_REFS
          + " WHERE "
          + COL_REPO_ID
          + " IN ? ALLOW FILTERING";

  static final String ERASE_OBJ = DELETE_OBJ;
  static final String ERASE_REF =
      "DELETE FROM %s."
          + TABLE_REFS
          + " WHERE "
          + COL_REPO_ID
          + " = ? AND "
          + COL_REFS_NAME
          + " = ?";

  private CassandraConstants() {}
}
