/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.versioned.storage.spanner;

final class SpannerConstants {

  static final String TABLE_REFS = "refs";
  static final String TABLE_OBJS = "objs";
  static final String COL_REPO_ID = "repo";
  static final String ERASE_OBJS =
      "DELETE FROM " + TABLE_OBJS + " WHERE " + COL_REPO_ID + " IN UNNEST (@RepoIds)";
  static final String ERASE_REFS =
      "DELETE FROM " + TABLE_REFS + " WHERE " + COL_REPO_ID + " IN UNNEST (@RepoIds)";
  static final String COL_OBJ_ID = "obj_id";
  static final String DELETE_OBJS =
      "DELETE FROM "
          + TABLE_OBJS
          + " WHERE "
          + COL_REPO_ID
          + "=@RepoId AND "
          + COL_OBJ_ID
          + " IN UNNEST (@Ids)";
  static final String COL_OBJ_TYPE = "obj_type";

  static final String COL_COMMIT_CREATED = "c_created";
  static final String COL_COMMIT_SEQ = "c_seq";
  static final String COL_COMMIT_MESSAGE = "c_message";
  static final String COL_COMMIT_HEADERS = "c_headers";
  static final String COL_COMMIT_REFERENCE_INDEX = "c_reference_index";
  static final String COL_COMMIT_REFERENCE_INDEX_STRIPES = "c_reference_index_stripes";
  static final String COL_COMMIT_TAIL = "c_tail";
  static final String COL_COMMIT_SECONDARY_PARENTS = "c_secondary_parents";
  static final String COL_COMMIT_INCREMENTAL_INDEX = "c_incremental_index";
  static final String COL_COMMIT_INCOMPLETE_INDEX = "c_incomplete_index";
  static final String COL_COMMIT_TYPE = "c_commit_type";
  static final String COL_REF_NAME = "r_name";
  static final String COL_REF_INITIAL_POINTER = "r_initial_pointer";
  static final String COL_REF_CREATED_AT = "r_created_at";
  static final String COL_VALUE_CONTENT_ID = "v_content_id";
  static final String COL_VALUE_PAYLOAD = "v_payload";
  static final String COL_VALUE_DATA = "v_data";
  static final String COL_SEGMENTS_STRIPES = "i_stripes";
  static final String COL_INDEX_INDEX = "i_index";
  static final String COL_TAG_COMMIT_ID = "t_commit_id";
  static final String COL_TAG_MESSAGE = "t_message";
  static final String COL_TAG_HEADERS = "t_headers";
  static final String COL_TAG_SIGNATURE = "t_signature";
  static final String COL_STRING_CONTENT_TYPE = "s_content_type";
  static final String COL_STRING_COMPRESSION = "s_compression";
  static final String COL_STRING_FILENAME = "s_filename";
  static final String COL_STRING_PREDECESSORS = "s_predecessors";
  static final String COL_STRING_TEXT = "s_text";

  static final String COLS_COMMIT =
      "c_created, c_seq, c_message, c_headers, c_reference_index, c_reference_index_stripes, c_tail, c_secondary_parents, c_incremental_index, c_incomplete_index, c_commit_type";
  static final String COLS_REF = "r_name, r_initial_pointer, r_created_at";
  static final String COLS_VALUE = "v_content_id, v_payload, v_data";
  static final String COLS_SEGMENTS = "i_stripes";
  static final String COLS_INDEX = "i_index";
  static final String COLS_TAG = "t_commit_id, t_message, t_headers, t_signature";
  static final String COLS_STRING =
      COL_STRING_CONTENT_TYPE
          + ", "
          + COL_STRING_COMPRESSION
          + ", "
          + COL_STRING_FILENAME
          + ", "
          + COL_STRING_PREDECESSORS
          + ", "
          + COL_STRING_TEXT;

  static final String CREATE_TABLE_OBJS =
      "CREATE TABLE "
          + TABLE_OBJS
          + " (\n    "
          + COL_REPO_ID
          + " STRING(128) NOT NULL, "
          + COL_OBJ_ID
          + " STRING(256) NOT NULL, "
          + COL_OBJ_TYPE
          + " STRING(32) NOT NULL"
          + ",\n    c_created INT64, c_seq INT64, c_message STRING(MAX), c_headers BYTES(MAX), c_reference_index STRING(256), c_reference_index_stripes BYTES(MAX), c_tail ARRAY<STRING(256)>, c_secondary_parents ARRAY<STRING(256)>, c_incremental_index BYTES(MAX), c_incomplete_index BOOL, c_commit_type STRING(32)"
          + ",\n    r_name STRING(512), r_initial_pointer STRING(256), r_created_at INT64"
          + ",\n    v_content_id STRING(128), v_payload INT64, v_data BYTES(MAX)"
          + ",\n    i_stripes BYTES(MAX)"
          + ",\n    i_index BYTES(MAX)"
          + ",\n    t_commit_id STRING(256), t_message STRING(MAX), t_headers BYTES(MAX), t_signature BYTES(MAX)"
          + ",\n    s_content_type STRING(256), s_compression STRING(32), s_filename STRING(512), s_predecessors ARRAY<STRING(256)>, s_text BYTES(MAX)"
          + "\n  ) PRIMARY KEY ("
          + COL_REPO_ID
          + ", "
          + COL_OBJ_ID
          + ")\n";
  static final String COL_REFS_NAME = "ref_name";
  static final String COL_REFS_POINTER = "pointer";
  static final String COL_REFS_DELETED = "deleted";
  static final String MARK_REFERENCE_AS_DELETED =
      "UPDATE "
          + TABLE_REFS
          + " SET "
          + COL_REFS_DELETED
          + "=true WHERE "
          + COL_REPO_ID
          + "=@RepoID AND "
          + COL_REFS_NAME
          + "=@Name AND "
          + COL_REFS_POINTER
          + "=@Pointer AND "
          + COL_REFS_DELETED
          + "=false";
  static final String UPDATE_REFERENCE_POINTER =
      "UPDATE "
          + TABLE_REFS
          + " SET "
          + COL_REFS_POINTER
          + "=@NewPointer WHERE "
          + COL_REPO_ID
          + "=@RepoID AND "
          + COL_REFS_NAME
          + "=@Name AND "
          + COL_REFS_POINTER
          + "=@Pointer AND "
          + COL_REFS_DELETED
          + "=false";
  static final String PURGE_REFERENCE =
      "DELETE FROM "
          + TABLE_REFS
          + " WHERE "
          + COL_REPO_ID
          + "=@RepoID AND "
          + COL_REFS_NAME
          + "=@Name AND "
          + COL_REFS_POINTER
          + "=@Pointer AND "
          + COL_REFS_DELETED
          + "=true";
  static final String FIND_REFERENCES =
      "SELECT "
          + COL_REFS_NAME
          + ", "
          + COL_REFS_POINTER
          + ", "
          + COL_REFS_DELETED
          + " FROM "
          + TABLE_REFS
          + " WHERE "
          + COL_REPO_ID
          + "=@RepoID AND "
          + COL_REFS_NAME
          + " IN UNNEST (@Names)";
  static final String CREATE_TABLE_REFS =
      "CREATE TABLE "
          + TABLE_REFS
          + " (\n    "
          + COL_REPO_ID
          + " STRING(128) NOT NULL, "
          + COL_REFS_NAME
          + " STRING(512) NOT NULL, "
          + COL_REFS_POINTER
          + " STRING(256), "
          + COL_REFS_DELETED
          + " BOOL"
          + "\n  ) PRIMARY KEY ("
          + COL_REPO_ID
          + ", "
          + COL_REFS_NAME
          + ")\n";
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

  static final String FIND_OBJS =
      "SELECT "
          + COLS_OBJS_ALL
          + " FROM "
          + TABLE_OBJS
          + " WHERE "
          + COL_REPO_ID
          + "=@RepoID AND "
          + COL_OBJ_ID
          + " IN UNNEST (@Ids)";

  static final String CHECK_OBJS =
      "SELECT "
          + COL_OBJ_ID
          + " FROM "
          + TABLE_OBJS
          + " WHERE "
          + COL_REPO_ID
          + "=@RepoID AND "
          + COL_OBJ_ID
          + " IN UNNEST (@Ids)";

  static final String FIND_OBJS_TYPED = FIND_OBJS + " AND " + COL_OBJ_TYPE + "=@Type";

  static final String SCAN_OBJS =
      "SELECT "
          + COLS_OBJS_ALL
          + " FROM "
          + TABLE_OBJS
          + " WHERE "
          + COL_REPO_ID
          + "=@RepoID AND "
          + COL_OBJ_TYPE
          + " IN UNNEST (@Types)";

  private SpannerConstants() {}
}
