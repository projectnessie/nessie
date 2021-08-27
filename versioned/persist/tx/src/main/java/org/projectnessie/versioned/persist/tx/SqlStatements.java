/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie.versioned.persist.tx;

public final class SqlStatements {

  public static final String TABLE_NAMED_REFERENCES = "named_refs";
  public static final String DELETE_NAMED_REFERENCE_ALL =
      String.format("DELETE FROM %s WHERE key_prefix = ?", TABLE_NAMED_REFERENCES);
  public static final String UPDATE_NAMED_REFERENCE =
      String.format(
          "UPDATE %s SET hash = ? WHERE key_prefix = ? AND ref = ? AND hash = ?",
          TABLE_NAMED_REFERENCES);
  public static final String INSERT_NAMED_REFERENCE =
      String.format(
          "INSERT INTO %s (key_prefix, ref, ref_type, hash) VALUES (?, ?, ?, ?)",
          TABLE_NAMED_REFERENCES);
  public static final String DELETE_NAMED_REFERENCE =
      String.format(
          "DELETE FROM %s WHERE key_prefix = ? AND ref = ? AND hash = ?", TABLE_NAMED_REFERENCES);
  public static final String SELECT_NAMED_REFERENCES =
      String.format(
          "SELECT ref_type, ref, hash FROM %s WHERE key_prefix = ?", TABLE_NAMED_REFERENCES);
  public static final String SELECT_NAMED_REFERENCE_NAME =
      String.format("SELECT hash FROM %s WHERE key_prefix = ? AND ref = ?", TABLE_NAMED_REFERENCES);
  public static final String SELECT_NAMED_REFERENCE =
      String.format(
          "SELECT hash FROM %s WHERE key_prefix = ? AND ref = ? AND ref_type = ?",
          TABLE_NAMED_REFERENCES);
  public static final String CREATE_TABLE_NAMED_REFERENCES =
      String.format(
          "CREATE TABLE %s (\n"
              + "  key_prefix {2},\n"
              + "  ref {4},\n"
              + "  ref_type {5},\n"
              + "  hash {1},\n"
              + "  PRIMARY KEY (key_prefix, ref)\n"
              + ")",
          TABLE_NAMED_REFERENCES);

  public static final String TABLE_GLOBAL_STATE = "global_state";
  public static final String DELETE_GLOBAL_STATE_ALL =
      String.format("DELETE FROM %s WHERE key_prefix = ?", TABLE_GLOBAL_STATE);
  public static final String UPDATE_GLOBAL_STATE_UNCOND =
      String.format(
          "UPDATE %s SET value = ?, chksum = ?, created_at = ? WHERE key_prefix = ? AND cid = ?",
          TABLE_GLOBAL_STATE);
  public static final String UPDATE_GLOBAL_STATE =
      String.format(
          "UPDATE %s SET value = ?, chksum = ?, created_at = ? WHERE key_prefix = ? AND cid = ? AND chksum = ?",
          TABLE_GLOBAL_STATE);
  public static final String INSERT_GLOBAL_STATE =
      String.format(
          "INSERT INTO %s (key_prefix, cid, chksum, value, created_at) VALUES (?, ?, ?, ?, ?)",
          TABLE_GLOBAL_STATE);
  public static final String SELECT_GLOBAL_STATE_MANY_WITH_LOGS =
      String.format(
          "SELECT cid, value, created_at FROM %s WHERE key_prefix = ? AND cid IN (%%s)",
          TABLE_GLOBAL_STATE);
  public static final String SELECT_GLOBAL_STATE_ALL =
      String.format("SELECT cid, value FROM %s WHERE key_prefix = ?", TABLE_GLOBAL_STATE);
  public static final String SELECT_GLOBAL_STATE_MANY =
      String.format(
          "SELECT cid, value FROM %s WHERE key_prefix = ? AND cid IN (%%s)", TABLE_GLOBAL_STATE);
  public static final String CREATE_TABLE_GLOBAL_STATE =
      String.format(
          "CREATE TABLE %s (\n"
              + "  key_prefix {2},\n"
              + "  cid {6},\n"
              + "  chksum {1},\n"
              + "  value {0},\n"
              + "  created_at {7},\n"
              + "  PRIMARY KEY (key_prefix, cid)\n"
              + ")",
          TABLE_GLOBAL_STATE);

  public static final String TABLE_COMMIT_LOG = "commit_log";
  public static final String DELETE_COMMIT_LOG_ALL =
      String.format("DELETE FROM %s WHERE key_prefix = ?", TABLE_COMMIT_LOG);
  public static final String INSERT_COMMIT_LOG =
      String.format("INSERT INTO %s (key_prefix, hash, value) VALUES (?, ?, ?)", TABLE_COMMIT_LOG);
  public static final String SELECT_COMMIT_LOG_MANY =
      String.format(
          "SELECT value FROM %s WHERE key_prefix = ? AND hash IN (%%s)", TABLE_COMMIT_LOG);
  public static final String SELECT_COMMIT_LOG =
      String.format("SELECT value FROM %s WHERE key_prefix = ? AND hash = ?", TABLE_COMMIT_LOG);
  public static final String CREATE_TABLE_COMMIT_LOG =
      String.format(
          "CREATE TABLE %s (\n"
              + "  key_prefix {2},\n"
              + "  hash {1},\n"
              + "  value {0},\n"
              + "  PRIMARY KEY (key_prefix, hash)\n"
              + ")",
          TABLE_COMMIT_LOG);

  public static final String TABLE_KEY_LIST = "key_list";
  public static final String DELETE_KEY_LIST_ALL =
      String.format("DELETE FROM %s WHERE key_prefix = ?", TABLE_KEY_LIST);
  public static final String SELECT_KEY_LIST_MANY =
      String.format(
          "SELECT id, value FROM %s WHERE key_prefix = ? AND id IN (%%s)", TABLE_KEY_LIST);
  public static final String INSERT_KEY_LIST =
      String.format("INSERT INTO %s (key_prefix, id, value) VALUES (?, ?, ?)", TABLE_KEY_LIST);
  public static final String CREATE_TABLE_KEY_LIST =
      String.format(
          "CREATE TABLE %s (\n"
              + "  key_prefix {2},\n"
              + "  id {1},\n"
              + "  value {0},\n"
              + "  PRIMARY KEY (key_prefix, id)\n"
              + ")",
          TABLE_KEY_LIST);

  private SqlStatements() {}
}
