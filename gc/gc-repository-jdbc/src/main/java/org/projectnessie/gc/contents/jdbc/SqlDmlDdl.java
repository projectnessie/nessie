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
package org.projectnessie.gc.contents.jdbc;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.intellij.lang.annotations.Language;

final class SqlDmlDdl {

  private SqlDmlDdl() {}

  static final int ERROR_LENGTH = 2000;

  @Language("SQL")
  static final String CREATE_LIVE_SETS =
      "CREATE TABLE gc_live_sets (\n"
          + "    live_set_id VARCHAR(40), \n"
          + "    set_status VARCHAR(40), \n"
          + "    identify_started TIMESTAMP, \n"
          + "    identify_finished TIMESTAMP, \n"
          + "    expire_started TIMESTAMP, \n"
          + "    expire_finished TIMESTAMP, \n"
          + "    error_message VARCHAR("
          + ERROR_LENGTH
          + "), \n"
          + "    PRIMARY KEY (live_set_id))";

  @Language("SQL")
  static final String CREATE_LIVE_SET_CONTENTS =
      "CREATE TABLE gc_live_set_contents (\n"
          + "    live_set_id VARCHAR(40), \n"
          + "    content_id VARCHAR(200), \n"
          + "    commit_id VARCHAR(100), \n"
          + "    content_key VARCHAR(500), \n"
          + "    content_type VARCHAR(40), \n"
          + "    metadata_location VARCHAR(1000), \n"
          + "    snapshot_id BIGINT, \n"
          + "    PRIMARY KEY(live_set_id, content_id, commit_id))";

  @Language("SQL")
  static final String CREATE_LIVE_SET_LOCATIONS =
      "CREATE TABLE gc_live_set_content_locations (\n"
          + "    live_set_id VARCHAR(40), \n"
          + "    content_id VARCHAR(200), \n"
          + "    base_location VARCHAR(500), \n"
          + "    PRIMARY KEY (live_set_id, content_id, base_location))";

  @Language("SQL")
  static final String CREATE_FILE_DELETIONS =
      "CREATE TABLE gc_file_deletions (\n"
          + "    live_set_id VARCHAR(40), \n"
          + "    base_uri VARCHAR(250), \n"
          + "    path_uri VARCHAR(250), \n"
          + "    modification_timestamp BIGINT, \n"
          + "    PRIMARY KEY (live_set_id, base_uri, path_uri))";

  @Language("SQL")
  static final String INSERT_FILE_DELETIONS =
      "INSERT INTO gc_file_deletions \n"
          + "    (live_set_id, base_uri, path_uri, modification_timestamp) VALUES (?, ?, ?, ?)";

  @Language("SQL")
  static final String SELECT_FILE_DELETIONS =
      "SELECT base_uri, path_uri, modification_timestamp \n"
          + "    FROM gc_file_deletions \n"
          + "    WHERE live_set_id = ? \n"
          + "    ORDER BY base_uri";

  @Language("SQL")
  static final String DELETE_FILE_DELETIONS = "DELETE FROM gc_file_deletions WHERE live_set_id = ?";

  @Language("SQL")
  static final String SELECT_LIVE_CONTENT_SET =
      "SELECT live_set_id, set_status, identify_started, identify_finished, expire_started, expire_finished, error_message \n"
          + "    FROM gc_live_sets \n"
          + "    WHERE live_set_id = ?";

  @Language("SQL")
  static final String SELECT_ALL_LIVE_CONTENT_SETS =
      "SELECT live_set_id, set_status, identify_started, identify_finished, expire_started, expire_finished, error_message \n"
          + "    FROM gc_live_sets";

  @Language("SQL")
  static final String DELETE_LIVE_CONTENTS =
      "DELETE FROM gc_live_set_contents WHERE live_set_id = ?";

  @Language("SQL")
  static final String DELETE_LIVE_CONTENT_SET = "DELETE FROM gc_live_sets WHERE live_set_id = ?";

  @Language("SQL")
  static final String DELETE_LIVE_SET_LOCATIONS =
      "DELETE FROM gc_live_set_content_locations WHERE live_set_id = ?";

  @Language("SQL")
  static final String INSERT_CONTENT_LOCATION =
      "INSERT INTO gc_live_set_content_locations \n"
          + "    (live_set_id, content_id, base_location) VALUES (?, ?, ?)";

  @Language("SQL")
  static final String SELECT_CONTENT_LOCATION =
      "SELECT base_location \nFROM gc_live_set_content_locations \n"
          + "    WHERE live_set_id = ? AND content_id = ?";

  @Language("SQL")
  static final String SELECT_CONTENT_LOCATION_ALL =
      "SELECT base_location \nFROM gc_live_set_content_locations \n" + "    WHERE live_set_id = ?";

  @Language("SQL")
  static final String SELECT_CONTENT_IDS =
      "SELECT DISTINCT content_id \nFROM gc_live_set_contents \n" + "    WHERE live_set_id = ?";

  @Language("SQL")
  static final String SELECT_CONTENT_COUNT =
      "SELECT COUNT(DISTINCT content_id) \n"
          + "    FROM gc_live_set_contents \n"
          + "    WHERE live_set_id = ?";

  @Language("SQL")
  static final String START_IDENTIFY =
      "INSERT INTO gc_live_sets \n"
          + "    (live_set_id, identify_started, set_status) \n"
          + "    VALUES (?, ?, ?)";

  @Language("SQL")
  static final String FINISH_IDENTIFY =
      "UPDATE gc_live_sets \n"
          + "    SET identify_finished = ?, set_status = ?, error_message = ? \n"
          + "    WHERE live_set_id = ? AND set_status = ?";

  @Language("SQL")
  static final String START_EXPIRE =
      "UPDATE gc_live_sets \n"
          + "    SET expire_started = ?, set_status = ? \n"
          + "    WHERE live_set_id = ? AND set_status = ?";

  @Language("SQL")
  static final String FINISH_EXPIRE =
      "UPDATE gc_live_sets \n"
          + "    SET expire_finished = ?, set_status = ?, error_message = ? \n"
          + "    WHERE live_set_id = ? AND set_status = ?";

  @Language("SQL")
  static final String ADD_CONTENT =
      "INSERT INTO gc_live_set_contents \n"
          + "    (live_set_id, content_id, commit_id, content_key, content_type, metadata_location, snapshot_id) \n"
          + "    VALUES (?, ?, ?, ?, ?, ?, ?)";

  @Language("SQL")
  static final String SELECT_CONTENT_REFERENCES =
      "SELECT content_id, commit_id, content_key, content_type, metadata_location, snapshot_id \n"
          + "    FROM gc_live_set_contents \n"
          + "    WHERE live_set_id = ? AND content_id = ?";

  static final Map<String, String> ALL_CREATES =
      ImmutableMap.of(
          "gc_live_sets", CREATE_LIVE_SETS,
          "gc_live_set_contents", CREATE_LIVE_SET_CONTENTS,
          "gc_live_set_content_locations", CREATE_LIVE_SET_LOCATIONS,
          "gc_file_deletions", CREATE_FILE_DELETIONS);
}
