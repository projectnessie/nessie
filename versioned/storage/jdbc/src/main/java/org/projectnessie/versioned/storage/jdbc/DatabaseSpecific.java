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
import java.util.Map;

public interface DatabaseSpecific {
  /** H2 integrity constraint violation. */
  int CONSTRAINT_VIOLATION_SQL_CODE = 23505;

  /** Deadlock error, returned by Postgres. */
  String DEADLOCK_SQL_STATE_POSTGRES = "40P01";

  /**
   * Cockroach "retry, write too old" error, see <a
   * href="https://www.cockroachlabs.com/docs/v21.1/transaction-retry-error-reference.html#retry_write_too_old">Cockroach's
   * Transaction Retry Error Reference</a>, and Postgres may return a "deadlock" error.
   */
  String RETRY_SQL_STATE_COCKROACH = "40001";

  /** Postgres &amp; Cockroach integrity constraint violation. */
  String CONSTRAINT_VIOLATION_SQL_STATE = "23505";

  Map<JdbcColumnType, String> columnTypes();

  Map<JdbcColumnType, Integer> columnTypeIds();

  boolean isConstraintViolation(SQLException e);

  boolean isRetryTransaction(SQLException e);

  String wrapInsert(String sql);
}
