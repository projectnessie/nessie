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

package com.dremio.nessie.versioned.store.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Used to apply bind-variables of prepared statements at their calculated positions.
 */
@FunctionalInterface
interface ValueApplicator {

  /**
   * Implementation shall set its bind-variable using the provided {@link PreparedStatement}
   * at the given {@code index} and return the number of bound variables.
   *
   * @param pstmt prepared statement to populate the bind-variable to
   * @param index (start-)index
   * @return number of bound variables
   * @throws SQLException in case a failure happens
   */
  int set(PreparedStatement pstmt, int index) throws SQLException;
}
