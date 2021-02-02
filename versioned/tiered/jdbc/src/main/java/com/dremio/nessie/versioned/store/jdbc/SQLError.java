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

import java.sql.SQLException;

/**
 * Wraps a {@link SQLException} as a runtime-exception to eliminate some boilerplate try-catch
 * between JDBC-API call-sites and the Store-implementation.
 */
class SQLError extends RuntimeException {

  SQLError(SQLException cause) {
    super(cause);
  }

  @FunctionalInterface
  interface HandleVoidSQL {
    void handle() throws SQLException;
  }

  static void run(HandleVoidSQL code) {
    try {
      code.handle();
    } catch (SQLException e) {
      throw new SQLError(e);
    }
  }

  @FunctionalInterface
  interface HandleSQL<R> {
    R handle() throws SQLException;
  }

  static <R> R call(HandleSQL<R> code) {
    try {
      return code.handle();
    } catch (SQLException e) {
      throw new SQLError(e);
    }
  }
}
