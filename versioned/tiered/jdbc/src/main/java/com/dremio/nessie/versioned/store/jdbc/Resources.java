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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import javax.sql.DataSource;

import com.dremio.nessie.versioned.util.AutoCloseables;
import com.google.common.collect.Lists;

/**
 * Tracks {@link AutoCloseable}s used by JDBC resources.
 */
final class Resources implements AutoCloseable {

  private final List<AutoCloseable> closeables = new ArrayList<>();
  private final Connection connection;
  private boolean closed;

  Resources(DataSource dataSource) {
    this.connection = add(SQLError.call(dataSource::getConnection));
  }

  ResultSet query(String sql, Consumer<PreparedStatement> prepareStatement) {
    PreparedStatement preparedStatement = prepareStatement(sql);
    prepareStatement.accept(preparedStatement);
    return add(SQLError.call(preparedStatement::executeQuery));
  }

  PreparedStatement prepareStatement(String sql) {
    return add(SQLError.call(() -> connection.prepareStatement(sql)));
  }

  <R extends AutoCloseable> R add(R closeable) {
    closeables.add(closeable);
    return closeable;
  }

  Connection getConnection() {
    return connection;
  }

  boolean isClosed() {
    return closed;
  }

  @Override
  public void close() throws Exception {
    if (!closed) {
      closed = true;
      AutoCloseables.close(Lists.reverse(closeables));
    }
  }
}
