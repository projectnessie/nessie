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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Spliterators.AbstractSpliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Java {@link java.util.Spliterator} using JDBC {@link PrepareStatement} and {@link ResultSet},
 * handles JDBC exceptions and JDBC resources. Callers must always use the {@link Stream} returned
 * by {@link #toStream()}.
 *
 * @param <T> type deserialized from each {@link ResultSet} row
 */
class JdbcSelectSpliterator<T> extends AbstractSpliterator<T> {

  /**
   * Handler for result-set deserialization, leave exception-handling up to the caller.
   *
   * @param <T> deserialized type
   */
  @FunctionalInterface
  interface ResultSetMapper<T> {
    /**
     * Deserialize the given result set.
     *
     * @param rs JDBC result set
     * @return deserialized object
     */
    T apply(ResultSet rs) throws SQLException;
  }

  /** Wrapper function to prepare a statement, but leave exception-handling up to the caller. */
  @FunctionalInterface
  interface PrepareStatement {
    void accept(PreparedStatement ps) throws SQLException;
  }

  /**
   * Wraps a {@link Stream} around a JDBC {@link Connection} performing a SQL {@code SELECT}.
   *
   * <p>The {@link PreparedStatement} + {@link ResultSet} are created lazily and closed when the
   * {@link Stream} is closed.
   *
   * @param conn the JDBC connection to use
   * @param sql the SQL {@code SELECT}
   * @param prepareStatement function to populate the bind-variables on a {@link PreparedStatement}
   * @param deserializer maps the current row in a {@link ResultSet} to the type returned by the
   *     {@link Stream}
   * @return Java {@link Stream} wrapping a SQL query
   */
  static <T> Stream<T> buildStream(
      Connection conn,
      String sql,
      PrepareStatement prepareStatement,
      ResultSetMapper<T> deserializer) {
    return new JdbcSelectSpliterator<T>(conn, sql, prepareStatement, deserializer).toStream();
  }

  private boolean done = false;
  private final Connection connection;
  private PreparedStatement ps;
  private ResultSet rs;
  private final String sql;
  private final ResultSetMapper<T> deserializer;
  private final PrepareStatement prepareStatement;
  private final List<AutoCloseable> closeables = new ArrayList<>();

  private JdbcSelectSpliterator(
      Connection conn,
      String sql,
      PrepareStatement prepareStatement,
      ResultSetMapper<T> deserializer) {
    super(Long.MAX_VALUE, 0);
    this.connection = conn;
    this.sql = sql;
    this.prepareStatement = prepareStatement;
    this.deserializer = deserializer;
  }

  private Stream<T> toStream() {
    return StreamSupport.stream(this, false).onClose(this::closeResources);
  }

  private void closeResources() {
    Exception e = null;
    while (!closeables.isEmpty()) {
      AutoCloseable closeable = closeables.remove(closeables.size() - 1);
      try {
        closeable.close();
      } catch (Exception ex) {
        if (e == null) {
          e = ex;
        } else {
          e.addSuppressed(ex);
        }
      }
    }
    if (e != null) {
      if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      }
      throw new RuntimeException(e);
    }
  }

  @Override
  public final boolean tryAdvance(Consumer<? super T> consumer) {
    if (done) {
      return false;
    }

    try {
      if (ps == null) {
        ps = connection.prepareStatement(sql);
        closeables.add(ps);
        prepareStatement.accept(ps);
        rs = ps.executeQuery();
        closeables.add(rs);
      }

      if (!rs.next()) {
        done = true;
        closeResources();
        return false;
      }

      consumer.accept(deserializer.apply(rs));

      return true;
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
