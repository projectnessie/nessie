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
package org.projectnessie.versioned.persist.tx;

import java.sql.Connection;
import java.util.function.Supplier;

public final class ConnectionWrapper implements AutoCloseable {
  private static final ThreadLocal<ConnectionWrapper> INHERITED = new ThreadLocal<>();

  private final Connection connection;
  private int useCount;

  public ConnectionWrapper(Connection connection) {
    this.connection = connection;
  }

  public static ConnectionWrapper borrow(Supplier<Connection> newConnectionProducer) {
    ConnectionWrapper current = INHERITED.get();
    if (current != null) {
      current.useCount++;
    } else {
      current = new ConnectionWrapper(newConnectionProducer.get());
      INHERITED.set(current);
    }
    return current;
  }

  public static boolean threadHasOpenConnection() {
    return INHERITED.get() != null;
  }

  public Connection conn() {
    return connection;
  }

  public void commit() {
    try {
      connection.commit();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void rollback() {
    try {
      connection.rollback();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    if (useCount > 0) {
      useCount--;
      return;
    }
    forceClose();
  }

  public void forceClose() {
    try {
      try {
        connection.rollback();
      } finally {
        INHERITED.set(null);
        connection.close();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
