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
package org.projectnessie.versioned.persist.tx.postgres;

import java.sql.SQLException;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapterConfig;
import org.projectnessie.versioned.persist.tests.extension.TestConnectionProviderSource;
import org.projectnessie.versioned.persist.tx.TxConnectionProvider;
import org.projectnessie.versioned.persist.tx.local.ImmutableLocalTxConnectionConfig;
import org.projectnessie.versioned.persist.tx.local.LocalConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

abstract class ContainerTestConnectionProviderSource
    implements TestConnectionProviderSource<TxConnectionProvider<?>> {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(ContainerTestConnectionProviderSource.class);

  private JdbcDatabaseContainer<?> container;
  private LocalConnectionProvider connectionProvider;

  @Override
  public void start() throws SQLException {
    container = createContainer().withLogConsumer(new Slf4jLogConsumer(LOGGER));
    container.start();

    connectionProvider = new LocalConnectionProvider();
    connectionProvider.configure(
        ImmutableLocalTxConnectionConfig.builder()
            .jdbcUrl(container.getJdbcUrl())
            .jdbcUser(container.getUsername())
            .jdbcPass(container.getPassword())
            .build());
    connectionProvider.initialize();
  }

  @Override
  public void stop() throws Exception {
    try {
      if (connectionProvider != null) {
        connectionProvider.close();
      }
    } finally {
      connectionProvider = null;
      try {
        if (container != null) {
          container.stop();
        }
      } finally {
        container = null;
      }
    }
  }

  @Override
  public DatabaseAdapterConfig<TxConnectionProvider<?>> updateConfig(
      DatabaseAdapterConfig<TxConnectionProvider<?>> config) {
    return config.withConnectionProvider(connectionProvider);
  }

  protected abstract JdbcDatabaseContainer<?> createContainer();
}
