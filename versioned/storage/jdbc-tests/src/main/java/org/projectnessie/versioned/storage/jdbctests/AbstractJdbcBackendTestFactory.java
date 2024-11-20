/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.versioned.storage.jdbctests;

import static org.testcontainers.shaded.com.google.common.base.Preconditions.checkState;

import io.agroal.api.configuration.AgroalConnectionFactoryConfiguration;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.projectnessie.versioned.storage.common.persist.Backend;
import org.projectnessie.versioned.storage.jdbc.DatabaseSpecific;
import org.projectnessie.versioned.storage.jdbc.DatabaseSpecifics;
import org.projectnessie.versioned.storage.jdbc.JdbcBackend;
import org.projectnessie.versioned.storage.jdbc.JdbcBackendConfig;
import org.projectnessie.versioned.storage.testextension.BackendTestFactory;

public abstract class AbstractJdbcBackendTestFactory implements BackendTestFactory {

  public abstract String jdbcUrl();

  public abstract String jdbcUser();

  public abstract String jdbcPass();

  public AgroalConnectionFactoryConfiguration.TransactionIsolation transactionIsolation() {
    return AgroalConnectionFactoryConfiguration.TransactionIsolation.READ_COMMITTED;
  }

  @Override
  public Backend createNewBackend() throws SQLException {
    checkState(jdbcUrl() != null, "Must set JDBC URL first");

    DataSource dataSource =
        DataSourceProducer.builder()
            .jdbcUrl(jdbcUrl())
            .jdbcUser(jdbcUser())
            .jdbcPass(jdbcPass())
            .transactionIsolation(transactionIsolation())
            .build()
            .createNewDataSource();

    JdbcBackendConfig config = JdbcBackendConfig.builder().dataSource(dataSource).build();

    DatabaseSpecific databaseSpecific = DatabaseSpecifics.detect(dataSource);
    return new JdbcBackend(config, databaseSpecific, true);
  }

  @Override
  public void start() throws Exception {}

  @Override
  public void stop() throws Exception {}
}
