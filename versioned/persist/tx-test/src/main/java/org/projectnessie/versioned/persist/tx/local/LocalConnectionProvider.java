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
package org.projectnessie.versioned.persist.tx.local;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.AgroalConnectionFactoryConfiguration.TransactionIsolation;
import io.agroal.api.configuration.supplier.AgroalConnectionFactoryConfigurationSupplier;
import io.agroal.api.configuration.supplier.AgroalConnectionPoolConfigurationSupplier;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.api.security.NamePrincipal;
import io.agroal.api.security.SimplePassword;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import javax.sql.DataSource;
import org.projectnessie.versioned.persist.tx.TxConnectionProvider;

public class LocalConnectionProvider extends TxConnectionProvider<LocalTxConnectionConfig> {

  private DataSource dataSource;

  @Override
  public void initialize() throws SQLException {
    if (dataSource == null) {
      AgroalDataSourceConfigurationSupplier dataSourceConfiguration =
          new AgroalDataSourceConfigurationSupplier();
      AgroalConnectionPoolConfigurationSupplier poolConfiguration =
          dataSourceConfiguration.connectionPoolConfiguration();
      AgroalConnectionFactoryConfigurationSupplier connectionFactoryConfiguration =
          poolConfiguration.connectionFactoryConfiguration();

      // configure pool
      poolConfiguration
          .initialSize(config.getPoolInitialSize())
          .maxSize(config.getPoolMaxSize())
          .minSize(config.getPoolMinSize())
          .maxLifetime(Duration.of(config.getPoolConnectionLifetimeMinutes(), ChronoUnit.MINUTES))
          .acquisitionTimeout(
              Duration.of(config.getPoolAcquisitionTimeoutSeconds(), ChronoUnit.SECONDS));

      // configure supplier
      connectionFactoryConfiguration.jdbcUrl(config.getJdbcUrl());
      if (config.getJdbcUser() != null) {
        connectionFactoryConfiguration.credential(new NamePrincipal(config.getJdbcUser()));
        connectionFactoryConfiguration.credential(new SimplePassword(config.getJdbcPass()));
      }
      connectionFactoryConfiguration.jdbcTransactionIsolation(
          TransactionIsolation.valueOf(config.getPoolTransactionIsolation()));
      connectionFactoryConfiguration.autoCommit(false);

      dataSource = AgroalDataSource.from(dataSourceConfiguration.get());
    }
  }

  @Override
  public void close() throws Exception {
    if (dataSource instanceof AutoCloseable) {
      try {
        ((AutoCloseable) dataSource).close();
      } finally {
        dataSource = null;
      }
    }
  }

  @Override
  public Connection borrowConnection() throws SQLException {
    return dataSource.getConnection();
  }
}
