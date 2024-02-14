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

import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.AgroalConnectionFactoryConfiguration.TransactionIsolation;
import io.agroal.api.configuration.supplier.AgroalConnectionFactoryConfigurationSupplier;
import io.agroal.api.configuration.supplier.AgroalConnectionPoolConfigurationSupplier;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.api.security.NamePrincipal;
import io.agroal.api.security.SimplePassword;
import jakarta.annotation.Nullable;
import java.sql.SQLException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import javax.sql.DataSource;
import org.immutables.value.Value;

@Value.Immutable
public abstract class DataSourceProducer {

  public static ImmutableDataSourceProducer.Builder builder() {
    return ImmutableDataSourceProducer.builder();
  }

  @Value.Default
  int initialSize() {
    return 2;
  }

  @Value.Default
  int maxSize() {
    return 5;
  }

  @Value.Default
  int minSize() {
    return 2;
  }

  @Value.Default
  int maxLifetimeMinutes() {
    return 5;
  }

  @Value.Default
  Duration maxLifetime() {
    return Duration.of(maxLifetimeMinutes(), ChronoUnit.MINUTES);
  }

  @Value.Default
  public int acquisitionTimeoutSeconds() {
    return 20;
  }

  @Value.Default
  Duration acquisitionTimeout() {
    return Duration.of(acquisitionTimeoutSeconds(), ChronoUnit.SECONDS);
  }

  @Value.Default
  TransactionIsolation transactionIsolation() {
    return TransactionIsolation.READ_COMMITTED;
  }

  public abstract String jdbcUrl();

  @Nullable
  public abstract String jdbcUser();

  @Nullable
  public abstract String jdbcPass();

  public DataSource createNewDataSource() throws SQLException {
    AgroalDataSourceConfigurationSupplier dataSourceConfiguration =
        new AgroalDataSourceConfigurationSupplier();
    AgroalConnectionPoolConfigurationSupplier poolConfiguration =
        dataSourceConfiguration.connectionPoolConfiguration();
    AgroalConnectionFactoryConfigurationSupplier connectionFactoryConfiguration =
        poolConfiguration.connectionFactoryConfiguration();

    // configure pool
    poolConfiguration
        .initialSize(initialSize())
        .maxSize(maxSize())
        .minSize(minSize())
        .maxLifetime(maxLifetime())
        .acquisitionTimeout(acquisitionTimeout());

    // configure supplier
    connectionFactoryConfiguration.jdbcUrl(jdbcUrl());
    if (jdbcUser() != null) {
      connectionFactoryConfiguration.credential(new NamePrincipal(jdbcUser()));
      connectionFactoryConfiguration.credential(new SimplePassword(jdbcPass()));
    }
    connectionFactoryConfiguration.jdbcTransactionIsolation(transactionIsolation());
    connectionFactoryConfiguration.autoCommit(false);

    return AgroalDataSource.from(dataSourceConfiguration.get());
  }
}
