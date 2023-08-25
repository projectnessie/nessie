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
package org.projectnessie.gc.contents.jdbc;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.AgroalConnectionFactoryConfiguration.TransactionIsolation;
import io.agroal.api.configuration.supplier.AgroalConnectionFactoryConfigurationSupplier;
import io.agroal.api.configuration.supplier.AgroalConnectionPoolConfigurationSupplier;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.api.security.AgroalSecurityProvider;
import io.agroal.api.security.NamePrincipal;
import io.agroal.api.security.SimplePassword;
import java.sql.SQLException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Value.Immutable
public abstract class AgroalJdbcDataSourceProvider implements JdbcDataSourceProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(AgroalJdbcDataSourceProvider.class);

  public static Builder builder() {
    return ImmutableAgroalJdbcDataSourceProvider.builder();
  }

  @SuppressWarnings({"UnusedReturnValue", "unused"})
  public interface Builder {
    Builder poolMinSize(int minSize);

    Builder poolMaxSize(int maxSize);

    Builder poolInitialSize(int initialSize);

    Builder poolConnectionLifetime(Duration connectionLifetime);

    Builder poolAcquisitionTimeout(Duration acquisitionTimeout);

    Builder jdbcUrl(String jdbcUrl);

    Builder addCredentials(Object credentials);

    Builder addCredentials(Object... credentials);

    Builder addAllCredentials(Iterable<?> credentials);

    Builder addSecurityProviders(AgroalSecurityProvider securityProvider);

    Builder addSecurityProviders(AgroalSecurityProvider... securityProviders);

    Builder addAllSecurityProviders(Iterable<? extends AgroalSecurityProvider> securityProviders);

    default Builder usernamePasswordCredentials(String jdbcUser, String jdbcPassword) {
      if (jdbcUser != null) {
        addCredentials(new NamePrincipal(jdbcUser));
      }
      if (jdbcPassword != null) {
        addCredentials(new SimplePassword(jdbcPassword));
      }
      return this;
    }

    Builder transactionIsolation(TransactionIsolation transactionIsolation);

    Builder putJdbcProperties(String key, String value);

    Builder putAllJdbcProperties(Map<String, ? extends String> entries);

    AgroalJdbcDataSourceProvider build();
  }

  @Value.Default
  int poolMinSize() {
    return 2;
  }

  @Value.Default
  int poolMaxSize() {
    return 5;
  }

  @Value.Default
  int poolInitialSize() {
    return 2;
  }

  @Value.Default
  Duration poolConnectionLifetime() {
    return Duration.of(5, ChronoUnit.MINUTES);
  }

  @Value.Default
  Duration poolAcquisitionTimeout() {
    return Duration.of(10, ChronoUnit.SECONDS);
  }

  abstract String jdbcUrl();

  abstract List<AgroalSecurityProvider> securityProviders();

  abstract List<Object> credentials();

  @Value.Default
  TransactionIsolation transactionIsolation() {
    return TransactionIsolation.READ_COMMITTED;
  }

  abstract Map<String, String> jdbcProperties();

  @Value.Lazy
  @Override
  public DataSource dataSource() throws SQLException {
    AgroalDataSourceConfigurationSupplier dataSourceConfiguration =
        new AgroalDataSourceConfigurationSupplier();
    AgroalConnectionPoolConfigurationSupplier poolConfiguration =
        dataSourceConfiguration.connectionPoolConfiguration();
    AgroalConnectionFactoryConfigurationSupplier connectionFactoryConfiguration =
        poolConfiguration.connectionFactoryConfiguration();

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Connecting to JDBC URL {}", jdbcUrl());
      for (Object credential : credentials()) {
        if (credential instanceof NamePrincipal) {
          LOGGER.debug(
              "Connecting using name principal {}", ((NamePrincipal) credential).getName());
        } else if (credential instanceof SimplePassword) {
          LOGGER.debug("Connecting using given password (not logged)");
        }
      }

      LOGGER.debug(
          "JDBC pool options: initial-size={}, max-size={}, min-size={}, connection-lifetime={}, acquisition-timeout={}",
          poolInitialSize(),
          poolMaxSize(),
          poolMinSize(),
          poolConnectionLifetime(),
          poolAcquisitionTimeout());
      jdbcProperties().forEach((k, v) -> LOGGER.debug("Using JDBC property {}={}", k, v));
    }

    poolConfiguration
        .initialSize(poolInitialSize())
        .maxSize(poolMaxSize())
        .minSize(poolMinSize())
        .maxLifetime(poolConnectionLifetime())
        .acquisitionTimeout(poolAcquisitionTimeout());

    securityProviders().forEach(connectionFactoryConfiguration::addSecurityProvider);
    connectionFactoryConfiguration.jdbcUrl(jdbcUrl());
    jdbcProperties().forEach(connectionFactoryConfiguration::jdbcProperty);
    credentials().forEach(connectionFactoryConfiguration::credential);
    connectionFactoryConfiguration.jdbcTransactionIsolation(transactionIsolation());
    connectionFactoryConfiguration.autoCommit(false);

    return AgroalDataSource.from(dataSourceConfiguration.get());
  }
}
