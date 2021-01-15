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

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.CockroachContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;

import com.dremio.nessie.versioned.impl.AbstractTieredStoreFixture;
import com.dremio.nessie.versioned.store.jdbc.ImmutableJdbcStoreConfig.Builder;

import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.api.security.NamePrincipal;
import io.agroal.api.security.SimplePassword;

public class JdbcFixture extends AbstractTieredStoreFixture<JdbcStore, JdbcStoreConfig> {
  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcFixture.class);

  private static DataSource dataSource;
  private static JdbcDatabaseContainer<?> databaseContainer;

  public JdbcFixture() {
    super(makeConfig(true));
  }

  private static JdbcStoreConfig makeConfig(boolean initDb) {
    Dialect dialect = getDialect();

    Builder builder = JdbcStoreConfig.builder()
        .dialect(dialect)
        .setupTables(initDb)
        .logCreateDDL(false);

    if (System.getProperty("it.nessie.store.jdbc.table_prefix") != null) {
      builder.tablePrefix(System.getProperty("it.nessie.store.jdbc.table_prefix"));
    }
    if (System.getProperty("it.nessie.store.jdbc.schema") != null) {
      builder.schema(System.getProperty("it.nessie.store.jdbc.schema"));
    }
    if (System.getProperty("it.nessie.store.jdbc.catalog") != null) {
      builder.catalog(System.getProperty("it.nessie.store.jdbc.catalog"));
    }
    return builder.build();
  }

  @Override
  public JdbcStore createStoreImpl() {
    try {
      JdbcStoreConfig cfg = makeConfig(dataSource == null);
      if (dataSource == null) {
        dataSource = createDataSource();
      }
      return new JdbcStore(cfg, dataSource);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  static void cleanup() {
    if (databaseContainer != null) {
      databaseContainer.stop();
      databaseContainer = null;
    }
    dataSource = null;
  }

  private static DataSource createDataSource() {
    Dialect dialect = getDialect();

    String url = "jdbc:";
    switch (dialect) {
      case H2:
        // give it a name for H2, so it's not one-database-per-connection
        url = "jdbc:h2:mem:nessie";
        break;
      case HSQL:
        url = "jdbc:hsqldb:mem:nessie";
        break;
      default:
        break;
    }
    String user = "";
    String pass = "";
    if (Boolean.getBoolean("it.nessie.store.jdbc.testcontainer")) {
      databaseContainer = startContainer(dialect);
      url = databaseContainer.getJdbcUrl();
      user = databaseContainer.getUsername();
      pass = databaseContainer.getPassword();
    }
    url = System.getProperty("it.nessie.store.jdbc.url", url);
    user = System.getProperty("it.nessie.store.jdbc.username", user);
    pass = System.getProperty("it.nessie.store.jdbc.password", pass);

    LOGGER.info("Creating data source, url={}, user={}", url, user);

    // We use the Agroal data-source/connection-pool here, because we need to cast the
    // java.sql.Connection to an OracleConnection in Dialect.ORACLE.setArray.
    AgroalDataSourceConfigurationSupplier cfgSupplier = new AgroalDataSourceConfigurationSupplier();
    cfgSupplier
        .connectionPoolConfiguration()
        .initialSize(2)
        .maxSize(5)
        .connectionFactoryConfiguration()
        .jdbcUrl(url)
        .principal(new NamePrincipal(user))
        .credential(new SimplePassword(pass));
    return new io.agroal.pool.DataSource(cfgSupplier.get());
  }

  private static Dialect getDialect() {
    String dialectString = System.getProperty("it.nessie.store.jdbc.dialect", Dialect.HSQL.name());
    if (dialectString == null || dialectString.isEmpty()) {
      String supportedDialects = Arrays.stream(Dialect.values())
          .map(Enum::name).collect(Collectors.joining(", "));
      throw new IllegalStateException(
          "Must set system property it.nessie.store.jdbc.dialect to one of: "
              + supportedDialects);
    }
    Dialect dialect = Dialect.valueOf(dialectString);
    return dialect;
  }

  @Override
  public void close() {
    getStore().truncateTables();
    getStore().close();
  }

  static JdbcDatabaseContainer<?> startContainer(Dialect dialect) {
    try {
      JdbcDatabaseContainer<?> container;
      String image;

      switch (dialect) {
        case COCKROACH:
          container = new CockroachContainer("cockroachdb/cockroach:"
              // Note v20.2.x doesn't work
              + System.getProperty("it.nessie.store.jdbc.container-image-version", "v20.1.11"));
          break;
        case POSTGRESQL:
          container = new PostgreSQLContainer<>("postgres:"
              + System.getProperty("it.nessie.store.jdbc.container-image-version", "9.6.12"));
          break;
        case ORACLE:
          image = System.getProperty("it.nessie.store.jdbc.container-image");
          if (image == null) {
            throw new IllegalStateException(
                "Dialect " + dialect + " requires that the system property "
                    + "it.nessie.store.jdbc.container-image points to the test-container image to use.");
          }
          int requestedOraclePort = Integer.getInteger("it.nessie.store.jdbc.oracle-port", -1);
          String requestedOracleSid = System.getProperty("it.nessie.store.jdbc.oracle-sid");
          String requestedOraclePassword = System.getProperty("it.nessie.store.jdbc.oracle-password");
          String requestedOracleUsername = System.getProperty("it.nessie.store.jdbc.oracle-username");
          String waitForRegex = System.getProperty("it.nessie.store.jdbc.oracle-wait-regex");
          boolean requestContainerConnectUrl = Boolean.getBoolean("it.nessie.store.jdbc.oracle-container-ip-url");
          container = new OracleContainer(image) {
            @Override
            public String getSid() {
              return requestedOracleSid != null ? requestedOracleSid : super.getSid();
            }

            @Override
            public String getJdbcUrl() {
              if (requestContainerConnectUrl) {
                int port = requestedOraclePort > 0 ? requestedOraclePort : 1521;
                String ip = getCurrentContainerInfo().getNetworkSettings().getIpAddress();
                return "jdbc:oracle:thin:" + getUsername() + "/" + getPassword() + "@" + ip + ":" + port + ":" + getSid();
              }
              return super.getJdbcUrl();
            }

            @Override
            public Integer getOraclePort() {
              return requestedOraclePort > 0 ? requestedOraclePort : super.getOraclePort();
            }

            @Override
            public String getUsername() {
              return requestedOracleUsername != null ? requestedOracleUsername : super.getUsername();
            }

            @Override
            public String getPassword() {
              return requestedOraclePassword != null ? requestedOraclePassword : super.getPassword();
            }

            @Override
            protected void waitUntilContainerStarted() {
              if (waitForRegex != null) {
                // Don't want to use a test-query, because the Oracle container starts the database
                // before it is actually usable.
                getWaitStrategy().waitUntilReady(this);
              } else {
                waitUntilContainerStarted();
              }
            }
          };
          container = container.withEnv("ORACLE_SID", ((OracleContainer)container).getSid())
              .withEnv("ORACLE_EDITION", "standard")
              .withEnv("ORACLE_PWD", container.getPassword());
          if (waitForRegex != null) {
            container = container.waitingFor(new LogMessageWaitStrategy()
                .withRegEx(waitForRegex)
                .withStartupTimeout(Duration.of(30L, ChronoUnit.MINUTES)));
          }
          break;
        case H2:
        default:
          throw new IllegalArgumentException(
              "Dialect " + dialect + " not supported for integration-tests");
      }

      container.withLogConsumer(new Slf4jLogConsumer(LOGGER))
          .start();

      return container;
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}
