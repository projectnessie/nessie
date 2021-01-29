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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;

public class JdbcOracleFixture extends JdbcFixture {
  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcOracleFixture.class);

  @Override
  protected DatabaseAdapter createDatabaseAdapter() {
    return new OracleDatabaseAdapter();
  }

  @Override
  JdbcDatabaseContainer<?> startContainer(DatabaseAdapter databaseAdapter) {
    try {
      JdbcDatabaseContainer<?> container;
      String image;

      switch (databaseAdapter.name()) {
        case "Oracle":
          image = System.getProperty("it.nessie.store.jdbc.container-image");
          if (image == null) {
            throw new IllegalStateException(
                "Dialect " + databaseAdapter + " requires that the system property "
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
                @SuppressWarnings("deprecation") String ip = getCurrentContainerInfo().getNetworkSettings().getIpAddress();
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
        default:
          throw new IllegalArgumentException(
              "Dialect " + databaseAdapter.name() + " not supported for integration-tests");
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
