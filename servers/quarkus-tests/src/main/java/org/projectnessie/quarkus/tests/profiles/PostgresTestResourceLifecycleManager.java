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
package org.projectnessie.quarkus.tests.profiles;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testcontainers.containers.PostgreSQLContainer.POSTGRESQL_PORT;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.common.DevServicesContext;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;

public class PostgresTestResourceLifecycleManager
    implements QuarkusTestResourceLifecycleManager, DevServicesContext.ContextAware {
  private JdbcDatabaseContainer<?> container;

  private Optional<String> containerNetworkId;

  @Override
  public void setIntegrationTestContext(DevServicesContext context) {
    containerNetworkId = context.containerNetworkId();
  }

  @Override
  public Map<String, String> start() {
    container =
        new PostgreSQLContainer<>(dockerImage("postgres"))
            .withLogConsumer(outputFrame -> {})
            .withStartupAttempts(5);
    containerNetworkId.ifPresent(container::withNetworkMode);
    try {
      // Only start the Docker container (local Dynamo-compatible). The DynamoDatabaseClient will
      // be configured via Quarkus -> Quarkus-Dynamo / DynamoVersionStoreFactory.
      container.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    String jdbcUrl = container.getJdbcUrl();

    if (containerNetworkId.isPresent()) {
      String hostPort = container.getHost() + ':' + container.getMappedPort(POSTGRESQL_PORT);
      String networkHostPort =
          container.getCurrentContainerInfo().getConfig().getHostName() + ':' + POSTGRESQL_PORT;
      jdbcUrl = jdbcUrl.replace(hostPort, networkHostPort);
    }

    return ImmutableMap.of(
        "quarkus.datasource.username",
        container.getUsername(),
        "quarkus.datasource.password",
        container.getPassword(),
        "quarkus.datasource.jdbc.url",
        jdbcUrl,
        "quarkus.datasource.jdbc.extended-leak-report",
        "true");
  }

  @Override
  public void stop() {
    if (container != null) {
      try {
        container.stop();
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        container = null;
      }
    }
  }

  protected static String dockerImage(String dbName) {
    URL resource =
        PostgresTestResourceLifecycleManager.class.getResource("Dockerfile-" + dbName + "-version");
    try (InputStream in = resource.openConnection().getInputStream()) {
      String[] imageTag =
          Arrays.stream(new String(in.readAllBytes(), UTF_8).split("\n"))
              .map(String::trim)
              .filter(l -> l.startsWith("FROM "))
              .map(l -> l.substring(5).trim().split(":"))
              .findFirst()
              .orElseThrow();
      String image = imageTag[0];
      String version = System.getProperty("it.nessie.container." + dbName + ".tag", imageTag[1]);
      return image + ':' + version;
    } catch (Exception e) {
      throw new RuntimeException("Failed to extract tag from " + resource, e);
    }
  }
}
