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
package org.projectnessie.versioned.storage.jdbc;

import static com.google.common.base.Preconditions.checkState;
import static java.nio.charset.StandardCharsets.UTF_8;

import jakarta.annotation.Nonnull;
import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

public abstract class ContainerBackendTestFactory extends AbstractJdbcBackendTestFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(ContainerBackendTestFactory.class);

  private JdbcDatabaseContainer<?> container;

  protected static String dockerImage(String dbName) {
    URL resource =
        ContainerBackendTestFactory.class.getResource("Dockerfile-" + dbName + "-version");
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

  @Override
  protected String jdbcUrl() {
    checkState(container != null, "Container not started");
    return container.getJdbcUrl();
  }

  @Override
  protected String jdbcUser() {
    checkState(container != null, "Container not started");
    return container.getUsername();
  }

  @Override
  protected String jdbcPass() {
    checkState(container != null, "Container not started");
    return container.getPassword();
  }

  private void startJdbc(Optional<String> containerNetworkId) throws Exception {
    if (container != null) {
      throw new IllegalStateException("Already started");
    }

    for (int retry = 0; ; retry++) {
      @SuppressWarnings("resource")
      JdbcDatabaseContainer<?> c = createContainer().withLogConsumer(new Slf4jLogConsumer(LOGGER));
      containerNetworkId.ifPresent(c::withNetworkMode);
      try {
        c.start();
        container = c;
        break;
      } catch (ContainerLaunchException e) {
        c.close();
        if (e.getCause() != null && retry < 3) {
          LOGGER.warn("Launch of container {} failed, will retry...", c.getDockerImageName(), e);
          continue;
        }
        LOGGER.error("Launch of container {} failed", c.getDockerImageName(), e);
        throw new RuntimeException(e);
      }
    }

    super.start();
  }

  @Override
  public void start() throws Exception {
    startJdbc(Optional.empty());
  }

  @Override
  public void stop() throws Exception {
    try {
      super.stop();
    } finally {
      try {
        if (container != null) {
          container.stop();
        }
      } finally {
        container = null;
      }
    }
  }

  @Nonnull
  protected abstract JdbcDatabaseContainer<?> createContainer();
}
