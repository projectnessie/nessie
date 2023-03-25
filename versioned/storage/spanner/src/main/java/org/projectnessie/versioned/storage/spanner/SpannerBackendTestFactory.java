/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.versioned.storage.spanner;

import static java.util.Collections.emptyList;

import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Instance;
import com.google.cloud.spanner.InstanceConfigId;
import com.google.cloud.spanner.InstanceId;
import com.google.cloud.spanner.InstanceInfo;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.common.annotations.VisibleForTesting;
import java.util.Optional;
import org.projectnessie.versioned.storage.testextension.BackendTestFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

public class SpannerBackendTestFactory implements BackendTestFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(SpannerBackendTestFactory.class);
  public static final int SPANNER_GRPC = 9010;
  public static final int SPANNER_REST = 9020;
  private static final int SPANNER_NODES =
      Integer.getInteger("nessie.storage.spanner-test-nodes", 1);

  private GenericContainer<?> container;
  private String projectId;
  private String emulatorHost;
  private DatabaseId databaseId;

  @Override
  public SpannerBackend createNewBackend() {
    return new SpannerBackend(spanner(), true, databaseId);
  }

  @VisibleForTesting
  Spanner spanner() {
    SpannerOptions options =
        SpannerOptions.newBuilder().setEmulatorHost(emulatorHost).setProjectId(projectId).build();
    return options.getService();
  }

  @SuppressWarnings("resource")
  public void startSpanner(Optional<String> containerNetworkId) {
    if (container != null) {
      throw new IllegalStateException("Already started");
    }

    String version = System.getProperty("it.nessie.container.spanner-local.tag", "latest");
    String imageName = "gcr.io/cloud-spanner-emulator/emulator:" + version;

    for (int retry = 0; ; retry++) {
      GenericContainer<?> c =
          new GenericContainer<>(imageName)
              .withLogConsumer(new Slf4jLogConsumer(LOGGER))
              .withExposedPorts(SPANNER_GRPC, SPANNER_REST);
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

    Integer port = containerNetworkId.isPresent() ? SPANNER_GRPC : container.getFirstMappedPort();
    String host =
        containerNetworkId.isPresent()
            ? container.getCurrentContainerInfo().getConfig().getHostName()
            : container.getHost();

    projectId = "test-project";
    String instanceConfig = "emulator-config";
    InstanceConfigId instanceConfigId = InstanceConfigId.of(projectId, instanceConfig);
    databaseId = DatabaseId.of(InstanceId.of(projectId, "nessie-instance"), "nessie-test-database");
    emulatorHost = String.format("%s:%d", host, port);

    try (Spanner spanner = spanner()) {
      Instance instance =
          spanner
              .getInstanceAdminClient()
              .createInstance(
                  InstanceInfo.newBuilder(databaseId.getInstanceId())
                      .setInstanceConfigId(instanceConfigId)
                      .setNodeCount(SPANNER_NODES)
                      .setDisplayName("Nessie Test")
                      .build())
              .get();
      LOGGER.info("Created Spanner instance {} with config {}", instance.getId(), instanceConfigId);

      Database database =
          spanner
              .getDatabaseAdminClient()
              .createDatabase(
                  databaseId.getInstanceId().getInstance(), databaseId.getDatabase(), emptyList())
              .get();
      LOGGER.info(
          "Created Spanner database {} using dialect {}", database.getId(), database.getDialect());

    } catch (Exception e) {
      throw new RuntimeException(
          "Could not create Spanner instance "
              + databaseId.getInstanceId()
              + " with config "
              + instanceConfigId,
          e);
    }
  }

  public DatabaseId databaseId() {
    return databaseId;
  }

  public String emulatorHost() {
    return emulatorHost;
  }

  @Override
  public void start() {
    startSpanner(Optional.empty());
  }

  @Override
  public void stop() {
    try {
      if (container != null) {
        container.stop();
      }
    } finally {
      container = null;
    }
  }
}
