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
package org.projectnessie.versioned.storage.mongodbtests2;

import com.mongodb.client.MongoClient;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.projectnessie.nessie.testing.containerspec.ContainerSpecHelper;
import org.projectnessie.versioned.storage.common.persist.Backend;
import org.projectnessie.versioned.storage.mongodb2.MongoDB2Backend;
import org.projectnessie.versioned.storage.mongodb2.MongoDB2BackendConfig;
import org.projectnessie.versioned.storage.mongodb2.MongoDB2BackendFactory;
import org.projectnessie.versioned.storage.testextension.BackendTestFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.mongodb.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

public class MongoDB2BackendTestFactory implements BackendTestFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(MongoDB2BackendTestFactory.class);
  public static final String MONGO_DB_NAME = "test";
  public static final int MONGO_PORT = 27017;

  private MongoDBContainer container;
  private String connectionString;

  @Override
  public String getName() {
    return MongoDB2BackendFactory.NAME;
  }

  @Override
  public void start(Optional<String> containerNetworkId) {
    if (container != null) {
      throw new IllegalStateException("Already started");
    }

    DockerImageName dockerImage =
        ContainerSpecHelper.builder()
            .name("mongodb")
            .containerClass(MongoDB2BackendTestFactory.class)
            .build()
            .dockerImageName(null)
            .asCompatibleSubstituteFor("mongo");

    for (int retry = 0; ; retry++) {
      MongoDBContainer c =
          new MongoDBContainer(dockerImage).withLogConsumer(new Slf4jLogConsumer(LOGGER));
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

    connectionString = container.getReplicaSetUrl(MONGO_DB_NAME);

    if (containerNetworkId.isPresent()) {
      String hostPort = container.getHost() + ':' + container.getMappedPort(MONGO_PORT);
      String networkHostPort =
          container.getCurrentContainerInfo().getConfig().getHostName() + ':' + MONGO_PORT;
      connectionString = connectionString.replace(hostPort, networkHostPort);
    }
  }

  @Override
  public Backend createNewBackend() {
    MongoClient client = buildNewClient();

    MongoDB2BackendConfig config =
        MongoDB2BackendConfig.builder().databaseName(MONGO_DB_NAME).client(client).build();

    return new MongoDB2Backend(config, true);
  }

  public MongoClient buildNewClient() {
    return MongoClientProducer.builder().connectionString(connectionString).build().createClient();
  }

  public String getDatabaseName() {
    return MONGO_DB_NAME;
  }

  public String getConnectionString() {
    return connectionString;
  }

  @Override
  public void start() {
    start(Optional.empty());
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

  @Override
  public Map<String, String> getQuarkusConfig() {
    Map<String, String> config = new HashMap<>();
    config.put("quarkus.mongodb.connection-string", connectionString);
    config.put("quarkus.mongodb.database", MONGO_DB_NAME);
    return config;
  }
}
