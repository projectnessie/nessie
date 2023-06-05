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
package org.projectnessie.versioned.storage.mongodb;

import com.google.common.annotations.VisibleForTesting;
import com.mongodb.client.MongoClient;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.projectnessie.versioned.storage.testextension.BackendTestFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

public class MongoDBBackendTestFactory implements BackendTestFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(MongoDBBackendTestFactory.class);
  public static final String MONGO_DB_NAME = "test";
  public static final int MONGO_PORT = 27017;

  private MongoDBContainer container;
  private String connectionString;

  @Override
  public String getName() {
    return MongoDBBackendFactory.NAME;
  }

  /**
   * Starts MongoDB with an optional Docker network ID and a flag to turn off all output to stdout
   * and stderr.
   */
  public void startMongo(Optional<String> containerNetworkId) {
    if (container != null) {
      throw new IllegalStateException("Already started");
    }

    String version = System.getProperty("it.nessie.container.mongodb.tag", "latest");

    for (int retry = 0; ; retry++) {
      MongoDBContainer c =
          new MongoDBContainer("mongo:" + version).withLogConsumer(new Slf4jLogConsumer(LOGGER));
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
  public MongoDBBackend createNewBackend() {
    MongoClient client = buildNewClient();

    MongoDBBackendConfig config =
        MongoDBBackendConfig.builder().databaseName(MONGO_DB_NAME).client(client).build();

    return new MongoDBBackend(config, true);
  }

  @VisibleForTesting
  MongoClient buildNewClient() {
    return MongoClientProducer.builder().connectionString(connectionString).build().createClient();
  }

  @Override
  public void start() {
    startMongo(Optional.empty());
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

  public Map<String, String> getQuarkusConfig() {
    Map<String, String> config = new HashMap<>();
    config.put("quarkus.mongodb.connection-string", connectionString);
    config.put("quarkus.mongodb.database", MONGO_DB_NAME);
    return config;
  }
}
