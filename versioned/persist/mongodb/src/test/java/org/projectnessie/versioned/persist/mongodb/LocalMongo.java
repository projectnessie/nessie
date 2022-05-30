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
package org.projectnessie.versioned.persist.mongodb;

import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MongoDBContainer;

public class LocalMongo {
  private static final Logger LOGGER = LoggerFactory.getLogger(LocalMongo.class);

  public static final String MONGO_DB_NAME = "test";
  public static final int MONGO_PORT = 27017;

  private MongoDBContainer container;
  private String connectionString;

  public MongoDBContainer getContainer() {
    return container;
  }

  public String getConnectionString() {
    return connectionString;
  }

  public String getDatabaseName() {
    return MONGO_DB_NAME;
  }

  /**
   * Starts MongoDB with an optional Docker network ID and a flag to turn off all output to stdout
   * and stderr.
   */
  public void startMongo(Optional<String> containerNetworkId, boolean quiet) {
    if (container != null) {
      throw new IllegalStateException("Already started");
    }

    String version = System.getProperty("it.nessie.container.mongodb.tag", "4.2-bionic");

    if (!quiet) {
      LOGGER.info("Starting Dynamo test container (network-id: {})", containerNetworkId);
    }

    container = new MongoDBContainer("mongo:" + version).withLogConsumer(outputFrame -> {});
    containerNetworkId.ifPresent(container::withNetworkMode);
    try {
      container.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    connectionString = container.getReplicaSetUrl(MONGO_DB_NAME);

    if (containerNetworkId.isPresent()) {
      String hostPort = container.getHost() + ':' + container.getMappedPort(MONGO_PORT);
      String networkHostPort =
          container.getCurrentContainerInfo().getConfig().getHostName() + ':' + MONGO_PORT;
      connectionString = connectionString.replace(hostPort, networkHostPort);
    }

    if (!quiet) {
      LOGGER.info(
          "Mongo test container connection string is {} (network-id: {})",
          connectionString,
          containerNetworkId);
    }
  }

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
