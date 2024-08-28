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
package org.projectnessie.versioned.storage.dynamodbtests2;

import static software.amazon.awssdk.regions.Region.US_WEST_2;

import java.util.Map;
import java.util.Optional;
import org.projectnessie.nessie.testing.containerspec.ContainerSpecHelper;
import org.projectnessie.versioned.storage.dynamodb2.DynamoDB2Backend;
import org.projectnessie.versioned.storage.dynamodb2.DynamoDB2BackendConfig;
import org.projectnessie.versioned.storage.dynamodb2.DynamoDB2BackendFactory;
import org.projectnessie.versioned.storage.dynamodb2.ImmutableDynamoDB2BackendConfig.Builder;
import org.projectnessie.versioned.storage.testextension.BackendTestFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

public class DynamoDB2BackendTestFactory implements BackendTestFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDB2BackendTestFactory.class);
  public static final int DYNAMODB_PORT = 8000;

  private GenericContainer<?> container;
  private String endpointURI;

  @Override
  public String getName() {
    return DynamoDB2BackendFactory.NAME;
  }

  @Override
  public DynamoDB2Backend createNewBackend() {
    return createNewBackend(dynamoDBConfigBuilder().build(), true);
  }

  public DynamoDB2Backend createNewBackend(
      DynamoDB2BackendConfig dynamoDBBackendConfig, boolean closeClient) {
    return new DynamoDB2Backend(dynamoDBBackendConfig, closeClient);
  }

  public Builder dynamoDBConfigBuilder() {
    return DynamoDB2BackendConfig.builder().client(buildNewClient());
  }

  public DynamoDbClient buildNewClient() {
    return DynamoClientProducer.builder()
        .endpointURI(endpointURI)
        .region("US_WEST_2")
        .credentialsProvider(
            StaticCredentialsProvider.create(AwsBasicCredentials.create("xxx", "xxx")))
        .build()
        .createClient();
  }

  @Override
  @SuppressWarnings("resource")
  public void start(Optional<String> containerNetworkId) {
    if (container != null) {
      throw new IllegalStateException("Already started");
    }

    DockerImageName dockerImage =
        ContainerSpecHelper.builder()
            .name("dynamodb-local")
            .containerClass(DynamoDB2BackendTestFactory.class)
            .build()
            .dockerImageName(null);

    for (int retry = 0; ; retry++) {
      GenericContainer<?> c =
          new GenericContainer<>(dockerImage)
              .withLogConsumer(new Slf4jLogConsumer(LOGGER))
              .withExposedPorts(DYNAMODB_PORT)
              .withCommand("-jar", "DynamoDBLocal.jar", "-inMemory", "-sharedDb");
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

    Integer port = containerNetworkId.isPresent() ? DYNAMODB_PORT : container.getFirstMappedPort();
    String host =
        containerNetworkId.isPresent()
            ? container.getCurrentContainerInfo().getConfig().getHostName()
            : container.getHost();

    endpointURI = String.format("http://%s:%d", host, port);
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

  public String getEndpointURI() {
    return endpointURI;
  }

  @Override
  public Map<String, String> getQuarkusConfig() {
    return Map.of(
        "quarkus.dynamodb.endpoint-override",
        getEndpointURI(),
        "quarkus.dynamodb.aws.region",
        US_WEST_2.id(),
        "quarkus.dynamodb.aws.credentials.type",
        "STATIC",
        "quarkus.dynamodb.aws.credentials.static-provider.access-key-id",
        "xxx",
        "quarkus.dynamodb.aws.credentials.static-provider.secret-access-key",
        "xxx");
  }
}
