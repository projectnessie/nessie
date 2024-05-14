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
package org.projectnessie.versioned.storage.dynamodbtests;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.Optional;
import org.projectnessie.versioned.storage.dynamodb.DynamoDBBackend;
import org.projectnessie.versioned.storage.dynamodb.DynamoDBBackendConfig;
import org.projectnessie.versioned.storage.dynamodb.DynamoDBBackendFactory;
import org.projectnessie.versioned.storage.dynamodb.ImmutableDynamoDBBackendConfig.Builder;
import org.projectnessie.versioned.storage.testextension.BackendTestFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

public class DynamoDBBackendTestFactory implements BackendTestFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBBackendTestFactory.class);
  public static final int DYNAMODB_PORT = 8000;

  private GenericContainer<?> container;
  private String endpointURI;

  @Override
  public String getName() {
    return DynamoDBBackendFactory.NAME;
  }

  @Override
  public DynamoDBBackend createNewBackend() {
    return createNewBackend(dynamoDBConfigBuilder().build(), true);
  }

  @SuppressWarnings("ClassEscapesDefinedScope")
  public DynamoDBBackend createNewBackend(
      DynamoDBBackendConfig dynamoDBBackendConfig, boolean closeClient) {
    return new DynamoDBBackend(dynamoDBBackendConfig, closeClient);
  }

  public Builder dynamoDBConfigBuilder() {
    return DynamoDBBackendConfig.builder().client(buildNewClient());
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

  protected static String dockerImage(String dbName) {
    URL resource =
        DynamoDBBackendTestFactory.class.getResource("Dockerfile-" + dbName + "-version");
    try (InputStream in = resource.openConnection().getInputStream()) {
      String[] imageTag =
          Arrays.stream(new String(in.readAllBytes(), UTF_8).split("\n"))
              .map(String::trim)
              .filter(l -> l.startsWith("FROM "))
              .map(l -> l.substring(5).trim().split(":"))
              .findFirst()
              .orElseThrow();
      String imageName = imageTag[0];
      String version =
          System.getProperty("it.nessie.container." + dbName + "-local.tag", imageTag[1]);
      return imageName + ':' + version;
    } catch (Exception e) {
      throw new RuntimeException("Failed to extract tag from " + resource, e);
    }
  }

  @SuppressWarnings("resource")
  public void startDynamo(Optional<String> containerNetworkId) {
    if (container != null) {
      throw new IllegalStateException("Already started");
    }

    String image = dockerImage("dynamodb-local");

    for (int retry = 0; ; retry++) {
      GenericContainer<?> c =
          new GenericContainer<>(image)
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
    startDynamo(Optional.empty());
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
}
