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
package org.projectnessie.versioned.persist.dynamodb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

/** DynamoDB test connection-provider source using a local DynamoDB instance via testcontainers. */
public class LocalDynamoTestConnectionProviderSource extends DynamoTestConnectionProviderSource {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(LocalDynamoTestConnectionProviderSource.class);

  private GenericContainer<?> container;
  private String endpointURI;

  @Override
  public DynamoClientConfig createDefaultConnectionProviderConfig() {
    return ImmutableDefaultDynamoClientConfig.builder().build();
  }

  @Override
  public DynamoDatabaseClient createConnectionProvider() {
    return new DynamoDatabaseClient();
  }

  @Override
  public void start() throws Exception {
    startDynamo();

    configureConnectionProviderConfigFromDefaults(
        c ->
            ImmutableDefaultDynamoClientConfig.builder()
                .endpointURI(endpointURI)
                .region("US_WEST_2")
                .credentialsProvider(
                    StaticCredentialsProvider.create(AwsBasicCredentials.create("xxx", "xxx")))
                .build());

    super.start();
  }

  public String getEndpointURI() {
    return endpointURI;
  }

  public void startDynamo() {
    if (container != null) {
      throw new IllegalStateException("Already started");
    }

    // dynalite is much faster than dynamodb-local
    // String version = System.getProperty("it.nessie.container.dynamodb.tag", "latest");
    // String imageName = "amazon/dynamodb-local:" + version;
    String version = System.getProperty("it.nessie.container.dynalite.tag", "latest");
    String imageName = "dimaqq/dynalite:" + version;

    container =
        new GenericContainer<>(imageName)
            .withLogConsumer(new Slf4jLogConsumer(LOGGER))
            .withExposedPorts(8000);
    container.start();

    Integer port = container.getFirstMappedPort();

    endpointURI = String.format("http://localhost:%d", port);
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
}
