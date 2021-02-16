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
package org.projectnessie.server.providers;

import static org.projectnessie.server.config.VersionStoreConfig.VersionStoreType.DYNAMO;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;

import javax.enterprise.context.Dependent;
import javax.inject.Inject;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.projectnessie.server.config.DynamoVersionStoreConfig;
import org.projectnessie.server.config.TieredVersionStoreConfig;
import org.projectnessie.versioned.dynamodb.DynamoStore;
import org.projectnessie.versioned.dynamodb.DynamoStoreConfig;
import org.projectnessie.versioned.store.Store;

import software.amazon.awssdk.regions.Region;

/**
 * DynamoDB version store factory.
 */
@StoreType(DYNAMO)
@Dependent
public class DynamoVersionStoreFactory extends TieredVersionStoreFactory {
  private final DynamoVersionStoreConfig config;

  private final String region;
  private final Optional<String> endpoint;

  /**
   * Creates a factory for dynamodb version stores.
   */
  @Inject
  public DynamoVersionStoreFactory(DynamoVersionStoreConfig config, TieredVersionStoreConfig tieredVersionStoreConfig,
      @ConfigProperty(name = "quarkus.dynamodb.aws.region") String region,
      @ConfigProperty(name = "quarkus.dynamodb.endpoint-override") Optional<String> endpoint) {
    super(tieredVersionStoreConfig);
    this.config = config;
    this.region = region;
    this.endpoint = endpoint;
  }

  /**
   * create a dynamo store based on config.
   */
  @Override
  protected Store createStore() {
    DynamoStore dynamo = new DynamoStore(
        DynamoStoreConfig.builder()
          .endpoint(endpoint.map(e -> {
            try {
              return new URI(e);
            } catch (URISyntaxException uriSyntaxException) {
              throw new RuntimeException(uriSyntaxException);
            }
          }))
          .region(Region.of(region))
          .initializeDatabase(config.isDynamoInitialize())
          .tablePrefix(config.getTablePrefix())
          .enableTracing(config.enableTracing())
          .build());
    dynamo.start();
    return dynamo;
  }
}
