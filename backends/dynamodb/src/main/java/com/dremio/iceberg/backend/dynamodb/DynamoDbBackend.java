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

package com.dremio.iceberg.backend.dynamodb;

import com.dremio.iceberg.backend.Backend;
import com.dremio.iceberg.backend.EntityBackend;
import com.dremio.iceberg.model.Table;
import com.dremio.iceberg.model.Tag;
import com.dremio.iceberg.model.User;
import com.dremio.iceberg.server.ServerConfiguration;
import java.net.URI;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;

/**
 * Implements Backend for DynamoDB.
 */
public class DynamoDbBackend implements Backend, AutoCloseable {

  private final DynamoDbClient client;
  private final DynamoDbEnhancedClient mapper;

  public DynamoDbBackend(DynamoDbClient client) {
    this.client = client;
    mapper = DynamoDbEnhancedClient.builder()
      .dynamoDbClient(client)
      .build();
  }

  public static DynamoDbBackend getInstance(String region) {
    return getInstance(region, null);
  }

  public static DynamoDbBackend getInstance(String region, String endpoint) {
    DynamoDbClientBuilder clientBuilder = DynamoDbClient.builder();
    if (endpoint != null) {
      clientBuilder = clientBuilder.endpointOverride(URI.create(endpoint));
    } else {
      clientBuilder = clientBuilder.region(Region.of(region));
    }

    DynamoDbClient client = clientBuilder.build();

    return new DynamoDbBackend(client);
  }

  @Override
  public EntityBackend<Table> tableBackend() {
    return new TableDynamoDbBackend(client, mapper);
  }

  public EntityBackend<User> userBackend() {
    return new UserDynamoDbBackend(client, mapper);
  }

  @Override
  public EntityBackend<Tag> tagBackend() {
    return new TagDynamoDbBackend(client, mapper);
  }

  @Override
  public void close() {

  }

  /**
   * Factory for DynamoDB Backend.
   */
  public static class BackendFactory implements Backend.Factory {

    @Override
    public Backend create(ServerConfiguration config) {
      return DynamoDbBackend
        .getInstance(config.getDatabaseConfiguration().getDbProps().get("region"),
          config.getDatabaseConfiguration().getDbProps().get("endpoint"));
    }
  }

}
