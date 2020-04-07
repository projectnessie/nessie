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

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.dremio.iceberg.backend.Backend;
import com.dremio.iceberg.backend.EntityBackend;
import com.dremio.iceberg.model.Table;
import com.dremio.iceberg.model.Tag;
import com.dremio.iceberg.model.User;
import com.dremio.iceberg.server.ServerConfiguration;

public class DynamoDBBackend implements Backend, AutoCloseable {

  private final DynamoDBMapper mapper;
  private final AmazonDynamoDB client;
  private final DynamoDBMapperConfig config;

  public DynamoDBBackend(AmazonDynamoDB client) {
    this.client = client;
    config = DynamoDBMapperConfig.builder()
      .withConsistentReads(DynamoDBMapperConfig.ConsistentReads.CONSISTENT)
      .build();
    mapper = new DynamoDBMapper(client);
  }

  public DynamoDBBackend(String region) {
    this(region, null);
  }

  public DynamoDBBackend(String region, String endpoint) {
    AmazonDynamoDBClientBuilder clientBuilder = AmazonDynamoDBClientBuilder.standard();
    if (endpoint != null) {
      clientBuilder = clientBuilder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, region));
    } else {
      clientBuilder = clientBuilder.withRegion(region);
    }

    client = clientBuilder
//      .withMetricsCollector()
//      .withMonitoringListener() //todo
      .build();
    config = DynamoDBMapperConfig.builder()
      .withConsistentReads(DynamoDBMapperConfig.ConsistentReads.CONSISTENT)
      .build();
    mapper = new DynamoDBMapper(client);
  }

  @Override
  public EntityBackend<Table> tableBackend() {
    return new TableDynamoDBBackend(client, config, mapper);
  }

  public EntityBackend<User> userBackend() {
    return new UserDynamoDBBackend(client, config, mapper);
  }

  @Override
  public EntityBackend<Tag> tagBackend() {
    return new TagDynamoDBBackend(client, config, mapper);
  }

  @Override
  public void close() throws Exception {

  }

  public static class BackendFactory implements Backend.Factory {

    @Override
    public Backend create(ServerConfiguration config) {
      return new DynamoDBBackend(config.getDatabaseConfiguration().getDbProps().get("region"),
        config.getDatabaseConfiguration().getDbProps().get("endpoint"));
    }
  }

}
