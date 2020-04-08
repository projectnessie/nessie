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
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.google.common.base.Joiner;

public class CreateTables {

  private final String region;
  private final String endpoint;

  public CreateTables(String region, String endpoint) {
    this.region = region;
    this.endpoint = endpoint;
  }

  private AmazonDynamoDB client() {
    AmazonDynamoDBClientBuilder clientBuilder = AmazonDynamoDBClientBuilder.standard();
    if (endpoint != null) {
      clientBuilder = clientBuilder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, region));
    } else {
      clientBuilder = clientBuilder.withRegion(region);
    }
    return clientBuilder.build();
  }

  public void create(String table) throws ClassNotFoundException {
    Class<?> clazz = Class.forName("com.dremio.iceberg.backend.dynamodb.model." + table);
    AmazonDynamoDB client = client();
    DynamoDBMapper mapper = new DynamoDBMapper(client);
    CreateTableRequest req = mapper.generateCreateTableRequest(clazz);
    req.setProvisionedThroughput(new ProvisionedThroughput(5L, 5L));
    client.createTable(req);
    client.shutdown();
  }

  public String list() {
    return Joiner.on(", ").join(client().listTables().getTableNames());
  }

  public static void main(String[] args) throws ClassNotFoundException {
    String region = "us-west-2";
    String endpoint = "http://localhost:8000";
    CreateTables tables = new CreateTables(region, endpoint);
    for(String table: new String[]{"Table"}) {
      try {
        tables.create(table);
      } catch (ResourceInUseException e) {
        System.out.println(table + " already exists");
      }
    }
    System.out.println(tables.list());
  }
}
