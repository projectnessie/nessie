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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.dremio.iceberg.backend.Backend;
import com.dremio.iceberg.model.Table;
import com.dremio.iceberg.model.Tables;
import com.google.common.collect.Maps;

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
    AmazonDynamoDBClientBuilder clientBuilder = AmazonDynamoDBClientBuilder.standard()
      .withRegion(region);
    if (endpoint != null) {
      clientBuilder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, region));
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
  public Table get(String name) {
    com.dremio.iceberg.backend.dynamodb.model.Table table =
      mapper.load(com.dremio.iceberg.backend.dynamodb.model.Table.class, name, config);
    if (table == null) {
      return null;
    }
    return table.toModelTable();
  }

  @Override
  public Tables getAll(String namespace, boolean includeDeleted) {
    Map<String, AttributeValue> eav = Maps.newHashMap();
    eav.put(":val1", new AttributeValue().withS(namespace));

    DynamoDBScanExpression scanExpression = new DynamoDBScanExpression();

    if (namespace != null) {
      scanExpression = scanExpression
        .withFilterExpression("namespace = :val1").withExpressionAttributeValues(eav);
    }

    List<com.dremio.iceberg.backend.dynamodb.model.Table> scanResult =
      mapper.parallelScan(com.dremio.iceberg.backend.dynamodb.model.Table.class,
        scanExpression, 4); //todo
    return new Tables(scanResult.stream()
      .map(com.dremio.iceberg.backend.dynamodb.model.Table::toModelTable)
      .filter(t -> includeDeleted || !t.isDeleted())
      .collect(Collectors.toList()));
  }

  @Override
  public void create(String name, Table table) {
    com.dremio.iceberg.backend.dynamodb.model.Table dynamoTable =
      com.dremio.iceberg.backend.dynamodb.model.Table.fromModelTable(table);
    mapper.save(dynamoTable, config);
  }

  @Override
  public void update(String name, Table table) {
    create(name, table);
  }

  @Override
  public void remove(String name) {
    mapper.delete(com.dremio.iceberg.backend.dynamodb.model.Table.fromModelTable(get(name)), config);
  }

  @Override
  public void close() throws Exception {
    client.shutdown();
  }
}
