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

import com.dremio.iceberg.backend.dynamodb.model.Table;
import com.google.common.base.Joiner;
import java.net.URI;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.mapper.BeanTableSchema;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.ResourceInUseException;

/**
 * create all tables the Alley Server needs.
 * For testing only. In prod use CloudFormation or equivalent
 */
public class CreateTables {

  private final String region;
  private final String endpoint;

  public CreateTables(String region, String endpoint) {
    this.region = region;
    this.endpoint = endpoint;
  }

  public void create(String tableName) throws ClassNotFoundException {
    Class<?> clazz = Class.forName("com.dremio.iceberg.backend.dynamodb.model." + tableName);
    DynamoDbEnhancedClient ec = DynamoDbEnhancedClient.builder()
                                                      .dynamoDbClient(client())
                                                      .build();
    BeanTableSchema<Table> schema = TableSchema.fromBean(
        com.dremio.iceberg.backend.dynamodb.model.Table.class);
    DynamoDbTable<Table> table = ec.table(
        "IcebergAlley" + tableName + "s",
        schema);
    table.createTable();
  }

  public String list() {
    return Joiner.on(", ").join(client().listTables().tableNames());
  }

  public DynamoDbClient client() {
    return DynamoDbClient.builder()
                         .endpointOverride(URI.create(endpoint))
                         .region(Region.of(region))
                         .build();
  }

  public static void main(String[] args) throws ClassNotFoundException {
    String region = "us-west-2";
    String endpoint = "http://localhost:8000";
    CreateTables tables = new CreateTables(region, endpoint);
    for (String table : new String[] {"Table", "Tag", "User"}) {
      try {
        tables.create(table);
      } catch (ResourceInUseException e) {
        System.out.println(table + " already exists");
      }
    }
    System.out.println(tables.list());
  }
}
