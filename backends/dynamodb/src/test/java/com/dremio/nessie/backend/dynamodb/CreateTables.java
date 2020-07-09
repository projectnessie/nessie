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

package com.dremio.nessie.backend.dynamodb;

import com.google.common.base.Joiner;
import java.net.URI;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ResourceInUseException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

/**
 * create all tables the Nessie Server needs. For testing only. In prod use CloudFormation or
 * equivalent
 */
public class CreateTables {

  private final String region;
  private final String endpoint;

  public CreateTables(String region, String endpoint) {
    this.region = region;
    this.endpoint = endpoint;
  }

  /**
   * create a table in dynamo db for the given class name.
   */
  public void create(String tableName) {
    String table = "Nessie" + tableName + "Database";
    CreateTableRequest request =
        CreateTableRequest.builder()
                          .attributeDefinitions(AttributeDefinition.builder()
                                                                   .attributeName("uuid")
                                                                   .attributeType(
                                                                     ScalarAttributeType.S)
                                                                   .build())
                          .keySchema(KeySchemaElement.builder()
                                                     .attributeName("uuid")
                                                     .keyType(KeyType.HASH)
                                                     .build())
                          .provisionedThroughput(ProvisionedThroughput.builder()
                                                                      .readCapacityUnits(10L)
                                                                      .writeCapacityUnits(10L)
                                                                      .build())
                          .tableName(table)
                          .build();
    client().createTable(request);
  }

  /**
   * list all tables currently in dynamodb.
   */
  public String list() {
    return Joiner.on(", ").join(client().listTables().tableNames());
  }

  private DynamoDbClient client() {
    return DynamoDbClient.builder()
                         .endpointOverride(URI.create(endpoint))
                         .region(Region.of(region))
                         .build();
  }

  /**
   * Create all tables needed for dynamodb to work as a backend.
   *
   * <p>
   * This should be used for only testing as the tables should be created w/ CloudFormation (or
   * equivalent) for production.
   * </p>
   */
  public static void main(String[] args) {
    String region = "us-west-2";
    String endpoint = "http://localhost:8000";
    CreateTables tables = new CreateTables(region, endpoint);
    for (String table : new String[]{"GitObject", "GitRef"}) {
      try {
        tables.create(table);
      } catch (ResourceInUseException e) {
        System.out.println(table + " already exists");
      }
    }
    System.out.println(tables.list());
  }
}
