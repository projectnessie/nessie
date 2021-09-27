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

import static org.projectnessie.versioned.persist.dynamodb.Tables.KEY_NAME;
import static org.projectnessie.versioned.persist.dynamodb.Tables.TABLE_COMMIT_LOG;
import static org.projectnessie.versioned.persist.dynamodb.Tables.TABLE_GLOBAL_LOG;
import static org.projectnessie.versioned.persist.dynamodb.Tables.TABLE_GLOBAL_POINTER;
import static org.projectnessie.versioned.persist.dynamodb.Tables.TABLE_KEY_LISTS;

import java.net.URI;
import java.util.List;
import java.util.stream.Stream;
import org.projectnessie.versioned.persist.adapter.DatabaseConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

public class DynamoDatabaseClient implements DatabaseConnectionProvider<DynamoClientConfig> {
  private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDatabaseClient.class);

  DynamoDbClient client;
  private boolean externallyProvidedClient;

  private DynamoClientConfig config;

  @Override
  public void configure(DynamoClientConfig config) {
    this.config = config;
  }

  @Override
  public void initialize() {
    if (this.client != null) {
      throw new IllegalStateException("Already initialized.");
    }

    if (config instanceof DefaultDynamoClientConfig) {
      DefaultDynamoClientConfig cfg = (DefaultDynamoClientConfig) config;
      DynamoDbClientBuilder clientBuilder =
          DynamoDbClient.builder()
              .httpClient(UrlConnectionHttpClient.create())
              .region(Region.of(cfg.getRegion()));

      if (cfg.getCredentialsProvider() != null) {
        clientBuilder = clientBuilder.credentialsProvider(cfg.getCredentialsProvider());
      }
      if (cfg.getEndpointURI() != null) {
        clientBuilder = clientBuilder.endpointOverride(URI.create(cfg.getEndpointURI()));
      }

      this.externallyProvidedClient = false;
      client = clientBuilder.build();
    } else if (config instanceof ProvidedDynamoClientConfig) {
      ProvidedDynamoClientConfig cfg = (ProvidedDynamoClientConfig) config;
      this.externallyProvidedClient = true;
      this.client = cfg.getDynamoDbClient();
    } else {
      throw new IllegalArgumentException(
          "Must provide a Dynamo-client-configuration of type DefaultDynamoClientConfig or ProvidedDynamoClientConfig.");
    }

    Stream.of(TABLE_GLOBAL_POINTER, TABLE_GLOBAL_LOG, TABLE_COMMIT_LOG, TABLE_KEY_LISTS)
        .forEach(this::createIfMissing);
  }

  @Override
  public void close() {
    if (client != null) {
      try {
        if (!externallyProvidedClient) {
          client.close();
        }
      } finally {
        client = null;
      }
    }
  }

  private void createIfMissing(String name) {
    if (!tableExists(name)) {
      createTable(name);
    }
  }

  private boolean tableExists(String name) {
    try {
      DescribeTableResponse table =
          client.describeTable(DescribeTableRequest.builder().tableName(name).build());
      verifyKeySchema(table.table());
      return true;
    } catch (ResourceNotFoundException e) {
      LOGGER.debug("Didn't find table '{}', going to create one.", name, e);
      return false;
    }
  }

  private void createTable(String name) {
    client.createTable(
        CreateTableRequest.builder()
            .tableName(name)
            .attributeDefinitions(
                AttributeDefinition.builder()
                    .attributeName(KEY_NAME)
                    .attributeType(ScalarAttributeType.S)
                    .build())
            .billingMode(BillingMode.PAY_PER_REQUEST)
            .keySchema(
                KeySchemaElement.builder().attributeName(KEY_NAME).keyType(KeyType.HASH).build())
            .build());
  }

  private static void verifyKeySchema(TableDescription description) {
    List<KeySchemaElement> elements = description.keySchema();

    if (elements.size() == 1) {
      KeySchemaElement key = elements.get(0);
      if (key.attributeName().equals(KEY_NAME)) {
        if (key.keyType() == KeyType.HASH) {
          return;
        }
      }
    }
    throw new IllegalStateException(
        String.format(
            "Invalid key schema for table: %s. Key schema should be a hash partitioned "
                + "attribute with the name 'id'.",
            description.tableName()));
  }
}
