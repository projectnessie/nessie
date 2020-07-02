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

import com.dremio.nessie.backend.EntityBackend;
import com.dremio.nessie.model.VersionedWrapper;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest.Builder;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;
import software.amazon.awssdk.services.dynamodb.paginators.ScanIterable;

abstract class AbstractEntityDynamoDbBackend<M> implements EntityBackend<M> {


  private final DynamoDbClient client;
  private final String tableName;
  private final boolean versioned;

  public AbstractEntityDynamoDbBackend(DynamoDbClient client, String tableName, boolean versioned) {
    this.client = client;
    this.tableName = tableName;
    this.versioned = versioned;
  }

  protected abstract Map<String, AttributeValue> toDynamoDB(VersionedWrapper<M> from);

  protected abstract VersionedWrapper<M> fromDynamoDB(Map<String, AttributeValue> from);

  @Override
  public VersionedWrapper<M> get(String name) {
    Map<String, AttributeValue> key = new HashMap<>();
    key.put("uuid", AttributeValue.builder().s(name).build());
    GetItemRequest request = GetItemRequest.builder()
                                           .consistentRead(true)
                                           .key(key)
                                           .tableName(tableName)
                                           .build();
    Map<String, AttributeValue> obj = client.getItem(request).item();
    if (obj == null || obj.isEmpty()) {
      return null;
    }
    return fromDynamoDB(obj);
  }

  public List<VersionedWrapper<M>> getAll(boolean includeDeleted) {
    ScanRequest request = ScanRequest.builder().tableName(tableName).build();

    ScanIterable results = client.scanPaginator(request);
    return results.items().stream().map(this::fromDynamoDB).collect(Collectors.toList());
  }

  @Override
  public VersionedWrapper<M> update(String name, VersionedWrapper<M> obj) {
    Map<String, AttributeValue> item = toDynamoDB(obj);
    Builder builder = PutItemRequest.builder()
                                    .tableName(tableName);
    if (versioned) {
      String expression;
      Long version = obj.getVersion();
      if (version == null) {
        item.put("version", AttributeValue.builder().n("1").build());
        expression = "attribute_not_exists(version)";
      } else {
        String newVersion = Long.toString(version + 1);
        item.put("version", AttributeValue.builder().n(newVersion).build());
        expression = "version = :v";
        Map<String, AttributeValue> expressionVals = new HashMap<>();
        expressionVals.put(":v", AttributeValue.builder().n(version.toString()).build());
        builder.expressionAttributeValues(expressionVals);
      }
      builder.conditionExpression(expression);
    }

    client.putItem(builder.item(item).build());
    return get(name);
  }

  @Override
  public void updateAll(Map<String, VersionedWrapper<M>> transaction) {

    Map<String, List<WriteRequest>> items = new HashMap<>();
    List<WriteRequest> writeRequests =
        transaction.values()
                   .stream()
                   .map(x -> PutRequest.builder().item(toDynamoDB(x)).build())
                   .map(x -> WriteRequest.builder().putRequest(x).build())
                   .collect(Collectors.toList());
    items.put(tableName, writeRequests);
    BatchWriteItemRequest request = BatchWriteItemRequest.builder()
                                                         .requestItems(items)
                                                         .build();
    client.batchWriteItem(request);
  }

  @Override
  public void remove(String name) {
    Map<String, AttributeValue> key = new HashMap<>();
    key.put("uuid", AttributeValue.builder().s(name).build());
    DeleteItemRequest request = DeleteItemRequest.builder()
                                                 .tableName(tableName)
                                                 .key(key)
                                                 .build();
    client.deleteItem(request);
  }

  @Override
  public void close() {
    client.close();
  }
}
