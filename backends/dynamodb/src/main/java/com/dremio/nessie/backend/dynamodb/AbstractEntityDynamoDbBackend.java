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
import com.dremio.nessie.backend.dynamodb.model.Base;
import com.dremio.nessie.model.VersionedWrapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Expression;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.GetItemEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.PageIterable;
import software.amazon.awssdk.enhanced.dynamodb.model.ScanEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.WriteBatch;
import software.amazon.awssdk.enhanced.dynamodb.model.WriteBatch.Builder;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;

abstract class AbstractEntityDynamoDbBackend<T extends Base, M> implements EntityBackend<M> {

  private final DynamoDbClient client;
  private final DynamoDbTable<T> table;
  private final Class<T> clazz;
  private final String namespace;
  private final DynamoDbEnhancedClient mapper;

  public AbstractEntityDynamoDbBackend(DynamoDbClient client,
                                       DynamoDbEnhancedClient mapper,
                                       Class<T> clazz,
                                       String tableName,
                                       String namespace) {
    this.clazz = clazz;
    this.namespace = namespace;
    this.mapper = mapper;
    TableSchema<T> schema = TableSchema.fromBean(clazz);
    table = mapper.table(tableName, schema);
    this.client = client;
  }

  protected abstract T toDynamoDB(VersionedWrapper<M> from);

  protected abstract VersionedWrapper<M> fromDynamoDB(T from);

  @Override
  public VersionedWrapper<M> get(String name) {
    T obj = table.getItem(GetItemEnhancedRequest.builder()
                                                .consistentRead(true)
                                                .key(Key.builder()
                                                        .partitionValue(name)
                                                        .build())
                                                .build());
    if (obj == null) {
      return null;
    }
    return fromDynamoDB(obj);
  }

  @Override
  public VersionedWrapper<M> get(String name, String sortKey) {
    T obj = table.getItem(GetItemEnhancedRequest.builder()
                                                .consistentRead(true)
                                                .key(Key.builder()
                                                        .partitionValue(name)
                                                        .sortValue(sortKey)
                                                        .build())
                                                .build());
    if (obj == null) {
      return null;
    }
    return fromDynamoDB(obj);
  }

  @Override
  public List<VersionedWrapper<M>> getAll(String name,
                                          String namespace,
                                          boolean includeDeleted) {

    ScanEnhancedRequest.Builder scanExpression = ScanEnhancedRequest.builder().consistentRead(true);
    Expression.Builder builder = Expression.builder();
    String expression = "";
    Map<String, AttributeValue> values = new HashMap<>();
    Map<String, String> expressions = new HashMap<>();
    if (namespace != null && !namespace.isEmpty()) {
      expression += "#t = :nspc";
      values.put(":nspc", AttributeValue.builder().s(namespace).build());
      expressions.put("#t", this.namespace);
    }
    if (name != null) {
      if (expression.isEmpty()) {
        expression += "#n = :nval";
      } else {
        expression += " and #n = :nval";
      }
      values.put(":nval", AttributeValue.builder().s(name).build());
      expressions.put("#n", "name");
    }
    if (!expressions.isEmpty()) {
      builder.expressionNames(expressions);
    }
    if (!includeDeleted) {
      if (expression.isEmpty()) {
        expression += "deleted = :delval";
      } else {
        expression += " and deleted = :delval";
      }
      values.put(":delval", AttributeValue.builder().bool(false).build());
    }
    PageIterable<T> results;
    if (!expression.isEmpty()) {
      results = table.scan(scanExpression.filterExpression(
        builder.expression(expression).expressionValues(values).build()
      ).build());
    } else {
      results = table.scan();
    }
    return results.items().stream().map(this::fromDynamoDB).collect(Collectors.toList());
  }

  @Override
  public void create(String name, VersionedWrapper<M> obj) throws IOException {
    try {
      T dynamoTable = toDynamoDB(obj);
      table.putItem(dynamoTable);
    } catch (ConditionalCheckFailedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void update(String name, VersionedWrapper<M> obj) {
    table.updateItem(toDynamoDB(obj));
  }

  @Override
  public void updateAll(Map<String, VersionedWrapper<M>> transaction) {
    Builder<T> builder = WriteBatch.builder(clazz).mappedTableResource(table);
    transaction.values().stream().map(this::toDynamoDB).forEach(builder::addPutItem);
    mapper.batchWriteItem(r -> r.addWriteBatch(builder.build()));
  }

  @Override
  public void remove(String name) {
    table.deleteItem(Key.builder().partitionValue(name).build());
  }

  @Override
  public void remove(String name, String sortKey) {
    table.deleteItem(Key.builder().partitionValue(name).sortValue(sortKey).build());
  }

  @Override
  public void close() {
    client.close();
  }
}
