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

import com.dremio.iceberg.backend.EntityBackend;
import com.dremio.iceberg.backend.dynamodb.model.Base;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Expression;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.GetItemEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.PageIterable;
import software.amazon.awssdk.enhanced.dynamodb.model.ScanEnhancedRequest;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

abstract class AbstractEntityDynamoDBBackend<T extends Base, M> implements EntityBackend<M>, AutoCloseable {
  private final DynamoDbClient client;
  private final Class<T> clazz;
  private final TableSchema<T> schema;
  private final DynamoDbTable<T> table;

  public AbstractEntityDynamoDBBackend(DynamoDbClient client,
                                       DynamoDbEnhancedClient mapper,
                                       Class<T> clazz,
                                       String tableName) {
    schema = TableSchema.fromBean(clazz);
    table = mapper.table(tableName, schema);
    this.client = client;
    this.clazz = clazz;
  }

  protected abstract T toDynamoDB(M from);

  protected abstract M fromDynamoDB(T from);

  @Override
  public M get(String name) {
    T obj = table.getItem(GetItemEnhancedRequest.builder().consistentRead(true)
      .key(Key.builder().partitionValue(name).build()).build());
    if (obj == null) {
      return null;
    }
    return fromDynamoDB(obj);
  }

  @Override
  public List<M> getAll(boolean includeDeleted) {
    return getAll(null, includeDeleted);
  }

  @Override
  public List<M> getAll(String namespace, boolean includeDeleted) {

    ScanEnhancedRequest.Builder scanExpression = ScanEnhancedRequest.builder().consistentRead(true);
    Expression.Builder builder = Expression.builder();
    String expression = "";
    Map<String, AttributeValue> values = Maps.newHashMap();
    if (namespace != null && !namespace.isEmpty()) {
      expression += "namespace = :nspc";
      values.put(":nspc", AttributeValue.builder().s(namespace).build());
    }
    if (!includeDeleted) {
      if (expression.isEmpty()) {
        expression += "deleted = :delval";
      } else {
        expression += " and deleted = :delval";
      }
      values.put(":value", AttributeValue.builder().n("0").build());
    }
    PageIterable<T> results = null;
    if (!expression.isEmpty()) {
      results = table.scan(scanExpression.filterExpression(builder.build()).build());
    } else {
      results = table.scan();
    }
    List<M> items = Lists.newArrayList();
    for (T item: results.items()) {
      items.add(fromDynamoDB(item));
    }
    return items;
  }

  @Override
  public void create(String name, M obj) {
    T dynamoTable = toDynamoDB(obj);
    table.putItem(dynamoTable);
  }

  @Override
  public void update(String name, M obj) {
    table.updateItem(toDynamoDB(obj));
  }

  @Override
  public void remove(String name) {
    table.deleteItem(Key.builder().partitionValue(name).build());
  }

  @Override
  public void close() {
    client.close();
  }
}
