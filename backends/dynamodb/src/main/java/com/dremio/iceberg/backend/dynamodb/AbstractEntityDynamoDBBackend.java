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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.dremio.iceberg.backend.EntityBackend;
import com.dremio.iceberg.backend.dynamodb.model.Base;
import com.google.common.collect.Maps;

public abstract class AbstractEntityDynamoDBBackend<T extends Base, M> implements EntityBackend<M>, AutoCloseable {
  private final AmazonDynamoDB client;
  private final DynamoDBMapperConfig config;
  private final DynamoDBMapper mapper;
  private Class<T> clazz;

  public AbstractEntityDynamoDBBackend(AmazonDynamoDB client,
                                       DynamoDBMapperConfig config,
                                       DynamoDBMapper mapper,
                                       Class<T> clazz) {
    this.client = client;
    this.config = config;
    this.mapper = mapper;
    this.clazz = clazz;
  }

  protected abstract T toDynamoDB(M from);

  protected abstract M fromDynamoDB(T from);

  @Override
  public M get(String name) {
    T table = mapper.load(clazz, name, config);
    if (table == null) {
      return null;
    }
    return fromDynamoDB(table);
  }

  @Override
  public List<M> getAll(boolean includeDeleted) {
    return getAll(null, includeDeleted);
  }

  @Override
  public List<M> getAll(String namespace, boolean includeDeleted) {

    DynamoDBScanExpression scanExpression = new DynamoDBScanExpression();
    if (namespace != null && !namespace.isEmpty()) {
      Condition condition = new Condition().withComparisonOperator(ComparisonOperator.EQ)
        .withAttributeValueList(new AttributeValue().withS(namespace));
      scanExpression.addFilterCondition("namespace", condition);
    }
    if (!includeDeleted) {
      Condition condition = new Condition().withComparisonOperator(ComparisonOperator.EQ)
        .withAttributeValueList(new AttributeValue().withN("0"));
      scanExpression.addFilterCondition("deleted", condition);
    }

    List<T> scanResult = mapper.scan(clazz, scanExpression, config); //todo make parallel?
    return scanResult.stream() .map(this::fromDynamoDB) .collect(Collectors.toList());
  }

  @Override
  public void create(String name, M table) {
    T dynamoTable = toDynamoDB(table);
    mapper.save(dynamoTable, config);
  }

  @Override
  public void update(String name, M table) {
    create(name, table);
  }

  @Override
  public void remove(String name) {
    mapper.delete(toDynamoDB(get(name)), config);
  }

  @Override
  public void close() throws Exception {
    client.shutdown();
  }
}
