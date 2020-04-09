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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.dremio.iceberg.backend.dynamodb.model.Table;

public class TableDynamoDBBackend extends AbstractEntityDynamoDBBackend<Table, com.dremio.iceberg.model.Table> {
  public TableDynamoDBBackend(AmazonDynamoDB client,
                              DynamoDBMapperConfig config,
                              DynamoDBMapper mapper,
                              Class<Table> clazz) {
    super(client, config, mapper, clazz);
  }

  @Override
  protected Table toDynamoDB(com.dremio.iceberg.model.Table from) {
    return Table.fromModelTable(from);
  }

  @Override
  protected com.dremio.iceberg.model.Table fromDynamoDB(Table from) {
    return from.toModelTable();
  }
}
