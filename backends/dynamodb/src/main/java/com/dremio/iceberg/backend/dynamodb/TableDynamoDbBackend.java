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
import com.dremio.iceberg.model.VersionedWrapper;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/**
 * dynamodb backend for Table.
 */
public class TableDynamoDbBackend
    extends AbstractEntityDynamoDbBackend<Table, com.dremio.iceberg.model.Table> {

  public TableDynamoDbBackend(DynamoDbClient client, DynamoDbEnhancedClient mapper) {
    super(client, mapper, Table.class, "IcebergAlleyTables");
  }

  @Override
  protected Table toDynamoDB(VersionedWrapper<com.dremio.iceberg.model.Table> from) {
    return Table.fromModelTable(from);
  }

  @Override
  protected VersionedWrapper<com.dremio.iceberg.model.Table> fromDynamoDB(Table from) {
    return from.toModelTable();
  }
}
