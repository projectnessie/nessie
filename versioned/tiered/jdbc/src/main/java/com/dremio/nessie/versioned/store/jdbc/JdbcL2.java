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
package com.dremio.nessie.versioned.store.jdbc;

import java.sql.ResultSet;
import java.util.stream.Stream;

import com.dremio.nessie.tiered.builder.L2;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.ValueType;
import com.dremio.nessie.versioned.store.jdbc.JdbcEntity.SQLChange;

class JdbcL2 extends JdbcBaseValue<L2> implements L2 {

  static final String TREE = "tree";

  static JdbcEntity<L2> createEntity(DatabaseAdapter databaseAdapter, JdbcStoreConfig config) {
    return new JdbcEntity<>(databaseAdapter, ValueType.L2, config,
        JdbcBaseValue.columnMapBuilder()
            .put(TREE, ColumnType.ID_LIST)
            .build(),
        JdbcL2::new,
        (resultSet, consumer) -> produceToConsumer(databaseAdapter, resultSet, consumer));
  }

  private static void produceToConsumer(DatabaseAdapter databaseAdapter, ResultSet resultSet, L2 consumer) {
    baseToConsumer(databaseAdapter, resultSet, consumer)
        .children(databaseAdapter.getIds(resultSet, TREE));
  }

  JdbcL2(Resources resources, SQLChange change, JdbcEntity<L2> entity) {
    super(resources, change, entity);
  }

  @Override
  public L2 children(Stream<Id> ids) {
    getDatabaseAdapter().setIds(change, TREE, ids);
    return this;
  }
}
