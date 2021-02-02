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

import com.dremio.nessie.tiered.builder.L3;
import com.dremio.nessie.versioned.store.KeyDelta;
import com.dremio.nessie.versioned.store.ValueType;
import com.dremio.nessie.versioned.store.jdbc.JdbcEntity.SQLChange;

class JdbcL3 extends JdbcBaseValue<L3> implements L3 {

  static final String TREE = "tree";

  static JdbcEntity<L3> createEntity(DatabaseAdapter databaseAdapter, JdbcStoreConfig config) {
    return new JdbcEntity<>(databaseAdapter, ValueType.L3, config,
        JdbcBaseValue.columnMapBuilder()
            .put(TREE, ColumnType.KEY_DELTA_LIST)
            .build(),
        JdbcL3::new,
        (resultSet, consumer) -> produceToConsumer(databaseAdapter, resultSet, consumer));
  }

  private static void produceToConsumer(DatabaseAdapter databaseAdapter, ResultSet resultSet, L3 consumer) {
    baseToConsumer(databaseAdapter, resultSet, consumer)
        .keyDelta(databaseAdapter.getKeyDeltas(resultSet, TREE));
  }

  JdbcL3(Resources resources, SQLChange change, JdbcEntity<L3> entity) {
    super(resources, change, entity);
  }

  @Override
  public L3 keyDelta(Stream<KeyDelta> keyDelta) {
    getDatabaseAdapter().setKeyDeltas(change, TREE, keyDelta);
    return this;
  }
}
