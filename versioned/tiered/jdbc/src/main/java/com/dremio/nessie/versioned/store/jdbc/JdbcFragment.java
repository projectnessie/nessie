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

import com.dremio.nessie.tiered.builder.Fragment;
import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.store.ValueType;
import com.dremio.nessie.versioned.store.jdbc.JdbcEntity.SQLChange;

class JdbcFragment extends JdbcBaseValue<Fragment> implements Fragment {

  static final String KEY_LIST = "keys";

  static JdbcEntity<Fragment> createEntity(DatabaseAdapter databaseAdapter, JdbcStoreConfig config) {
    return new JdbcEntity<>(databaseAdapter, ValueType.KEY_FRAGMENT, config,
        JdbcBaseValue.columnMapBuilder()
            .put(KEY_LIST, ColumnType.KEY_LIST)
            .build(),
        JdbcFragment::new,
        (resultSet, consumer) -> produceToConsumer(databaseAdapter, resultSet, consumer));
  }

  private static void produceToConsumer(DatabaseAdapter databaseAdapter, ResultSet resultSet, Fragment consumer) {
    baseToConsumer(databaseAdapter, resultSet, consumer)
        .keys(databaseAdapter.getKeys(resultSet, KEY_LIST));
  }

  JdbcFragment(Resources resources, SQLChange change, JdbcEntity<Fragment> entity) {
    super(resources, change, entity);
  }

  @Override
  public Fragment keys(Stream<Key> keys) {
    getDatabaseAdapter().setKeys(change, KEY_LIST, keys);
    return this;
  }
}
