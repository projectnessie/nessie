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
import java.sql.SQLException;

import com.dremio.nessie.tiered.builder.BaseWrappedValue;
import com.dremio.nessie.versioned.store.ValueType;
import com.dremio.nessie.versioned.store.jdbc.JdbcEntity.SQLChange;
import com.google.protobuf.ByteString;

class JdbcWrappedValue<C extends BaseWrappedValue<C>> extends JdbcBaseValue<C> implements BaseWrappedValue<C> {

  static final String VALUE = "value";

  static <V extends BaseWrappedValue<V>> JdbcEntity<V> createEntity(ValueType<V> value, JdbcStoreConfig config) {
    return new JdbcEntity<>(config.getDialect(), value, config,
        JdbcBaseValue.columnMapBuilder()
            .put(VALUE, ColumnType.BINARY)
            .build(),
        JdbcWrappedValue::new,
        (r, c) -> produceToConsumer(config.getDialect(), r, c));
  }

  private static <V extends BaseWrappedValue<V>> void produceToConsumer(Dialect dialect,
      ResultSet resultSet, V consumer) throws SQLException {
    JdbcBaseValue.produceToConsumer(dialect, resultSet, consumer)
        .value(dialect.getBinary(resultSet, VALUE));
  }

  JdbcWrappedValue(Resources resources, SQLChange change, JdbcEntity<C> entity) {
    super(resources, change, entity);
  }

  @SuppressWarnings("unchecked")
  @Override
  public C value(ByteString value) {
    entity.dialect.setBinary(change, VALUE, value);
    return (C) this;
  }
}
