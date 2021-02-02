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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.jdbc.JdbcEntity.SQLChange;
import com.google.common.collect.ImmutableMap.Builder;

/**
 * Helper/utility class to deal with {@code tree_*} columns.
 */
final class TreeColumns {

  static final String[] TREE_COLUMNS = IntStream.range(0, 43).mapToObj(i -> "tree_" + i).toArray(String[]::new);

  static void forEntity(Builder<String, ColumnType> columns) {
    for (String treeColumn : TREE_COLUMNS) {
      columns.put(treeColumn, ColumnType.ID);
    }
  }

  static void children(Stream<Id> children, DatabaseAdapter databaseAdapter, SQLChange change) {
    AtomicInteger count = children.collect(Collector.of(
        AtomicInteger::new,
        (i, id) -> {
          int index = i.getAndIncrement();
          if (index >= 43) {
            throw new IllegalArgumentException(String.format("Expected %d ids, got %d", TREE_COLUMNS.length, index));
          }
          databaseAdapter.setId(change, TREE_COLUMNS[index], id);
        },
        (i1, i2) -> new AtomicInteger(i1.get() + i2.get())
    ));
    if (count.get() != TREE_COLUMNS.length) {
      throw new IllegalArgumentException(String.format("Expected %d ids, got %d", TREE_COLUMNS.length, count.get()));
    }
  }

  static Stream<Id> toConsumer(DatabaseAdapter databaseAdapter, ResultSet resultSet) {
    // Need to eagerly fetch the values from the result-set, so buffer in a `List`
    return Stream.of(TREE_COLUMNS).map(c -> databaseAdapter.getId(resultSet, c)).collect(Collectors.toList()).stream();
  }
}
