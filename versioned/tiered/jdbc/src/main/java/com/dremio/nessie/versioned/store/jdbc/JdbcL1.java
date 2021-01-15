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
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.dremio.nessie.tiered.builder.L1;
import com.dremio.nessie.versioned.Key.Mutation;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.ValueType;
import com.dremio.nessie.versioned.store.jdbc.JdbcEntity.SQLChange;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.primitives.Ints;

class JdbcL1 extends JdbcBaseValue<L1> implements L1 {

  static final String MUTATIONS = "mutations";
  static final String FRAGMENTS = "fragments";
  static final String IS_CHECKPOINT = "chk";
  static final String ORIGIN = "origin";
  static final String DISTANCE = "dist";
  static final String PARENTS = "parents";
  static final String METADATA = "metadata";

  static final String[] TREE_COLUMNS = IntStream.range(0, 43).mapToObj(i -> "tree_" + i).toArray(String[]::new);

  static JdbcEntity<L1> createEntity(JdbcStoreConfig config) {
    Builder<String, ColumnType> columns = JdbcBaseValue.columnMapBuilder()
        .put(METADATA, ColumnType.ID)
        .put(PARENTS, ColumnType.ID_LIST)
        .put(IS_CHECKPOINT, ColumnType.BOOL)
        .put(ORIGIN, ColumnType.ID)
        .put(DISTANCE, ColumnType.LONG)
        .put(MUTATIONS, ColumnType.KEY_MUTATION_LIST)
        .put(FRAGMENTS, ColumnType.ID_LIST);
    for (String treeColumn : TREE_COLUMNS) {
      columns.put(treeColumn, ColumnType.ID);
    }

    return new JdbcEntity<>(config.getDialect(), ValueType.L1, config,
            columns.build(),
        JdbcL1::new,
        (r, c) -> produceToConsumer(config.getDialect(), r, c));
  }

  private static void produceToConsumer(Dialect dialect, ResultSet resultSet, L1 consumer) throws SQLException {
    consumer = JdbcBaseValue.produceToConsumer(dialect, resultSet, consumer)
        .commitMetadataId(dialect.getId(resultSet, METADATA))
        .ancestors(dialect.getIds(resultSet, PARENTS));
    List<Mutation> mutations = new ArrayList<>();
    dialect.getMutations(resultSet, MUTATIONS, mutations::add);
    consumer = consumer.keyMutations(mutations.stream())
        .children(Stream.of(TREE_COLUMNS).map(c -> {
          try {
            return dialect.getId(resultSet, c);
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        }).collect(Collectors.toList()).stream());
    if (dialect.getBool(resultSet, IS_CHECKPOINT)) {
      consumer.completeKeyList(dialect.getIds(resultSet, FRAGMENTS));
    } else {
      consumer.incrementalKeyList(
          dialect.getId(resultSet, ORIGIN),
          Ints.saturatedCast(dialect.getLong(resultSet, DISTANCE))
      );
    }
  }

  JdbcL1(Resources resources, SQLChange change, JdbcEntity<L1> entity) {
    super(resources, change, entity);
  }

  @Override
  public L1 commitMetadataId(Id id) {
    entity.dialect.setId(change, METADATA, id);
    return this;
  }

  @Override
  public L1 ancestors(Stream<Id> ancestors) {
    entity.dialect.setIds(change, PARENTS, ancestors);
    return this;
  }

  @Override
  public L1 children(Stream<Id> children) {
    List<Id> ch = children.collect(Collectors.toList());
    if (ch.size() != TREE_COLUMNS.length) {
      throw new IllegalArgumentException("Expected " + TREE_COLUMNS.length + " children, but got " + ch.size());
    }
    for (int i = 0; i < TREE_COLUMNS.length; i++) {
      entity.dialect.setId(change, TREE_COLUMNS[i], ch.get(i));
    }
    return this;
  }

  @Override
  public L1 keyMutations(Stream<Mutation> keyMutations) {
    entity.dialect.setMutations(change, MUTATIONS, keyMutations);
    return this;
  }

  @Override
  public L1 incrementalKeyList(Id checkpointId, int distanceFromCheckpoint) {
    entity.dialect.setBool(change, IS_CHECKPOINT, false);
    entity.dialect.setId(change, ORIGIN, checkpointId);
    entity.dialect.setLong(change, DISTANCE, (long) distanceFromCheckpoint);
    return this;
  }

  @Override
  public L1 completeKeyList(Stream<Id> fragmentIds) {
    entity.dialect.setBool(change, IS_CHECKPOINT, true);
    entity.dialect.setIds(change, FRAGMENTS, fragmentIds);
    return this;
  }
}
