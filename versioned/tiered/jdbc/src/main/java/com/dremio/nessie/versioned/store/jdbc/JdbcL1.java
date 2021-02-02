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
import java.util.ArrayList;
import java.util.List;
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

  static JdbcEntity<L1> createEntity(DatabaseAdapter databaseAdapter, JdbcStoreConfig config) {
    Builder<String, ColumnType> columns = JdbcBaseValue.columnMapBuilder()
        .put(METADATA, ColumnType.ID)
        .put(PARENTS, ColumnType.ID_LIST)
        .put(IS_CHECKPOINT, ColumnType.BOOL)
        .put(ORIGIN, ColumnType.ID)
        .put(DISTANCE, ColumnType.LONG)
        .put(MUTATIONS, ColumnType.KEY_MUTATION_LIST)
        .put(FRAGMENTS, ColumnType.ID_LIST);
    TreeColumns.forEntity(columns);

    return new JdbcEntity<>(databaseAdapter, ValueType.L1, config,
            columns.build(),
        JdbcL1::new,
        (resultSet, consumer) -> produceToConsumer(databaseAdapter, resultSet, consumer));
  }

  private static void produceToConsumer(DatabaseAdapter databaseAdapter, ResultSet resultSet, L1 consumer) {
    baseToConsumer(databaseAdapter, resultSet, consumer)
        .commitMetadataId(databaseAdapter.getId(resultSet, METADATA))
        .ancestors(databaseAdapter.getIds(resultSet, PARENTS));
    List<Mutation> mutations = new ArrayList<>();
    databaseAdapter.getMutations(resultSet, MUTATIONS, mutations::add);
    consumer = consumer.keyMutations(mutations.stream())
        .children(TreeColumns.toConsumer(databaseAdapter, resultSet));
    if (databaseAdapter.getBoolean(resultSet, IS_CHECKPOINT)) {
      consumer.completeKeyList(databaseAdapter.getIds(resultSet, FRAGMENTS));
    } else {
      consumer.incrementalKeyList(
          databaseAdapter.getId(resultSet, ORIGIN),
          Ints.saturatedCast(databaseAdapter.getLong(resultSet, DISTANCE))
      );
    }
  }

  JdbcL1(Resources resources, SQLChange change, JdbcEntity<L1> entity) {
    super(resources, change, entity);
  }

  @Override
  public L1 commitMetadataId(Id id) {
    getDatabaseAdapter().setId(change, METADATA, id);
    return this;
  }

  @Override
  public L1 ancestors(Stream<Id> ancestors) {
    getDatabaseAdapter().setIds(change, PARENTS, ancestors);
    return this;
  }

  @Override
  public L1 children(Stream<Id> children) {
    TreeColumns.children(children, getDatabaseAdapter(), change);
    return this;
  }

  @Override
  public L1 keyMutations(Stream<Mutation> keyMutations) {
    getDatabaseAdapter().setMutations(change, MUTATIONS, keyMutations);
    return this;
  }

  @Override
  public L1 incrementalKeyList(Id checkpointId, int distanceFromCheckpoint) {
    getDatabaseAdapter().setBoolean(change, IS_CHECKPOINT, false);
    getDatabaseAdapter().setId(change, ORIGIN, checkpointId);
    getDatabaseAdapter().setLong(change, DISTANCE, (long) distanceFromCheckpoint);
    return this;
  }

  @Override
  public L1 completeKeyList(Stream<Id> fragmentIds) {
    getDatabaseAdapter().setBoolean(change, IS_CHECKPOINT, true);
    getDatabaseAdapter().setIds(change, FRAGMENTS, fragmentIds);
    return this;
  }
}
