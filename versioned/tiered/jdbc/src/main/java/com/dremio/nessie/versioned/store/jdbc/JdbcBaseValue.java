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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

import com.dremio.nessie.tiered.builder.BaseValue;
import com.dremio.nessie.versioned.impl.condition.ConditionExpression;
import com.dremio.nessie.versioned.impl.condition.ExpressionPath;
import com.dremio.nessie.versioned.store.ConditionFailedException;
import com.dremio.nessie.versioned.store.Entity;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.NotFoundException;
import com.dremio.nessie.versioned.store.jdbc.JdbcEntity.SQLChange;
import com.dremio.nessie.versioned.store.jdbc.JdbcEntity.SQLDelete;
import com.google.common.collect.ImmutableMap;

abstract class JdbcBaseValue<C extends BaseValue<C>> implements BaseValue<C> {

  static final String DT = "dt";

  final JdbcEntity<C> entity;
  final Resources resources;
  final SQLChange change;

  static <C extends BaseValue<C>> C produceToConsumer(DatabaseAdapter databaseAdapter,
      ResultSet resultSet, C consumer) throws SQLException {
    return consumer.id(databaseAdapter.getId(resultSet, JdbcEntity.ID))
        .dt(databaseAdapter.getDt(resultSet, DT));
  }

  /**
   * Functional interface to supply the constructor-reference.
   * @param <C> consumer-type
   */
  @FunctionalInterface
  interface JdbcValueSupplier<C extends BaseValue<C>> {
    JdbcBaseValue<C> createNewValue(Resources resources, SQLChange change, JdbcEntity<C> entity);
  }

  JdbcBaseValue(Resources resources, SQLChange change, JdbcEntity<C> entity) {
    this.entity = entity;
    this.resources = resources;
    this.change = change;
  }

  protected static ImmutableMap.Builder<String, ColumnType> columnMapBuilder() {
    return ImmutableMap.<String, ColumnType>builder()
        .put(JdbcEntity.ID, ColumnType.ID)
        .put(DT, ColumnType.DT);
  }

  @SuppressWarnings("unchecked")
  @Override
  public C id(Id id) {
    entity.databaseAdapter.setId(change, JdbcEntity.ID, id);
    return (C) this;
  }

  @SuppressWarnings("unchecked")
  @Override
  public C dt(long dt) {
    entity.databaseAdapter.setDt(change, DT, dt);
    return (C) this;
  }

  /**
   * Used in {@link #executeUpdates(ExecuteUpdateStrategy, Conditions, Id)} to specify its exact behavior.
   */
  enum ExecuteUpdateStrategy {
    INSERT(false, true, false),
    UPSERT(true, true, false),
    UPSERT_MUST_DELETE(true, true, true),
    DELETE(true, false, true);

    final boolean delete;
    final boolean change;
    final boolean deleteMustSucceed;

    ExecuteUpdateStrategy(boolean delete, boolean change, boolean deleteMustSucceed) {
      this.delete = delete;
      this.change = change;
      this.deleteMustSucceed = deleteMustSucceed;
    }
  }

  /**
   * Handles the different execution "strategies" from {@link JdbcStore}'s delete/update/put/putIfAbsent/save
   * operations.
   * @param strategy strategy to use
   * @param conditions conditions for the change
   * @param id id (only relevant when an (additional) DELETE is executed (defined by {@code strategy}
   * @return number or rows affected
   * @throws SQLException propagates SQL exceptions
   */
  int executeUpdates(ExecuteUpdateStrategy strategy, Conditions conditions, Id id) throws SQLException {
    int r = -1;
    if (strategy.delete) {
      SQLDelete delete;
      if (change instanceof SQLDelete) {
        delete = (SQLDelete) change;
      } else {
        delete = entity.newDelete();
      }
      conditions.applicators.forEach(delete::addCondition);
      delete.prepareStatement(resources);
      r = delete.executeUpdate();
      if (r > 1) {
        throw new IllegalArgumentException(delete.statement.toString() + " covers too many rows: " + r);
      }
      if (strategy.deleteMustSucceed && r != 1) {
        try {
          PreparedStatement checkStmt = resources
              .add(resources.connection.prepareStatement("SELECT "
                  + JdbcEntity.ID
                  + " FROM "
                  + entity.tableName
                  + " WHERE "
                  + JdbcEntity.ID + " = ?"));
          entity.databaseAdapter.setId(checkStmt, 1, id);
          ResultSet rs = resources.add(checkStmt.executeQuery());
          throw rs.next()
              ? new ConditionFailedException(conditions.toString())
              : new NotFoundException(conditions.toString());
        } catch (SQLException e) {
          throw new RuntimeException(delete.statement.toString(), e);
        }
      }
    }
    if (strategy.change) {
      change.prepareStatement(resources);
      r = change.executeUpdate();
    }
    return r;
  }

  /**
   * Used to set a condition-value for the given expression-path, can be overridden to specialize
   * the handling for some value-types and attributes.
   */
  void conditionValue(UpdateContext updateContext, Conditions conditions, ExpressionPath path, Entity value) {
    conditions.applicators.put(
        entity.equalsClause(updateContext, path),
        (stmt, index) -> entity.databaseAdapter
            .setEntity(stmt, index, entity.propertyType(path), value));
  }

  /**
   * Used to set a value for the given expression-path, can be overridden to specialize
   * the handling for some value-types and attributes.
   */
  void updateValue(UpdateContext updateContext, ExpressionPath path, Entity value) {
    updateContext.change.setColumn(
        entity.equalsClause(updateContext, path),
        (stmt, index) -> entity.databaseAdapter
            .setEntity(stmt, index, entity.propertyType(path), value));
  }

  /**
   * Used to implement a "size(attribute)" function for the given expression-path, can be overridden to specialize
   * the handling for some value-types and attributes. The default throws {@link UnsupportedOperationException}.
   */
  public void conditionSize(UpdateContext updateContext,
      Optional<ConditionExpression> condition,
      ExpressionPath path, int expectedSize) {
    throw new UnsupportedOperationException("conditionSize for " + path + " not supported");
  }

  /**
   * Used to implement a "append-to-list" function for the given expression-path, can be overridden to specialize
   * the handling for some value-types and attributes. The default throws {@link UnsupportedOperationException}.
   */
  void updateListAppend(UpdateContext updateContext, ExpressionPath path, Entity value) {
    throw new UnsupportedOperationException("updateListAppend for " + path + " not supported");
  }

  /**
   * Used to implement a "remove value" functionality for the given expression-path, can be overridden to specialize
   * the handling for some value-types and attributes. The default implementation sets the column to {@code NULL}.
   */
  void removeValue(UpdateContext updateContext, ExpressionPath path) {
    updateContext.change.setColumn(
        entity.equalsClause(updateContext, path),
        (stmt, index) -> entity.databaseAdapter.setNull(stmt, index, entity.propertyType(path)));
  }
}
