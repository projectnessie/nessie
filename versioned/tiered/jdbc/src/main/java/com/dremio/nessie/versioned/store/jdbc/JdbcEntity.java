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
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.nessie.tiered.builder.BaseValue;
import com.dremio.nessie.versioned.impl.condition.ExpressionPath;
import com.dremio.nessie.versioned.impl.condition.ExpressionPath.PathSegment;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.ValueType;
import com.dremio.nessie.versioned.store.jdbc.JdbcBaseValue.JdbcValueSupplier;

/**
 * Describes the JDBC-store entity for a {@link ValueType} and provides functionality to supply
 * {@link BaseValue} implementations to persist values to a database (aka saving) and functionality
 * to provide values from the database to a supplied {@link BaseValue} implementation (aka loading).
 * @param <C> consumer-type
 */
class JdbcEntity<C extends BaseValue<C>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcEntity.class);

  static final String ID = "id";

  /**
   * Functional interface to provide the method that does convert a SQL row from a JDBC
   * {@link ResultSet} to an externally provided implementation of a {@link BaseValue}.
   * @param <C> consumer-type.
   */
  @FunctionalInterface
  interface ProduceToConsumer<C> {
    void produceToConsumer(ResultSet resultSet, C consumer) throws SQLException;
  }

  final Dialect dialect;
  final String tableName;
  final Map<String, ColumnType> columnTypeMap;
  final String queryAll;
  final String queryIds;
  final JdbcValueSupplier<C> jdbcValueSupplier;
  final ProduceToConsumer<C> produceToConsumer;

  private static final String IDS_MARKER = "__IDS__";

  JdbcEntity(Dialect dialect, ValueType<C> type, JdbcStoreConfig config, Map<String, ColumnType> columnTypeMap,
      JdbcValueSupplier<C> jdbcValueSupplier, ProduceToConsumer<C> produceToConsumer) {
    this.dialect = dialect;
    String tableName = type.getTableName(config.getTablePrefix());
    if (config.getSchema() != null) {
      tableName = config.getSchema() + '.' + tableName;
    }
    this.tableName = tableName;
    this.columnTypeMap = columnTypeMap;
    this.jdbcValueSupplier = jdbcValueSupplier;
    this.produceToConsumer = produceToConsumer;

    String allColumns = String.join(", ", columnTypeMap.keySet());

    String queryCondition = Stream.of(Optional.of(ID + " IN (" + IDS_MARKER + ")"))
        .map(Optional::get)
        .collect(Collectors.joining(" AND "));

    this.queryAll = "SELECT "
        + allColumns
        + " FROM "
        + tableName;

    this.queryIds = "SELECT "
        + allColumns
        + " FROM "
        + tableName
        + " WHERE "
        + queryCondition;
  }

  void buildUpdate(Resources resources, UpdateContext updates, Conditions conditions) throws SQLException {
    conditions.applicators.forEach(updates.change::addCondition);

    updates.change.prepareStatement(resources);
  }

  String equalsClause(UpdateContext updateContext, ExpressionPath path) {
    ExpressionPath.NameSegment root = path.getRoot();
    String rootName = root.getName();
    if (root.getChild().isPresent()) {
      ExpressionPath.PathSegment child = root.getChild().get();
      if (!child.isPosition()) {
        throw new IllegalArgumentException("Unsupported expression-path " + path);
      }

      int position = child.asPosition().getPosition();
      position += updateContext.adjustedIndex(rootName);

      if (!child.getChild().isPresent()) {
        // e.g. "tree[0]"
        if ("tree".equals(rootName)) {
          return rootName + "_" + position;
        }
      } else {
        PathSegment childAttr = child.getChild().get();

        if (childAttr.isName()) {
          String childAttrName = childAttr.asName().getName();

          return "c_" + childAttrName + "_" + position;
        }
      }

      throw new IllegalArgumentException("Unsupported expression-path " + path);
    }
    return rootName;
  }

  ColumnType propertyType(ExpressionPath path) {
    ExpressionPath.NameSegment root = path.getRoot();
    String rootName = root.getName();
    String colName = rootName;
    if (root.getChild().isPresent()) {
      ExpressionPath.PathSegment child = root.getChild().get();
      int position = child.asPosition().getPosition();

      if (!child.getChild().isPresent()) {
        // e.g. "tree[0]"
        if ("tree".equals(rootName) && child.isPosition()) {
          return ColumnType.ID;
        } else {
          throw new IllegalArgumentException("Unsupported expression-path " + path);
        }
      } else {
        if ("commits".equals(rootName) && child.isPosition()) {
          String attrName = child.getChild().get().asName().getName();
          colName = "c_" + attrName + "_" + position;
        } else {
          throw new IllegalArgumentException("Unsupported expression-path " + path);
        }
      }

    }
    ColumnType type = columnTypeMap.get(colName);
    if (type == null) {
      throw new IllegalArgumentException("No column-type for " + colName);
    }
    return type;
  }

  interface QueryIterator<C extends BaseValue<C>> {
    boolean hasNext() throws SQLException;

    void produce(C producer) throws SQLException;
  }

  QueryIterator<C> queryAll(Resources resources) throws SQLException {
    PreparedStatement pstmt = resources.add(resources.connection.prepareStatement(queryAll));
    ResultSet resultSet = resources.add(pstmt.executeQuery());

    return new QueryIterator<C>() {
      @Override
      public boolean hasNext() throws SQLException {
        return resultSet.next();
      }

      public void produce(C producer) throws SQLException {
        produceToConsumer.produceToConsumer(resultSet, producer);
      }
    };
  }

  int query(Resources resources, Collection<Id> ids, Function<Id, C> consumerSupplier,
      Consumer<Id> finished) throws SQLException {

    String idBinds = IntStream.range(0, ids.size()).mapToObj(x -> "?").collect(Collectors.joining(", "));
    String sql = queryIds.replace(IDS_MARKER, idBinds);
    PreparedStatement pstmt = resources.add(resources.connection.prepareStatement(sql));

    int i = 1;
    for (Iterator<Id> idIter = ids.iterator(); idIter.hasNext(); i++) {
      dialect.setId(pstmt, i, idIter.next());
    }

    LOGGER.trace("query {}", pstmt);

    ResultSet resultSet = resources.add(pstmt.executeQuery());

    int valueCount = 0;
    while (resultSet.next()) {
      Id id = dialect.getId(resultSet, ID);
      produceToConsumer.produceToConsumer(resultSet, consumerSupplier.apply(id));
      finished.accept(id);
      valueCount++;
    }

    return valueCount;
  }

  JdbcBaseValue<C> newValue(Resources resources, SQLChange change) {
    return jdbcValueSupplier.createNewValue(resources, change, this);
  }

  SQLInsert newInsert() {
    return new SQLInsert();
  }

  SQLUpdate newUpdate() {
    return new SQLUpdate();
  }

  SQLDelete newDelete() {
    return new SQLDelete();
  }

  /**
   * Wraps an SQL DML statement via it's sub-classes.
   */
  abstract static class SQLChange implements AutoCloseable {

    /**
     * "Dynamic" column updates - either for {@code INSERT} or {@code UPDATE}.
     * <p>The map key is the column name.</p>
     * <p>The map value is a {@link ValueApplicator} use to populate the bind variable of
     * the generated {@link PreparedStatement}.</p>
     */
    final Map<String, ValueApplicator> updates = new LinkedHashMap<>();
    /**
     * "Static" column updates - either for {@code INSERT} or {@code UPDATE}.
     * <p>The map key is the column name.</p>
     * <p>The map value is the full expression including the column name, for example
     * {@code my_column = other_column} or {@code my_column = NULL}.</p>
     */
    final Map<String, String> setExpressions = new LinkedHashMap<>();
    /**
     * "Dynamic" column conditions - either for {@code DELETE} or {@code UPDATE}.
     * <p>The map key is the column name.</p>
     * <p>The map value is a {@link ValueApplicator} use to populate the bind variable of
     * the generated {@link PreparedStatement}.</p>
     */
    final Map<String, ValueApplicator> conditions = new LinkedHashMap<>();
    /**
     * "Static" column conditions - either for {@code DELETE} or {@code UPDATE}.
     * <p>The map key is the column name.</p>
     * <p>The map value is the full expression including the column name, for example
     * {@code my_column = other_column} or {@code my_column IS NOT NULL}.</p>
     */
    final Map<String, String> conditionExpressions = new LinkedHashMap<>();
    /**
     * The generated prepared statement.
     */
    PreparedStatement statement;

    void setColumn(String column, ValueApplicator applicator) {
      updates.put(column, applicator);
    }

    void setExpression(String col, String expression) {
      setExpressions.put(col, expression);
    }

    void addCondition(String column, ValueApplicator applicator) {
      conditions.put(column, applicator);
    }

    void addConditionExpression(String column, String expression) {
      conditionExpressions.put(column, expression);
    }

    int executeUpdate() throws SQLException {
      LOGGER.trace("executeUpdate {}", statement);
      return statement.executeUpdate();
    }

    void prepareStatement(Resources resources) throws SQLException {
      String sql = generateStatement();
      statement = resources.add(resources.connection.prepareStatement(sql));
      int i = 1;
      for (ValueApplicator value : updates.values()) {
        i += value.set(statement, i);
      }
      for (ValueApplicator value : conditions.values()) {
        i += value.set(statement, i);
      }
    }

    abstract String generateStatement();

    @Override
    public void close() throws Exception {
      if (statement != null) {
        statement.close();
      }
    }
  }

  /**
   * {@link SQLChange} implementation for SQL {@code INSERT}.
   */
  class SQLInsert extends SQLChange {

    @Override
    String generateStatement() {
      if (!conditions.isEmpty() || !conditionExpressions.isEmpty()) {
        throw new IllegalStateException("collection of column-updates must be empty for a INSERT");
      }
      if (!setExpressions.isEmpty()) {
        throw new IllegalStateException("collection of column-expressions must be empty for a INSERT");
      }
      return "INSERT INTO "
          + tableName
          + " (" + String.join(", ", updates.keySet()) + ") "
          + " VALUES (" + IntStream.range(0, updates.size()).mapToObj(x -> "?").collect(Collectors.joining(", ")) + ")";
    }
  }

  /**
   * {@link SQLChange} implementation for SQL {@code UPDATE}.
   */
  class SQLUpdate extends SQLChange {
    @Override
    String generateStatement() {
      return "UPDATE "
          + tableName
          + " SET "
          + Stream.concat(
              updates.keySet().stream().map(valueApplicator -> valueApplicator + " = ?"),
              setExpressions.values().stream())
            .collect(Collectors.joining(", "))
          + " WHERE "
          + Stream.concat(
              conditions.keySet().stream().map(valueApplicator -> valueApplicator + " = ?"),
              conditionExpressions.values().stream())
            .collect(Collectors.joining(" AND "));
    }

    @Override
    void setColumn(String column, ValueApplicator applicator) {
      setExpressions.remove(column);
      super.setColumn(column, applicator);
    }
  }

  /**
   * {@link SQLChange} implementation for SQL {@code DELETE}.
   */
  class SQLDelete extends SQLChange {
    @Override
    String generateStatement() {
      if (!updates.isEmpty()) {
        throw new IllegalStateException("collection of column-updates must be empty for a DELETE");
      }
      if (!setExpressions.isEmpty()) {
        throw new IllegalStateException("collection of column-expressions must be empty for a DELETE");
      }
      return "DELETE FROM "
          + tableName
          + " WHERE "
          + Stream.concat(
              conditions.keySet().stream().map(valueApplicator -> valueApplicator + " = ?"),
              conditionExpressions.values().stream())
            .collect(Collectors.joining(" AND "));
    }
  }
}
