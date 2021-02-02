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
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
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
import com.google.common.base.Preconditions;

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
    void produceToConsumer(ResultSet resultSet, C consumer);
  }

  private final DatabaseAdapter databaseAdapter;
  private final String tableName;
  private final Map<String, ColumnType> columnTypeMap;
  private final String queryAll;
  private final String queryIds;
  private final JdbcValueSupplier<C> jdbcValueSupplier;
  private final ProduceToConsumer<C> produceToConsumer;

  private static final String IDS_MARKER = "__IDS__";

  JdbcEntity(DatabaseAdapter databaseAdapter, ValueType<C> type, JdbcStoreConfig config, Map<String, ColumnType> columnTypeMap,
      JdbcValueSupplier<C> jdbcValueSupplier, ProduceToConsumer<C> produceToConsumer) {
    this.databaseAdapter = databaseAdapter;
    String tableName = type.getTableName(config.getTablePrefix());
    if (config.getSchema() != null) {
      tableName = String.format("%s.%s", config.getSchema(), tableName);
    }
    this.tableName = tableName;
    this.columnTypeMap = columnTypeMap;
    this.jdbcValueSupplier = jdbcValueSupplier;
    this.produceToConsumer = produceToConsumer;

    String allColumns = String.join(", ", columnTypeMap.keySet());

    this.queryAll = String.format("SELECT %s FROM %s", allColumns, tableName);

    this.queryIds = String.format("SELECT %s FROM %s WHERE %s IN (%s)",
        allColumns, tableName, ID, IDS_MARKER);
  }

  String createTableDDL(String rawTableName) {
    return String.format("CREATE TABLE %s (\n    %s"
            + ",\n    %s)",
        tableName,
        columnTypeMap.entrySet().stream()
            .map(e -> String.format("%s %s", e.getKey(), databaseAdapter.columnType(e.getValue())))
            .collect(Collectors.joining(",\n    ")),
        databaseAdapter.primaryKey(rawTableName, JdbcEntity.ID)
    );
  }

  DatabaseAdapter getDatabaseAdapter() {
    return databaseAdapter;
  }

  String getTableName() {
    return tableName;
  }

  void buildUpdate(Resources resources, UpdateContext updates, Conditions conditions) {
    conditions.forEach(updates.getChange()::addCondition);

    updates.getChange().prepareStatement(resources);
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
          return String.format("%s_%d", rootName, position);
        }
      } else {
        PathSegment childAttr = child.getChild().get();

        if (childAttr.isName()) {
          String childAttrName = childAttr.asName().getName();

          return commitColumnName(childAttrName, position);
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
          colName = commitColumnName(attrName, position);
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

  private static String commitColumnName(String attrName, int position) {
    return String.format("c_%s_%d", attrName, position);
  }

  interface QueryIterator<C extends BaseValue<C>> {
    boolean hasNext();

    void produce(C producer);
  }

  QueryIterator<C> queryAll(Resources resources) {
    ResultSet resultSet = resources.query(queryAll, preparedStatement -> {});

    return new QueryIterator<C>() {
      @Override
      public boolean hasNext() {
        return SQLError.call(resultSet::next);
      }

      public void produce(C producer) {
        produceToConsumer.produceToConsumer(resultSet, producer);
      }
    };
  }

  private static String bindVariables(int numBinds) {
    return IntStream.range(0, numBinds).mapToObj(ignore -> "?").collect(Collectors.joining(", "));
  }

  int query(Resources resources, Collection<Id> ids, Function<Id, C> consumerSupplier, Consumer<Id> finished) {
    String sql = queryIds.replace(IDS_MARKER, bindVariables(ids.size()));
    ResultSet resultSet = resources.query(sql, pstmt -> {
      int i = 1;
      for (Iterator<Id> idIter = ids.iterator(); idIter.hasNext(); i++) {
        databaseAdapter.setId(pstmt, i, idIter.next());
      }
    });

    int valueCount = 0;
    while (SQLError.call(resultSet::next)) {
      Id id = databaseAdapter.getId(resultSet, ID);
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
    protected final Map<String, ValueApplicator> updates = new LinkedHashMap<>();
    /**
     * "Static" column updates - either for {@code INSERT} or {@code UPDATE}.
     * <p>The map key is the column name.</p>
     * <p>The map value is the full expression including the column name, for example
     * {@code my_column = other_column} or {@code my_column = NULL}.</p>
     */
    protected final Map<String, String> setExpressions = new LinkedHashMap<>();
    /**
     * "Dynamic" column conditions - either for {@code DELETE} or {@code UPDATE}.
     * <p>The map key is the column name.</p>
     * <p>The map value is a {@link ValueApplicator} use to populate the bind variable of
     * the generated {@link PreparedStatement}.</p>
     */
    protected final Map<String, ValueApplicator> conditions = new LinkedHashMap<>();
    /**
     * "Static" column conditions - either for {@code DELETE} or {@code UPDATE}.
     * <p>The map key is the column name.</p>
     * <p>The map value is the full expression including the column name, for example
     * {@code my_column = other_column} or {@code my_column IS NOT NULL}.</p>
     */
    protected final Map<String, String> conditionExpressions = new LinkedHashMap<>();
    /**
     * The generated prepared statement.
     */
    private PreparedStatement statement;

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

    int executeUpdate() {
      LOGGER.trace("executeUpdate {}", statement);
      return SQLError.call(statement::executeUpdate);
    }

    void prepareStatement(Resources resources) {
      String sql = generateStatement();
      statement = resources.prepareStatement(sql);
      int i = 1;
      for (ValueApplicator value : updates.values()) {
        i += value.set(statement, i);
      }
      for (ValueApplicator value : conditions.values()) {
        i += value.set(statement, i);
      }
    }

    abstract String generateStatement();

    String columnsExpression(Map<String, ValueApplicator> prepared, Map<String, String> unprepared, String delimiter) {
      return Stream.concat(
          prepared.keySet().stream().map(columnName -> columnName + " = ?"),
          unprepared.values().stream())
          .collect(Collectors.joining(delimiter));
    }

    String whereConditionSQL() {
      return columnsExpression(conditions, conditionExpressions, " AND ");
    }

    @Override
    public void close() throws Exception {
      if (statement != null) {
        statement.close();
      }
    }

    @Override
    public String toString() {
      return String.format("%s{statement=%s}", getClass().getSimpleName(), statement);
    }
  }

  /**
   * {@link SQLChange} implementation for SQL {@code INSERT}.
   */
  class SQLInsert extends SQLChange {

    @Override
    String generateStatement() {
      Preconditions.checkArgument(conditions.isEmpty() && conditionExpressions.isEmpty(),
          "collection of column-updates must be empty for a INSERT");
      Preconditions.checkArgument(setExpressions.isEmpty(), "collection of column-expressions must be empty for a INSERT");
      return String.format("INSERT INTO %s (%s) VALUES (%s)",
          tableName,
          String.join(", ", updates.keySet()),
          bindVariables(updates.size()));
    }
  }

  /**
   * {@link SQLChange} implementation for SQL {@code UPDATE}.
   */
  class SQLUpdate extends SQLChange {
    @Override
    String generateStatement() {
      return String.format("UPDATE %s SET %s WHERE %s",
          tableName,
          columnsExpression(updates, setExpressions, ", "),
          whereConditionSQL());
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
      Preconditions.checkArgument(updates.isEmpty(), "collection of column-updates must be empty for a DELETE");
      Preconditions.checkArgument(setExpressions.isEmpty(), "collection of column-expressions must be empty for a DELETE");
      return String.format("DELETE FROM %s WHERE %s",
          tableName,
          whereConditionSQL());
    }
  }
}
