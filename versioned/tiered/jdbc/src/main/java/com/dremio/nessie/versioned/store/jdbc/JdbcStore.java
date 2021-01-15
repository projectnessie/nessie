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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators.AbstractSpliterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.nessie.tiered.builder.BaseValue;
import com.dremio.nessie.versioned.impl.EntityStoreHelper;
import com.dremio.nessie.versioned.impl.condition.ConditionExpression;
import com.dremio.nessie.versioned.impl.condition.ExpressionFunction;
import com.dremio.nessie.versioned.impl.condition.ExpressionFunction.FunctionName;
import com.dremio.nessie.versioned.impl.condition.ExpressionPath;
import com.dremio.nessie.versioned.impl.condition.RemoveClause;
import com.dremio.nessie.versioned.impl.condition.SetClause;
import com.dremio.nessie.versioned.impl.condition.UpdateClause;
import com.dremio.nessie.versioned.impl.condition.UpdateExpression;
import com.dremio.nessie.versioned.impl.condition.Value;
import com.dremio.nessie.versioned.store.ConditionFailedException;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.LoadOp;
import com.dremio.nessie.versioned.store.LoadStep;
import com.dremio.nessie.versioned.store.NotFoundException;
import com.dremio.nessie.versioned.store.SaveOp;
import com.dremio.nessie.versioned.store.Store;
import com.dremio.nessie.versioned.store.StoreOperationException;
import com.dremio.nessie.versioned.store.ValueType;
import com.dremio.nessie.versioned.store.jdbc.JdbcBaseValue.ExecuteUpdateStrategy;
import com.dremio.nessie.versioned.store.jdbc.JdbcEntity.QueryIterator;
import com.dremio.nessie.versioned.store.jdbc.JdbcEntity.SQLChange;
import com.dremio.nessie.versioned.store.jdbc.JdbcEntity.SQLUpdate;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;

public class JdbcStore implements Store {
  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcStore.class);

  private final JdbcStoreConfig config;
  private final DataSource dataSource;

  private final Map<ValueType<?>, JdbcEntity<?>> entityDefinitions;

  /**
   * create a DynamoStore.
   */
  public JdbcStore(JdbcStoreConfig config, DataSource dataSource) {
    this.config = config;
    this.dataSource = dataSource;

    this.entityDefinitions = ImmutableMap.<ValueType<?>, JdbcEntity<?>>builder()
        .put(ValueType.KEY_FRAGMENT, JdbcFragment.createEntity(config))
        .put(ValueType.L1, JdbcL1.createEntity(config))
        .put(ValueType.L2, JdbcL2.createEntity(config))
        .put(ValueType.L3, JdbcL3.createEntity(config))
        .put(ValueType.VALUE, JdbcWrappedValue.createEntity(ValueType.VALUE, config))
        .put(ValueType.COMMIT_METADATA, JdbcWrappedValue.createEntity(ValueType.COMMIT_METADATA, config))
        .put(ValueType.REF, JdbcRef.createEntity(config))
        .build();

    if (!entityDefinitions.keySet().equals(new HashSet<>(ValueType.values()))) {
      throw new IllegalStateException("JdbcStore implementation does not define tables for all entity types. "
          + entityDefinitions.keySet() + " vs " + ValueType.values());
    }
  }

  @Override
  public void start() {
    Dialect dialect = config.getDialect();
    String catalog = config.getCatalog();
    String schema = config.getSchema();

    LOGGER.info("Starting JDBC store using dialect {} against data source '{}'",
        dialect.name(), dataSource);

    if (config.logCreateDDL()) {
      LOGGER.info("Config option 'logCreateDDL' is set to 'true', DDL statements to create the "
          + "Nessie database objects for dialect {} are:\n{}",
          dialect.name(),
          Stream.concat(
              dialect.additionalDDL().stream(),
              entityDefinitions.entrySet().stream().map(e -> createTableDDL(e.getKey().getTableName(config.getTablePrefix()), e.getValue()))
          ).collect(Collectors.joining("\n\n")));
    }
    if (config.setupTables()) {
      LOGGER.info("Setting up tables using catalog {} and schema {}",
          catalog != null ? catalog : "<none>",
          schema != null ? schema : "<none>");
      try (Connection conn = dataSource.getConnection()) {

        try (Statement st = conn.createStatement()) {
          Stream<String> createTables = entityDefinitions.entrySet().stream()
              .filter(e -> !tableExists(conn, dialect, catalog, schema, e.getKey().getTableName(config.getTablePrefix())))
              .map(e -> createTableDDL(e.getKey().getTableName(config.getTablePrefix()), e.getValue()));

          if (dialect.batchDDL()) {
            String ddl = createTables.map(s -> s + ";").collect(Collectors.joining("\n\n"));
            if (!ddl.isEmpty()) {
              ddl = "BEGIN;\n"
                  + dialect.additionalDDL().stream().map(s -> s + ";").collect(Collectors.joining("\n\n"))
                  + ddl
                  + "END TRANSACTION;\n";
              LOGGER.debug("create-table: {}", ddl);
              st.execute(ddl);
            }
          } else {
            List<String> ddls = createTables.collect(Collectors.toList());
            if (!ddls.isEmpty()) {
              for (String ddl : dialect.additionalDDL()) {
                LOGGER.debug("create-DDL: {}", ddl);
                st.execute(ddl);
              }
            }
            for (String ddl : ddls) {
              LOGGER.debug("create-table: {}", ddl);
              st.execute(ddl);
            }
          }
        }
      } catch (SQLException e) {
        LOGGER.error("Failed to setup database tables", e);
        throw new StoreOperationException("SQL Exception caught", e);
      }
    }
    if (config.initializeDatabase()) {
      // make sure we have an empty l1 (ignore result, doesn't matter)
      EntityStoreHelper.storeMinimumEntities(this::putIfAbsent);
    }
  }

  private static boolean tableExists(Connection conn, Dialect dialect, String catalog, String schema, String table) {
    if (dialect.metadataUpperCase()) {
      catalog = catalog != null ? catalog.toUpperCase(Locale.ROOT) : null;
      schema = schema != null ? schema.toUpperCase(Locale.ROOT) : null;
      table = table != null ? table.toUpperCase(Locale.ROOT) : null;
    }

    try (ResultSet tables = conn.getMetaData().getTables(catalog, schema, table, null)) {
      return tables.next();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private String createTableDDL(String rawTableName, JdbcEntity<?> entity) {
    return String.format("CREATE TABLE %s (\n    %s"
            + ",\n    %s)",
        entity.tableName,
        entity.columnTypeMap.entrySet().stream()
            .map(e -> String.format("%s %s", e.getKey(), entity.dialect.columnType(e.getValue())))
            .collect(Collectors.joining(",\n    ")),
        entity.dialect.primaryKey(rawTableName, JdbcEntity.ID)
    );
  }

  @Override
  public void close() {
    // nothing to do here actually, the JDBC DataSource is injected
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  public void load(LoadStep loadstep) {
    Map<ValueType<?>, TypeLoadOps<?>> loadOpsByType = new HashMap<>();

    try (Resources resources = new Resources(dataSource)) {
      while (true) {
        loadstep.getOps().forEach(op -> {
          TypeLoadOps<?> perType = loadOpsByType
              .computeIfAbsent(op.getValueType(), TypeLoadOps::new);
          perType.add((LoadOp) op);
        });

        for (TypeLoadOps<?> perType : loadOpsByType.values()) {
          JdbcEntity def = entityDefinitions.get(perType.type);
          def.query(resources, perType.ops.keySet(),
              id -> perType.ops.get(id).getReceiver(),
              id -> perType.ops.remove(id).done());
        }

        String missed = loadOpsByType.values().stream()
            .flatMap(perType -> perType.ops.values().stream())
            .map(op -> String.format("%s:%s", op.getValueType().getValueName(), op.getId()))
            .collect(Collectors.joining(", "));
        if (!missed.isEmpty()) {
          throw new NotFoundException(missed);
        }

        Optional<LoadStep> next = loadstep.getNext();

        if (!next.isPresent()) {
          break;
        }
        loadstep = next.get();
        loadOpsByType.clear();
      }
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new StoreOperationException("SQL Exception caught", e);
    }
  }

  static class TypeLoadOps<C extends BaseValue<C>> {
    final ValueType<C> type;
    final Map<Id, LoadOp<C>> ops = new HashMap<>();

    TypeLoadOps(ValueType<C> type) {
      this.type = type;
    }

    void add(LoadOp<C> op) {
      Preconditions.checkNotNull(op.getId());
      ops.put(op.getId(), op);
    }
  }

  @Override
  public <C extends BaseValue<C>> void loadSingle(ValueType<C> type, Id id, C consumer) {
    try (Resources resources = new Resources(dataSource)) {
      doLoadSingle(type, id, consumer, resources);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new StoreOperationException("Unhandled Exception caught for " + type + ":" + id, e);
    }
  }

  private <C extends BaseValue<C>> void doLoadSingle(ValueType<C> type, Id id, C consumer,
      Resources resources) throws Exception {
    @SuppressWarnings("unchecked") JdbcEntity<C> entity = (JdbcEntity<C>) entityDefinitions.get(type);

    if (entity.query(resources, Collections.singleton(id), x -> consumer, x -> {}) == 0) {
      throw new NotFoundException(String.format("%s:%s", type, id));
    }
  }

  @Override
  public <C extends BaseValue<C>> Stream<Acceptor<C>> getValues(ValueType<C> type) {
    try {
      Resources resources = new Resources(dataSource);
      AtomicBoolean consumeState = new AtomicBoolean();

      @SuppressWarnings("unchecked") JdbcEntity<C> entity = (JdbcEntity<C>) entityDefinitions.get(type);

      QueryIterator<C> queryIterator = entity.queryAll(resources);

      Spliterator<Acceptor<C>> spliterator = new AbstractSpliterator<Acceptor<C>>(
          Long.MAX_VALUE, Spliterator.ORDERED | Spliterator.IMMUTABLE | Spliterator.NONNULL) {
        @Override
        public boolean tryAdvance(Consumer<? super Acceptor<C>> action) {
          if (resources.closed) {
            return false;
          }

          try {
            if (consumeState.get()) {
              throw new IllegalStateException("A previous row fetched from the JDBC ResultSet has not "
                  + "been consumed via the provided Acceptor");
            }

            if (!queryIterator.hasNext()) {
              try {
                return false;
              } finally {
                resources.close();
              }
            }

            if (!consumeState.compareAndSet(false, true)) {
              throw new IllegalStateException("A row was fetched from the JDBC ResultSet, but the "
                  + "previous row was not consumed in the provided Acceptor instance");
            }

            Store.Acceptor<C> acceptor = producer -> {
              if (!consumeState.compareAndSet(true, false)) {
                throw new IllegalStateException("A provided Acceptor.applyValue() was called without a row being fetched.");
              }

              try {
                queryIterator.produce(producer);
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
            };

            action.accept(acceptor);

            return true;
          } catch (RuntimeException e) {
            try {
              resources.close();
            } catch (Exception ex) {
              e.addSuppressed(ex);
            }
            throw e;
          } catch (Exception e) {
            try {
              resources.close();
            } catch (Exception ex) {
              e.addSuppressed(ex);
            }
            throw new StoreOperationException("Failed to iterate", e);
          }
        }
      };

      return StreamSupport.stream(spliterator, false)
          .onClose(() -> {
            try {
              resources.close();
            } catch (RuntimeException e) {
              throw e;
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          });
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new StoreOperationException("Unhandled Exception caught", e);
    }
  }

  @Override
  public <C extends BaseValue<C>> boolean putIfAbsent(SaveOp<C> saveOp) {
    ConditionExpression condition = ConditionExpression.of(ExpressionFunction.attributeNotExists(
        ExpressionPath.builder(KEY_NAME).build()));
    try {
      put(saveOp, Optional.of(condition));
      return true;
    } catch (ConditionFailedException ex) {
      return false;
    }
  }

  @Override
  public <C extends BaseValue<C>> void put(SaveOp<C> saveOp, Optional<ConditionExpression> condition) {
    try (Resources resources = new Resources(dataSource)) {
      resources.connection.setAutoCommit(false);
      saveSingle(resources, saveOp, condition);
      resources.connection.commit();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      if (config.getDialect().isIntegrityConstraintViolation(e)) {
        throw new ConditionFailedException(e.getMessage());
      }
      throw new StoreOperationException("SQL Exception caught for " + saveOp.getType() + ":" + saveOp.getId()
          + ", condition " + condition, e);
    }
  }

  @Override
  public void save(List<SaveOp<?>> ops) {
    try (Resources resources = new Resources(dataSource)) {
      resources.connection.setAutoCommit(false);
      for (SaveOp<?> op : ops) {
        saveSingle(resources, op, Optional.empty());
      }
      resources.connection.commit();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      if (config.getDialect().isIntegrityConstraintViolation(e)) {
        throw new ConditionFailedException(e.getMessage());
      }
      throw new StoreOperationException("SQL Exception caught", e);
    }
  }

  private <C extends BaseValue<C>> void saveSingle(Resources resources, SaveOp<C> saveOp,
      Optional<ConditionExpression> condition) throws SQLException {
    @SuppressWarnings("unchecked") JdbcEntity<C> entity = (JdbcEntity<C>) entityDefinitions.get(saveOp.getType());
    SQLChange change = entity.newInsert();
    JdbcBaseValue<C> value = entity.newValue(resources, change);

    UpdateContext updateContext = new UpdateContext(change, saveOp.getId());
    Conditions conditions = sqlConditions(value, updateContext, condition);
    if (conditions.ifNotExists) {
      if (conditions.applicators.size() != 1) {
        // 'WHERE ID=?' is added implicitly
        throw new IllegalArgumentException("attributeNotExists(ID) cannot be combined with other conditions");
      }
      @SuppressWarnings("unchecked") C cons = (C) value;
      saveOp.serialize(cons);
      value.executeUpdates(ExecuteUpdateStrategy.INSERT, new Conditions(), saveOp.getId());
    } else if (!condition.isPresent()) {
      @SuppressWarnings("unchecked") C cons = (C) value;
      saveOp.serialize(cons);
      Conditions upsertConditions = new Conditions().addIdCondition(saveOp.getId(), entity.dialect);
      value.executeUpdates(ExecuteUpdateStrategy.UPSERT, upsertConditions, saveOp.getId());
    } else {
      @SuppressWarnings("unchecked") C cons = (C) value;
      saveOp.serialize(cons);
      value.executeUpdates(ExecuteUpdateStrategy.UPSERT_MUST_DELETE, conditions, saveOp.getId());
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public <C extends BaseValue<C>> boolean update(ValueType<C> type, Id id, UpdateExpression update,
      Optional<ConditionExpression> condition, Optional<BaseValue<C>> consumer)
      throws NotFoundException {
    JdbcEntity<C> entity = (JdbcEntity<C>) entityDefinitions.get(type);

    try (Resources resources = new Resources(dataSource)) {
      resources.connection.setAutoCommit(false);

      SQLUpdate change = entity.newUpdate();
      JdbcBaseValue<C> value = entity.newValue(resources, change);

      UpdateContext updateContext = new UpdateContext(change, id);
      Conditions conditions = sqlConditions(value, updateContext, condition);
      UpdateContext updates = sqlUpdates(value, update, updateContext);

      if (!updates.change.updates.isEmpty()) {
        entity.buildUpdate(resources, updates, conditions);
        if (change.executeUpdate() != 1) {
          return false;
        }
      }

      if (consumer.isPresent()) {
        doLoadSingle(type, id, (C) consumer.get(), resources);
      }

      resources.connection.commit();

      return true;
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      if (config.getDialect().isIntegrityConstraintViolation(e)) {
        throw new ConditionFailedException(e.getMessage());
      }
      throw new StoreOperationException("SQL Exception caught for " + update + ", condition " + condition, e);
    }
  }

  @Override
  public <C extends BaseValue<C>> boolean delete(ValueType<C> type, Id id, Optional<ConditionExpression> condition) {
    @SuppressWarnings("unchecked") JdbcEntity<C> entity = (JdbcEntity<C>) entityDefinitions.get(type);

    try (Resources resources = new Resources(dataSource)) {
      resources.connection.setAutoCommit(false);

      SQLChange change = entity.newDelete();
      change.addCondition(JdbcEntity.ID, entity.dialect.idApplicator(id));
      JdbcBaseValue<C> value = entity.newValue(resources, change);

      UpdateContext updateContext = new UpdateContext(change, id);
      Conditions conditions = sqlConditions(value, updateContext, condition);

      boolean r = value.executeUpdates(ExecuteUpdateStrategy.DELETE, conditions, id) == 1;
      resources.connection.commit();
      return r;
    } catch (NotFoundException | ConditionFailedException e) {
      return false;
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      if (config.getDialect().isIntegrityConstraintViolation(e)) {
        throw new ConditionFailedException(e.getMessage());
      }
      throw new StoreOperationException("SQL Exception caught for " + type + ":" + id + ", condition " + condition, e);
    }
  }

  @VisibleForTesting
  void truncateTables() {
    try (Connection conn = dataSource.getConnection(); Statement st = conn.createStatement()) {
      if (config.getDialect().batchDDL()) {
        String ddl = "BEGIN;\n"
            + entityDefinitions.values().stream().map(def -> "TRUNCATE TABLE " + def.tableName + ";").collect(Collectors.joining("\n"))
            + "\nEND TRANSACTION;\n";
        LOGGER.debug("Truncating {}", ddl);
        st.execute(ddl);
      } else {
        entityDefinitions.values().forEach(def -> {
          try {
            LOGGER.debug("Truncating {}", def.tableName);
            st.execute(String.format("TRUNCATE TABLE %s", def.tableName));
          } catch (SQLException e) {
            throw new StoreOperationException("SQL Exception caught", e);
          }
        });
      }
    } catch (SQLException e) {
      throw new StoreOperationException("SQL Exception caught", e);
    }
  }

  private <C extends BaseValue<C>> UpdateContext sqlUpdates(JdbcBaseValue<C> value,
      UpdateExpression update, UpdateContext updateContext) {
    for (UpdateClause clause : update.getClauses()) {
      UpdateClause.Type clauseType = clause.getType();
      ExpressionPath path;
      switch (clauseType) {
        /*
        case ADD:
          AddClause addClause = (AddClause) clause;
          path = addClause.getPath();
          value = addClause.getValue();
          break;
        */
        case REMOVE:
          RemoveClause removeClause = (RemoveClause) clause;
          path = removeClause.getPath();
          value.removeValue(updateContext, path);
          break;
        case SET:
          SetClause setClause = (SetClause) clause;
          path = setClause.getPath();
          Value setValue = setClause.getValue();
          switch (setValue.getType()) {
            case PATH:
              throw new UnsupportedOperationException("Unsupported clause " + clause);
              //break;
            case FUNCTION:
              ExpressionFunction function = (ExpressionFunction) setValue;
              FunctionName functionName = function.getName();
              List<Value> arguments = function.getArguments();
              switch (functionName) {
                /*
                case EQUALS:
                  colType = entityDefinition.columnType(dialect, path);
                  updates.put(
                      dialect.equalsClause(path, positionOffset),
                      dialect.valueForPreparedStatement(colType, arguments.get(1).getValue()));
                  break;
                */
                case LIST_APPEND:
                  value.updateListAppend(updateContext, path, arguments.get(1).getValue());
                  break;
                default:
                  throw new IllegalArgumentException("Function " + functionName + " not supported for update-clause");
              }
              break;
            case VALUE:
              value.updateValue(updateContext, path, setValue.getValue());
              break;
            default:
              throw new IllegalArgumentException(setValue.getType().name());
          }
          break;
        // case DELETE: // TODO No DeleteClause ???
        default:
          throw new UnsupportedOperationException("Unsupported clause " + clause);
      }
    }
    return updateContext;
  }

  private <C extends BaseValue<C>> Conditions sqlConditions(JdbcBaseValue<C> value,
      UpdateContext updateContext, Optional<ConditionExpression> condition) {
    Conditions conditions = new Conditions();

    if (condition.isPresent()) {
      conditions.addIdCondition(updateContext.id, value.entity.dialect);

      for (ExpressionFunction function : condition.get().getFunctions()) {
        FunctionName functionName = function.getName();
        List<Value> arguments = function.getArguments();
        switch (functionName) {
          case EQUALS:
            switch (arguments.get(0).getType()) {
              case FUNCTION:
                switch (arguments.get(0).getFunction().getName()) {
                  case SIZE:
                    ExpressionPath path = arguments.get(0).getFunction().getArguments().get(0).getPath();
                    value.conditionSize(updateContext, condition, path,
                        Ints.checkedCast(arguments.get(1).getValue().getNumber()));
                    break;
                  case ATTRIBUTE_NOT_EXISTS:
                  case EQUALS:
                  case IF_NOT_EXISTS:
                  case LIST_APPEND:
                  default:
                    throw new IllegalArgumentException("Function " + function + " not supported");
                }
                break;
              case PATH:
              case VALUE:
                ExpressionPath path = arguments.get(0).getPath();
                value.conditionValue(updateContext, conditions, path, arguments.get(1).getValue());
                break;
              default:
                throw new IllegalArgumentException("Function " + function + " not supported");
            }
            break;
          case ATTRIBUTE_NOT_EXISTS:
            conditions.ifNotExists = true;
            if (Store.KEY_NAME.equals(arguments.get(0).getPath().getRoot().getName())) {
              conditions.ifNotExists = true;
            } else {
              throw new IllegalArgumentException("Function " + function + " not supported");
            }
            break;
          /*
          case IF_NOT_EXISTS:
            break;
          */
          /*
          case SIZE:
            break;
          */
          /*
          case LIST_APPEND:
            break;
          */
          default:
            throw new IllegalArgumentException("Function " + functionName + " ("
              + arguments + ") not supported for update-condition");
        }
      }
    }
    return conditions;
  }
}
