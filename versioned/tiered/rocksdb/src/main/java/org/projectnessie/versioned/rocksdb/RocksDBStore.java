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
package org.projectnessie.versioned.rocksdb;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.projectnessie.versioned.impl.condition.ConditionExpression;
import org.projectnessie.versioned.impl.condition.UpdateExpression;
import org.projectnessie.versioned.store.ConditionFailedException;
import org.projectnessie.versioned.store.Id;
import org.projectnessie.versioned.store.LoadOp;
import org.projectnessie.versioned.store.LoadStep;
import org.projectnessie.versioned.store.NotFoundException;
import org.projectnessie.versioned.store.SaveOp;
import org.projectnessie.versioned.store.Store;
import org.projectnessie.versioned.store.ValueType;
import org.projectnessie.versioned.tiered.BaseValue;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Transaction;
import org.rocksdb.TransactionDB;
import org.rocksdb.TransactionDBOptions;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * A RocksDB specific backing store to store and retrieve Nessie metadata.
 */
public class RocksDBStore implements Store {

  private static final Logger LOGGER = LoggerFactory.getLogger(RocksDBStore.class);
  private static final RocksDBValueVisitor VALUE_VISITOR = new RocksDBValueVisitor();
  private static final String DEFAULT_COLUMN_FAMILY = new String(RocksDB.DEFAULT_COLUMN_FAMILY, UTF_8);
  private static final WriteOptions WRITE_OPTIONS = new WriteOptions();

  static {
    RocksDB.loadLibrary();
  }

  protected TransactionDB rocksDB;
  protected Map<ValueType<?>, ColumnFamilyHandle> valueTypeToColumnFamily;
  private final RocksDBStoreConfig config;

  /**
   * Creates a store ready for connection to RocksDB.
   * @param config the configuration for the store.
   */
  public RocksDBStore(RocksDBStoreConfig config) {
    this.config = config;
  }

  @Override
  public void start() {
    final String dbPath = verifyPath();
    final List<ColumnFamilyDescriptor> columnFamilies = getColumnFamilies(dbPath);

    try (final DBOptions dbOptions = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true)) {
      // TODO: Consider setting WAL limits.
      final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
      rocksDB = TransactionDB.open(dbOptions, new TransactionDBOptions(), dbPath, columnFamilies, columnFamilyHandles);

      final ImmutableMap.Builder<ValueType<?>, ColumnFamilyHandle> builder = new ImmutableMap.Builder<>();
      for (ColumnFamilyHandle handle : columnFamilyHandles) {
        final String valueTypeName = new String(handle.getName(), UTF_8);
        if (!valueTypeName.equals(DEFAULT_COLUMN_FAMILY)) {
          builder.put(ValueType.byValueName(valueTypeName), handle);
        }
      }
      valueTypeToColumnFamily = builder.build();
    } catch (RocksDBException e) {
      throw new RuntimeException("RocksDB failed to start", e);
    }
  }

  @Override
  public void close() {
    if (null != rocksDB) {
      rocksDB.close();
      rocksDB = null;
    }
  }

  @Override
  public void load(LoadStep loadstep) throws NotFoundException {
    for (LoadStep step = loadstep; step != null; step = step.getNext().orElse(null)) {
      final List<LoadOp<?>> loadOps = step.getOps().collect(Collectors.toList());
      if (loadOps.isEmpty()) {
        continue;
      }

      final List<ColumnFamilyHandle> columnFamilies = new ArrayList<>(loadOps.size());
      final List<byte[]> keys = new ArrayList<>(loadOps.size());
      loadOps.forEach(op -> {
        columnFamilies.add(getColumnFamilyHandle(op.getValueType()));
        keys.add(op.getId().toBytes());
      });

      final List<byte[]> reads;
      try {
        reads = rocksDB.multiGetAsList(columnFamilies, keys);
      } catch (RocksDBException e) {
        throw new RuntimeException("Load operation failed to load the keys given", e);
      }

      if (reads.size() != loadOps.size()) {
        throw new NotFoundException(String.format("[%d] object(s) missing in load.", loadOps.size() - reads.size()));
      }

      for (int i = 0; i < reads.size(); ++i) {
        final LoadOp<?> loadOp = loadOps.get(i);
        if (null == reads.get(i)) {
          throw new NotFoundException(String.format("Unable to find requested ref with ID: %s", loadOp.getId()));
        }

        @SuppressWarnings("rawtypes") final ValueType type = loadOp.getValueType();
        RocksSerDe.deserializeToConsumer(type, reads.get(i), loadOp.getReceiver());
        loadOp.done();
      }
    }
  }

  @Override
  public <C extends BaseValue<C>> boolean putIfAbsent(SaveOp<C> saveOp) {
    final ColumnFamilyHandle columnFamilyHandle = getColumnFamilyHandle(saveOp.getType());
    try (final Transaction transaction = rocksDB.beginTransaction(WRITE_OPTIONS)) {
      // Get exclusive access to key if it exists.
      final byte[] buffer = getForUpdate(transaction, columnFamilyHandle, saveOp.getId());
      if (null == buffer) {
        transaction.put(columnFamilyHandle, saveOp.getId().toBytes(), RocksSerDe.serializeWithConsumer(saveOp));
        transaction.commit();
        return true;
      }
      // Id already exists.
      return false;
    } catch (RocksDBException e) {
      throw new RuntimeException(String.format("putIfAbsent operation failed on %s for ID: %s",
          saveOp.getType().name(), saveOp.getId()), e);
    }
  }

  @Override
  public <C extends BaseValue<C>> void put(SaveOp<C> saveOp, Optional<ConditionExpression> condition) {
    final ColumnFamilyHandle columnFamilyHandle = getColumnFamilyHandle(saveOp.getType());

    try (final Transaction transaction = rocksDB.beginTransaction(WRITE_OPTIONS)) {
      try {
        isConditionExpressionValid(transaction, columnFamilyHandle, saveOp.getId(), saveOp.getType(), condition, "put");
      } catch (ConditionFailedException | NotFoundException e) {
        throw new ConditionFailedException(String.format("Condition failed during put operation. %s", e.getMessage()), e);
      }
      transaction.put(columnFamilyHandle, saveOp.getId().toBytes(), RocksSerDe.serializeWithConsumer(saveOp));
      transaction.commit();
    } catch (RocksDBException e) {
      throw new RuntimeException(String.format("put operation failed on %s for ID: %s", saveOp.getType().name(), saveOp.getId()), e);
    }
  }

  @Override
  public <C extends BaseValue<C>> boolean delete(ValueType<C> type, Id id, Optional<ConditionExpression> condition) {
    final ColumnFamilyHandle columnFamilyHandle = getColumnFamilyHandle(type);

    try (final Transaction transaction = rocksDB.beginTransaction(WRITE_OPTIONS)) {
      try {
        isConditionExpressionValid(transaction, columnFamilyHandle, id, type, condition, "delete");
      } catch (ConditionFailedException e) {
        LOGGER.debug("Condition failed during delete operation.");
        return false;
      }
      transaction.delete(columnFamilyHandle, id.toBytes());
      transaction.commit();
      return true;
    } catch (RocksDBException e) {
      throw new RuntimeException(String.format("put operation failed on %s for ID: %s", type.name(), id), e);
    }
  }

  @Override
  public void save(List<SaveOp<?>> ops) {
    final Map<ValueType<?>, List<SaveOp<?>>> perType = ops.stream().collect(Collectors.groupingBy(SaveOp::getType));

    try {
      final WriteBatch batch = new WriteBatch();
      for (Map.Entry<ValueType<?>, List<SaveOp<?>>> entry : perType.entrySet()) {
        for (SaveOp<?> op : entry.getValue()) {
          batch.put(getColumnFamilyHandle(entry.getKey()), op.getId().toBytes(), RocksSerDe.serializeWithConsumer(op));
        }
      }
      rocksDB.write(WRITE_OPTIONS, batch);
    } catch (RocksDBException e) {
      throw new RuntimeException("Save operation failed", e);
    }
  }

  @Override
  public <C extends BaseValue<C>> void loadSingle(ValueType<C> type, Id id, C consumer) {
    final ColumnFamilyHandle columnFamilyHandle = getColumnFamilyHandle(type);
    try {
      final byte[] buffer = rocksDB.get(columnFamilyHandle, id.toBytes());
      checkValue(buffer, "load", id);
      RocksSerDe.deserializeToConsumer(type, buffer, consumer);
    } catch (RocksDBException e) {
      throw new RuntimeException(String.format("put operation failed on %s for ID: %s", type.name(), id), e);
    }
  }

  @Override
  public <C extends BaseValue<C>> boolean update(ValueType<C> type, Id id, UpdateExpression update,
                                          Optional<ConditionExpression> condition, Optional<BaseValue<C>> consumer)
      throws NotFoundException {
    throw new UnsupportedOperationException();
  }

  @Override
  public <C extends BaseValue<C>> Stream<Acceptor<C>> getValues(ValueType<C> type) {
    final ColumnFamilyHandle columnFamilyHandle = getColumnFamilyHandle(ValueType.REF);
    final RocksIterator rocksIter = rocksDB.newIterator(columnFamilyHandle);

    final Iterable<Acceptor<C>> iterable = () -> new AbstractIterator<Acceptor<C>>() {
      private boolean isFirst = true;

      @Override
      protected Acceptor<C> computeNext() {
        if (isFirst) {
          rocksIter.seekToFirst();
          isFirst = false;
        } else {
          rocksIter.next();
        }

        if (rocksIter.isValid()) {
          return (consumer) -> RocksSerDe.deserializeToConsumer(type, rocksIter.value(), consumer);
        }

        rocksIter.close();
        return endOfData();
      }
    };

    return StreamSupport.stream(iterable.spliterator(), false).onClose(rocksIter::close);
  }


  @VisibleForTesting
  static List<Function> translate(ConditionExpression conditionExpression) {
    return conditionExpression.getFunctions().stream()
      .map(f -> f.accept(VALUE_VISITOR))
      .collect(Collectors.toList());
  }

  private <C extends BaseValue<C>> void isConditionExpressionValid(Transaction transaction, ColumnFamilyHandle columnFamilyHandle,
                                                                      Id id, ValueType<C> type, Optional<ConditionExpression> condition,
                                                                      String operation) throws RocksDBException, ConditionFailedException {
    if (condition.isPresent()) {
      final byte[] value = getAndCheckValue(transaction, columnFamilyHandle, id, operation);
      final RocksBaseValue<C> consumer = RocksSerDe.getConsumer(type);
      RocksSerDe.deserializeToConsumer(type, value, consumer);
      consumer.evaluate(translate(condition.get()));
    }
  }

  private byte[] getAndCheckValue(Transaction transaction, ColumnFamilyHandle columnFamilyHandle,
                                  Id id, String operation) throws RocksDBException {
    final byte[] value = getForUpdate(transaction, columnFamilyHandle, id);
    checkValue(value, operation, id);
    return value;
  }

  private byte[] getForUpdate(Transaction transaction, ColumnFamilyHandle columnFamilyHandle, Id id)
      throws RocksDBException {
    return transaction.getForUpdate(new ReadOptions(), columnFamilyHandle, id.toBytes(), true);
  }

  private void checkValue(byte[] value, String operation, Id id) {
    if (null == value) {
      throw new NotFoundException(String.format("Unable to %s item with ID: %s", operation, id));
    }
  }

  private <C extends BaseValue<C>> ColumnFamilyHandle getColumnFamilyHandle(ValueType<C> valueType) {
    final ColumnFamilyHandle columnFamilyHandle = valueTypeToColumnFamily.get(valueType);
    if (null == columnFamilyHandle) {
      throw new UnsupportedOperationException(String.format("Unsupported Entity type: %s", valueType.name()));
    }
    return columnFamilyHandle;
  }

  private List<ColumnFamilyDescriptor> getColumnFamilies(String dbPath) {
    final List<byte[]> defaultColumnFamilies = Stream.concat(
        Stream.of(RocksDB.DEFAULT_COLUMN_FAMILY),
        ValueType.values().stream().map(v -> v.getValueName().getBytes(UTF_8))).collect(ImmutableList.toImmutableList());

    List<byte[]> columnFamilies = null;
    try (final Options options = new Options().setCreateIfMissing(true)) {
      columnFamilies = RocksDB.listColumnFamilies(options, dbPath);

      if (!columnFamilies.isEmpty() && !defaultColumnFamilies.equals(columnFamilies)) {
        throw new RuntimeException(String.format("Unexpected format for Nessie database at '%s'.", dbPath));
      }
    } catch (RocksDBException e) {
      LOGGER.warn("Error listing column families for Nessie database, using defaults.", e);
    }

    if (columnFamilies == null || columnFamilies.isEmpty()) {
      columnFamilies = defaultColumnFamilies;
    }

    return columnFamilies.stream()
      .map(c -> new ColumnFamilyDescriptor(c, new ColumnFamilyOptions().optimizeUniversalStyleCompaction()))
      .collect(Collectors.toList());
  }

  private String verifyPath() {
    final Path dbDirectory = Paths.get(config.getDbDirectory());
    if (Files.exists(dbDirectory)) {
      if (!Files.isDirectory(dbDirectory)) {
        throw new RuntimeException(
          String.format("Invalid path '%s' for Nessie database, not a directory.", dbDirectory.toAbsolutePath()));
      }
    } else {
      try {
        Files.createDirectory(dbDirectory);
      } catch (IOException e) {
        throw new RuntimeException(
          String.format("Failed to create directory '%s' for Nessie database.", dbDirectory.toAbsolutePath()));
      }
    }

    return dbDirectory.toAbsolutePath().toString();
  }
}
