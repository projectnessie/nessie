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

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.rocksdb.FlushOptions;
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

  static {
    RocksDB.loadLibrary();
  }

  private TransactionDB rocksDB;
  private Map<ValueType<?>, ColumnFamilyHandle> valueTypeToColumnFamily;
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
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    if (null != rocksDB) {
      try (FlushOptions options = new FlushOptions().setWaitForFlush(true)) {
        valueTypeToColumnFamily.values().forEach(cf -> {
          try {
            rocksDB.flush(options, cf);
          } catch (RocksDBException e) {
            LOGGER.error("Error flushing column family while closing Nessie RocksDB store.", e);
          }
          cf.close();
        });
      }

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
        throw new RuntimeException(e);
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
    try (final Transaction transaction = rocksDB.beginTransaction(new WriteOptions())) {
      // Get exclusive access to key if it exists.
      final byte[] buffer = transaction.getForUpdate(new ReadOptions(), columnFamilyHandle, saveOp.getId().toBytes(), true);
      if (null == buffer) {
        transaction.put(columnFamilyHandle, saveOp.getId().toBytes(), RocksSerDe.serializeWithConsumer(saveOp));
        transaction.commit();
        return true;
      }
      // Id already exists.
      return false;
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public <C extends BaseValue<C>> void put(SaveOp<C> saveOp, Optional<ConditionExpression> condition) {
    final ColumnFamilyHandle columnFamilyHandle = getColumnFamilyHandle(saveOp.getType());

    try (final Transaction transaction = rocksDB.beginTransaction(new WriteOptions())) {
      if (condition.isPresent()) {
        final byte[] buffer = transaction.getForUpdate(new ReadOptions(), columnFamilyHandle, saveOp.getId().toBytes(), true);
        if (null == buffer) {
          throw new ConditionFailedException("Unable to load item with ID: " + saveOp.getId());
        }
        final RocksBaseValue<C> consumer = RocksSerDe.getConsumer(saveOp.getType());
        RocksSerDe.deserializeToConsumer(saveOp.getType(), buffer, consumer);

        if (!consumer.evaluate(translate(condition.get()))) {
          throw new ConditionFailedException("Condition failed during put operation");
        }
      }

      transaction.put(columnFamilyHandle, saveOp.getId().toBytes(), RocksSerDe.serializeWithConsumer(saveOp));
      transaction.commit();
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public <C extends BaseValue<C>> boolean delete(ValueType<C> type, Id id, Optional<ConditionExpression> condition) {
    final ColumnFamilyHandle columnFamilyHandle = getColumnFamilyHandle(type);

    try (final Transaction transaction = rocksDB.beginTransaction(new WriteOptions())) {
      final byte[] value = transaction.getForUpdate(new ReadOptions(), columnFamilyHandle, id.toBytes(), true);

      if (null == value) {
        throw new NotFoundException("No value was found for the given id to delete.");
      }

      if (condition.isPresent()) {
        final RocksBaseValue<C> consumer = RocksSerDe.getConsumer(type);
        // TODO: Does the critical section need to be locked?
        // TODO: Critical Section - evaluating the condition expression and deleting the entity.
        // Check if condition expression is valid.
        RocksSerDe.deserializeToConsumer(type, value, consumer);
        if (!(consumer.evaluate(translate(condition.get())))) {
          LOGGER.debug("Condition failed during delete operation.");
          return false;
        }
      }

      transaction.delete(columnFamilyHandle, id.toBytes());
      transaction.commit();
      return true;
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
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
      rocksDB.write(new WriteOptions(), batch);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public <C extends BaseValue<C>> void loadSingle(ValueType<C> type, Id id, C consumer) {
    final ColumnFamilyHandle columnFamilyHandle = getColumnFamilyHandle(type);
    try {
      final byte[] buffer = rocksDB.get(columnFamilyHandle, id.toBytes());
      if (null == buffer) {
        throw new NotFoundException("Unable to load item with ID: " + id);
      }
      RocksSerDe.deserializeToConsumer(type, buffer, consumer);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
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

    final Iterable<Acceptor<C>> iterable = () -> new AbstractIterator<Acceptor<C>>() {
      private final RocksIterator itr = rocksDB.newIterator(columnFamilyHandle);
      private boolean isFirst = true;

      @Override
      protected Acceptor<C> computeNext() {
        if (isFirst) {
          itr.seekToFirst();
          isFirst = false;
        } else {
          itr.next();
        }

        if (itr.isValid()) {
          return (consumer) -> RocksSerDe.deserializeToConsumer(type, itr.value(), consumer);
        }

        itr.close();
        return endOfData();
      }
    };

    return StreamSupport.stream(iterable.spliterator(), false);
  }

  /**
   * Delete all the data in all column families, used for testing only.
   */
  @VisibleForTesting
  void deleteAllData() {
    // RocksDB doesn't expose a way to get the min/max key for a column family, so just use the min/max possible.
    byte[] minId = new byte[20];
    byte[] maxId = new byte[20];
    Arrays.fill(minId, (byte)0);
    Arrays.fill(maxId, (byte)255);

    for (ColumnFamilyHandle handle : valueTypeToColumnFamily.values()) {
      try {
        rocksDB.deleteRange(handle, minId, maxId);
        // Since RocksDB#deleteRange() is exclusive of the max key, delete it to ensure the column family is empty.
        rocksDB.delete(maxId);
      } catch (RocksDBException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @VisibleForTesting
  static List<Function> translate(ConditionExpression conditionExpression) {
    return conditionExpression.getFunctions().stream()
      .map(f -> f.accept(VALUE_VISITOR))
      .collect(Collectors.toList());
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
    final File dbDirectory = new File(config.getDbDirectory());
    if (dbDirectory.exists()) {
      if (!dbDirectory.isDirectory()) {
        throw new RuntimeException(
          String.format("Invalid path '%s' for Nessie database, not a directory.", dbDirectory.getAbsolutePath()));
      }
    } else if (!dbDirectory.mkdirs()) {
      throw new RuntimeException(
          String.format("Failed to create directory '%s' for Nessie database.", dbDirectory.getAbsolutePath()));
    }

    return dbDirectory.getAbsolutePath();
  }
}
