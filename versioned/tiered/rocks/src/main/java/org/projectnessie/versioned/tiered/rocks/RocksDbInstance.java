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
package org.projectnessie.versioned.tiered.rocks;

import static org.rocksdb.RocksDB.DEFAULT_COLUMN_FAMILY;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.StampedLock;
import java.util.stream.Collectors;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.TransactionDB;
import org.rocksdb.TransactionDBOptions;

/**
 * Provides the {@link RocksDB} instance for potentially multiple {@link RocksDatabaseAdapter}
 * instances.
 */
public class RocksDbInstance implements AutoCloseable {

  private TransactionDB db;

  private String dbPath;

  public static final String CF_GLOBAL_POINTER = "global_pointer";
  public static final String CF_GLOBAL_LOG = "global_log";
  public static final String CF_COMMIT_LOG = "commit_log";

  public static final List<String> CF_ALL =
      Arrays.asList(CF_GLOBAL_POINTER, CF_GLOBAL_LOG, CF_COMMIT_LOG);

  private ColumnFamilyHandle cfGlobalPointer;
  private ColumnFamilyHandle cfGlobalLog;
  private ColumnFamilyHandle cfCommitLog;

  private final ReadWriteLock lock = new StampedLock().asReadWriteLock();

  private int starts;

  public RocksDbInstance() {
    RocksDB.loadLibrary();
  }

  public void setDbPath(String dbPath) {
    this.dbPath = dbPath;
  }

  public synchronized TransactionDB start() throws RocksDBException {
    if (db == null) {
      if (dbPath == null || dbPath.trim().isEmpty()) {
        throw new IllegalStateException("RocksDB instance missing dbPath option.");
      }

      List<byte[]> columnFamilies = new ArrayList<>();
      columnFamilies.add(DEFAULT_COLUMN_FAMILY);
      CF_ALL.stream().map(s -> s.getBytes(StandardCharsets.UTF_8)).forEach(columnFamilies::add);

      List<ColumnFamilyDescriptor> columnFamilyDescriptors =
          columnFamilies.stream()
              .map(
                  c ->
                      new ColumnFamilyDescriptor(
                          c, new ColumnFamilyOptions().optimizeUniversalStyleCompaction()))
              .collect(Collectors.toList());

      try (final DBOptions dbOptions =
          new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true)) {
        // TODO: Consider setting WAL limits.
        List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
        db =
            TransactionDB.open(
                dbOptions,
                new TransactionDBOptions(),
                dbPath,
                columnFamilyDescriptors,
                columnFamilyHandles);

        Map<String, ColumnFamilyHandle> columnFamilyHandleMap = new HashMap<>();
        for (int i = 0; i < CF_ALL.size(); i++) {
          String cf = CF_ALL.get(i);
          columnFamilyHandleMap.put(cf, columnFamilyHandles.get(i + 1));
        }

        cfGlobalPointer = columnFamilyHandleMap.get(CF_GLOBAL_POINTER);
        cfGlobalLog = columnFamilyHandleMap.get(CF_GLOBAL_LOG);
        cfCommitLog = columnFamilyHandleMap.get(CF_COMMIT_LOG);
      } catch (RocksDBException e) {
        throw new RuntimeException("RocksDB failed to start", e);
      }
    }
    starts++;
    return db;
  }

  @Override
  public synchronized void close() {
    if (db != null) {
      starts--;
      if (starts == 0) {
        db.close();
      }
    }
  }

  public ColumnFamilyHandle getCfGlobalPointer() {
    return cfGlobalPointer;
  }

  public ColumnFamilyHandle getCfGlobalLog() {
    return cfGlobalLog;
  }

  public ColumnFamilyHandle getCfCommitLog() {
    return cfCommitLog;
  }

  public ReadWriteLock getLock() {
    return lock;
  }
}
