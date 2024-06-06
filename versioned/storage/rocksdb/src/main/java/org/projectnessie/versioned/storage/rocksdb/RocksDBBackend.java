/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.versioned.storage.rocksdb;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Arrays.asList;
import static org.projectnessie.versioned.storage.common.util.Closing.closeMultiple;
import static org.rocksdb.RocksDB.DEFAULT_COLUMN_FAMILY;

import jakarta.annotation.Nonnull;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.common.config.StoreConfig;
import org.projectnessie.versioned.storage.common.persist.Backend;
import org.projectnessie.versioned.storage.common.persist.PersistFactory;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.TransactionDB;
import org.rocksdb.TransactionDBOptions;

public final class RocksDBBackend implements Backend {
  public static final String CF_REFERENCES = "nessie_refs";
  public static final String CF_OBJECTS = "nessie_objects";

  private static final List<String> CF_ALL = asList(CF_REFERENCES, CF_OBJECTS);

  private final RocksDBBackendConfig config;

  private TransactionDB db;
  private ColumnFamilyHandle cfReferences;
  private ColumnFamilyHandle cfObjects;

  private final Map<String, RocksDBRepo> repositories = new ConcurrentHashMap<>();

  public RocksDBBackend(RocksDBBackendConfig config) {
    RocksDB.loadLibrary();
    this.config = config;
  }

  List<ColumnFamilyHandle> all() {
    return asList(cfReferences, cfObjects);
  }

  TransactionDB db() {
    return db;
  }

  ColumnFamilyHandle refs() {
    return cfReferences;
  }

  ColumnFamilyHandle objs() {
    return cfObjects;
  }

  @Override
  public synchronized void close() {
    if (db != null) {
      try {
        closeMultiple(cfObjects, cfReferences, db);
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        db = null;
        cfReferences = null;
        cfObjects = null;
      }
    }
  }

  private synchronized void initialize() {
    if (db == null) {
      Path dbPath = config.databasePath();

      checkState(dbPath != null, "RocksDB instance is missing the databasePath option.");
      checkState(
          !Files.exists(dbPath) || Files.isDirectory(dbPath),
          "RocksDB cannot use databasePath %s.",
          dbPath);

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
                dbPath.toString(),
                columnFamilyDescriptors,
                columnFamilyHandles);

        Map<String, ColumnFamilyHandle> columnFamilyHandleMap = new HashMap<>();
        for (int i = 0; i < CF_ALL.size(); i++) {
          String cf = CF_ALL.get(i);
          columnFamilyHandleMap.put(cf, columnFamilyHandles.get(i + 1));
        }

        cfReferences = columnFamilyHandleMap.get(CF_REFERENCES);
        cfObjects = columnFamilyHandleMap.get(CF_OBJECTS);
      } catch (RocksDBException e) {
        throw new RuntimeException("RocksDB failed to start", e);
      }
    }
  }

  @Override
  public Optional<String> setupSchema() {
    initialize();
    return Optional.of("database path: " + config.databasePath());
  }

  @Nonnull
  @Override
  public PersistFactory createFactory() {
    initialize();
    return new RocksDBPersistFactory(this);
  }

  RocksDBRepo repo(StoreConfig config) {
    return repositories.computeIfAbsent(config.repositoryId(), r -> new RocksDBRepo());
  }

  @Override
  public void eraseRepositories(Set<String> repositoryIds) {
    if (repositoryIds == null || repositoryIds.isEmpty()) {
      return;
    }

    // erase() does not use any lock, it's use is rare, taking the risk of having a corrupted,
    // erased repo

    @SuppressWarnings("resource")
    TransactionDB db = db();

    List<ByteString> prefixed =
        repositoryIds.stream().map(RocksDBBackend::keyPrefix).collect(Collectors.toList());

    all()
        .forEach(
            cf -> {
              try (RocksIterator iter = db.newIterator(cf)) {
                List<ByteString> deletes = new ArrayList<>();
                for (iter.seekToFirst(); iter.isValid(); iter.next()) {
                  ByteString key = ByteString.copyFrom(iter.key());
                  if (prefixed.stream().anyMatch(key::startsWith)) {
                    deletes.add(key);
                  }
                }
                deletes.forEach(
                    key -> {
                      try {
                        db.delete(cf, key.toByteArray());
                      } catch (RocksDBException e) {
                        throw rocksDbException(e);
                      }
                    });
              }
            });
  }

  static RuntimeException rocksDbException(RocksDBException e) {
    throw new RuntimeException("Unhandled RocksDB exception", e);
  }

  static ByteString keyPrefix(String repositoryId) {
    return ByteString.copyFromUtf8(repositoryId + ':');
  }
}
