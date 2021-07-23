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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.cbor.databind.CBORMapper;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.tiered.adapter.CommitLogEntry;
import org.projectnessie.versioned.tiered.adapter.spi.DbObjectsSerializers;
import org.projectnessie.versioned.tiered.nontx.GlobalStateLogEntry;
import org.projectnessie.versioned.tiered.nontx.GlobalStatePointer;
import org.projectnessie.versioned.tiered.nontx.NonTxDatabaseAdapter;
import org.projectnessie.versioned.tiered.nontx.NonTxOperationContext;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.Holder;
import org.rocksdb.RocksDBException;
import org.rocksdb.TransactionDB;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

public class RocksDatabaseAdapter extends NonTxDatabaseAdapter<RocksDatabaseAdapterConfig> {

  private final TransactionDB db;
  private final RocksDbInstance dbInstance;

  private final ByteString keyPrefix;

  private static final ObjectMapper MAPPER =
      DbObjectsSerializers.register(new CBORMapper().findAndRegisterModules());

  public RocksDatabaseAdapter(RocksDatabaseAdapterConfig config) {
    super(config);

    this.keyPrefix = ByteString.copyFromUtf8(config.getKeyPrefix() + ':');

    // get the externally configured RocksDbInstance
    RocksDbInstance dbInstance = config.getDbInstance();

    if (dbInstance == null) {
      // Create a RocksDbInstance, if none has been configured externally. This is mostly used
      // for tests and benchmarks.
      dbInstance = new RocksDbInstance();
      dbInstance.setDbPath(config.getDbPath());
    }

    this.dbInstance = dbInstance;
    try {
      db = dbInstance.start();
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    dbInstance.close();
  }

  private byte[] dbKey(Hash hash) {
    return keyPrefix.concat(hash.asBytes()).toByteArray();
  }

  private byte[] dbKey(ByteString key) {
    return keyPrefix.concat(key).toByteArray();
  }

  private byte[] globalPointerKey() {
    // TODO this should be a "final field"
    return dbKey(ByteString.EMPTY);
  }

  private byte[] serialize(Object entry) {
    try {
      return MAPPER.writeValueAsBytes(entry);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private <T> T deserialize(byte[] bytes, Class<T> type) {
    try {
      return bytes != null ? MAPPER.readValue(bytes, type) : null;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void reinitializeRepo() throws ReferenceConflictException {
    try {
      db.delete(dbInstance.getCfGlobalPointer(), globalPointerKey());
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
    super.initializeRepo();
  }

  @Override
  protected GlobalStatePointer fetchGlobalPointer(NonTxOperationContext ctx) {
    try {
      byte[] serialized = db.get(dbInstance.getCfGlobalPointer(), globalPointerKey());
      return deserialize(serialized, GlobalStatePointer.class);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void writeIndividualCommit(NonTxOperationContext ctx, CommitLogEntry entry)
      throws ReferenceConflictException {
    Lock lock = dbInstance.getLock().writeLock();
    lock.lock();
    try {
      byte[] key = dbKey(entry.getHash());
      if (db.keyMayExist(key, new Holder<>())) {
        throw hashCollisionDetected();
      }

      db.put(dbInstance.getCfCommitLog(), key, serialize(entry));
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    } finally {
      lock.unlock();
    }
  }

  @Override
  protected void writeIndividualCommits(NonTxOperationContext ctx, List<CommitLogEntry> entries)
      throws ReferenceConflictException {
    Lock lock = dbInstance.getLock().writeLock();
    lock.lock();
    try {
      WriteBatch batch = new WriteBatch();
      for (CommitLogEntry e : entries) {
        byte[] key = dbKey(e.getHash());
        batch.put(dbInstance.getCfCommitLog(), key, serialize(e));
      }
      db.write(new WriteOptions(), batch);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    } finally {
      lock.unlock();
    }
  }

  @Override
  protected void writeGlobalCommit(NonTxOperationContext ctx, GlobalStateLogEntry entry)
      throws ReferenceConflictException {
    Lock lock = dbInstance.getLock().writeLock();
    lock.lock();
    try {
      byte[] key = dbKey(entry.getId());
      if (db.keyMayExist(key, new Holder<>())) {
        throw hashCollisionDetected();
      }

      db.put(dbInstance.getCfGlobalLog(), key, serialize(entry));
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    } finally {
      lock.unlock();
    }
  }

  @Override
  protected void unsafeWriteGlobalPointer(NonTxOperationContext ctx, GlobalStatePointer pointer) {
    try {
      db.put(dbInstance.getCfGlobalPointer(), globalPointerKey(), serialize(pointer));
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  protected boolean globalPointerCas(
      NonTxOperationContext ctx, GlobalStatePointer expected, GlobalStatePointer newPointer) {
    Lock lock = dbInstance.getLock().writeLock();
    lock.lock();
    try {
      GlobalStatePointer oldPointer =
          deserialize(
              db.get(dbInstance.getCfGlobalPointer(), globalPointerKey()),
              GlobalStatePointer.class);
      if (oldPointer == null || !oldPointer.getGlobalId().equals(expected.getGlobalId())) {
        return false;
      }
      db.put(dbInstance.getCfGlobalPointer(), globalPointerKey(), serialize(newPointer));
      return true;
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    } finally {
      lock.unlock();
    }
  }

  @Override
  protected void cleanUpCommitCas(
      NonTxOperationContext ctx, Hash globalId, Set<Hash> branchCommits) {
    Lock lock = dbInstance.getLock().writeLock();
    lock.lock();
    try {
      WriteBatch batch = new WriteBatch();
      batch.delete(dbInstance.getCfGlobalLog(), dbKey(globalId));
      for (Hash h : branchCommits) {
        batch.delete(dbInstance.getCfCommitLog(), dbKey(h));
      }
      db.write(new WriteOptions(), batch);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    } finally {
      lock.unlock();
    }
  }

  @Override
  protected GlobalStateLogEntry fetchFromGlobalLog(NonTxOperationContext ctx, Hash id) {
    try {
      byte[] v = db.get(dbInstance.getCfGlobalLog(), dbKey(id));
      return deserialize(v, GlobalStateLogEntry.class);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected CommitLogEntry fetchFromCommitLog(NonTxOperationContext ctx, Hash hash) {
    try {
      byte[] v = db.get(dbInstance.getCfCommitLog(), dbKey(hash));
      return deserialize(v, CommitLogEntry.class);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected List<CommitLogEntry> fetchPageFromCommitLog(
      NonTxOperationContext ctx, List<Hash> hashes) {
    return fetchPage(dbInstance.getCfCommitLog(), CommitLogEntry.class, hashes);
  }

  @Override
  protected List<GlobalStateLogEntry> fetchPageFromGlobalLog(
      NonTxOperationContext ctx, List<Hash> hashes) {
    return fetchPage(dbInstance.getCfGlobalLog(), GlobalStateLogEntry.class, hashes);
  }

  private <T> List<T> fetchPage(ColumnFamilyHandle cfHandle, Class<T> type, List<Hash> hashes) {
    try {
      List<ColumnFamilyHandle> cf = new ArrayList<>(hashes.size());
      for (int i = 0; i < hashes.size(); i++) {
        cf.add(cfHandle);
      }
      List<byte[]> result =
          db.multiGetAsList(cf, hashes.stream().map(this::dbKey).collect(Collectors.toList()));
      return result.stream().map(v -> deserialize(v, type)).collect(Collectors.toList());
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }
}
