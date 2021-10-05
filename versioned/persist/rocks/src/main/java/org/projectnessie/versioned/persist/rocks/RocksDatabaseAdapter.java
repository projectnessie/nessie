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
package org.projectnessie.versioned.persist.rocks;

import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.hashCollisionDetected;
import static org.projectnessie.versioned.persist.serialize.ProtoSerialization.protoToCommitLogEntry;
import static org.projectnessie.versioned.persist.serialize.ProtoSerialization.protoToKeyList;
import static org.projectnessie.versioned.persist.serialize.ProtoSerialization.toProto;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.KeyListEntity;
import org.projectnessie.versioned.persist.adapter.KeyWithType;
import org.projectnessie.versioned.persist.nontx.NonTransactionalDatabaseAdapter;
import org.projectnessie.versioned.persist.nontx.NonTransactionalDatabaseAdapterConfig;
import org.projectnessie.versioned.persist.nontx.NonTransactionalOperationContext;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.GlobalStateLogEntry;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.GlobalStatePointer;
import org.projectnessie.versioned.persist.serialize.ProtoSerialization;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.Holder;
import org.rocksdb.RocksDBException;
import org.rocksdb.TransactionDB;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

public class RocksDatabaseAdapter
    extends NonTransactionalDatabaseAdapter<NonTransactionalDatabaseAdapterConfig> {

  private final TransactionDB db;
  private final RocksDbInstance dbInstance;

  private final ByteString keyPrefix;
  private final byte[] globalPointerKey;

  public RocksDatabaseAdapter(
      NonTransactionalDatabaseAdapterConfig config, RocksDbInstance dbInstance) {
    super(config);

    this.keyPrefix = ByteString.copyFromUtf8(config.getKeyPrefix() + ':');
    this.globalPointerKey = ByteString.copyFromUtf8(config.getKeyPrefix()).toByteArray();

    // get the externally configured RocksDbInstance
    Objects.requireNonNull(
        dbInstance, "Requires a non-null RocksDbInstance from RocksDatabaseAdapterConfig");

    this.dbInstance = dbInstance;
    this.db = dbInstance.getDb();
  }

  private byte[] dbKey(Hash hash) {
    return keyPrefix.concat(hash.asBytes()).toByteArray();
  }

  private byte[] dbKey(ByteString key) {
    return keyPrefix.concat(key).toByteArray();
  }

  private byte[] globalPointerKey() {
    return globalPointerKey;
  }

  @Override
  public void reinitializeRepo(String defaultBranchName) {
    try {
      db.delete(dbInstance.getCfGlobalPointer(), globalPointerKey());
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
    super.initializeRepo(defaultBranchName);
  }

  @Override
  protected GlobalStatePointer fetchGlobalPointer(NonTransactionalOperationContext ctx) {
    try {
      byte[] serialized = db.get(dbInstance.getCfGlobalPointer(), globalPointerKey());
      return serialized != null ? GlobalStatePointer.parseFrom(serialized) : null;
    } catch (InvalidProtocolBufferException | RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void writeIndividualCommit(NonTransactionalOperationContext ctx, CommitLogEntry entry)
      throws ReferenceConflictException {
    Lock lock = dbInstance.getLock().writeLock();
    lock.lock();
    try {
      byte[] key = dbKey(entry.getHash());
      if (db.keyMayExist(key, new Holder<>())) {
        throw hashCollisionDetected();
      }

      db.put(dbInstance.getCfCommitLog(), key, toProto(entry).toByteArray());
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    } finally {
      lock.unlock();
    }
  }

  @Override
  protected void writeMultipleCommits(
      NonTransactionalOperationContext ctx, List<CommitLogEntry> entries) {
    Lock lock = dbInstance.getLock().writeLock();
    lock.lock();
    try {
      WriteBatch batch = new WriteBatch();
      for (CommitLogEntry e : entries) {
        byte[] key = dbKey(e.getHash());
        batch.put(dbInstance.getCfCommitLog(), key, toProto(e).toByteArray());
      }
      db.write(new WriteOptions(), batch);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    } finally {
      lock.unlock();
    }
  }

  @Override
  protected void writeGlobalCommit(NonTransactionalOperationContext ctx, GlobalStateLogEntry entry)
      throws ReferenceConflictException {
    Lock lock = dbInstance.getLock().writeLock();
    lock.lock();
    try {
      byte[] key = dbKey(entry.getId());
      if (db.keyMayExist(key, new Holder<>())) {
        throw hashCollisionDetected();
      }

      db.put(dbInstance.getCfGlobalLog(), key, entry.toByteArray());
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    } finally {
      lock.unlock();
    }
  }

  @Override
  protected void unsafeWriteGlobalPointer(
      NonTransactionalOperationContext ctx, GlobalStatePointer pointer) {
    try {
      db.put(dbInstance.getCfGlobalPointer(), globalPointerKey(), pointer.toByteArray());
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected boolean globalPointerCas(
      NonTransactionalOperationContext ctx,
      GlobalStatePointer expected,
      GlobalStatePointer newPointer) {
    Lock lock = dbInstance.getLock().writeLock();
    lock.lock();
    try {
      byte[] bytes = db.get(dbInstance.getCfGlobalPointer(), globalPointerKey());
      GlobalStatePointer oldPointer = bytes != null ? GlobalStatePointer.parseFrom(bytes) : null;
      if (oldPointer == null || !oldPointer.getGlobalId().equals(expected.getGlobalId())) {
        return false;
      }
      db.put(dbInstance.getCfGlobalPointer(), globalPointerKey(), newPointer.toByteArray());
      return true;
    } catch (InvalidProtocolBufferException | RocksDBException e) {
      throw new RuntimeException(e);
    } finally {
      lock.unlock();
    }
  }

  @Override
  protected void cleanUpCommitCas(
      NonTransactionalOperationContext ctx,
      Hash globalId,
      Set<Hash> branchCommits,
      Set<Hash> newKeyLists) {
    Lock lock = dbInstance.getLock().writeLock();
    lock.lock();
    try {
      WriteBatch batch = new WriteBatch();
      batch.delete(dbInstance.getCfGlobalLog(), dbKey(globalId));
      for (Hash h : branchCommits) {
        batch.delete(dbInstance.getCfCommitLog(), dbKey(h));
      }
      for (Hash h : newKeyLists) {
        batch.delete(dbInstance.getCfKeyList(), dbKey(h));
      }
      db.write(new WriteOptions(), batch);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    } finally {
      lock.unlock();
    }
  }

  @Override
  protected GlobalStateLogEntry fetchFromGlobalLog(NonTransactionalOperationContext ctx, Hash id) {
    try {
      byte[] v = db.get(dbInstance.getCfGlobalLog(), dbKey(id));
      return v != null ? GlobalStateLogEntry.parseFrom(v) : null;
    } catch (InvalidProtocolBufferException | RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected CommitLogEntry fetchFromCommitLog(NonTransactionalOperationContext ctx, Hash hash) {
    try {
      byte[] v = db.get(dbInstance.getCfCommitLog(), dbKey(hash));
      return protoToCommitLogEntry(v);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected List<CommitLogEntry> fetchPageFromCommitLog(
      NonTransactionalOperationContext ctx, List<Hash> hashes) {
    return fetchPage(
        dbInstance.getCfCommitLog(), hashes, ProtoSerialization::protoToCommitLogEntry);
  }

  @Override
  protected List<GlobalStateLogEntry> fetchPageFromGlobalLog(
      NonTransactionalOperationContext ctx, List<Hash> hashes) {
    return fetchPage(
        dbInstance.getCfGlobalLog(),
        hashes,
        v -> {
          try {
            return v != null ? GlobalStateLogEntry.parseFrom(v) : null;
          } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
          }
        });
  }

  private <T> List<T> fetchPage(
      ColumnFamilyHandle cfHandle, List<Hash> hashes, Function<byte[], T> deserializer) {
    try {
      List<ColumnFamilyHandle> cf = new ArrayList<>(hashes.size());
      for (int i = 0; i < hashes.size(); i++) {
        cf.add(cfHandle);
      }
      List<byte[]> result =
          db.multiGetAsList(cf, hashes.stream().map(this::dbKey).collect(Collectors.toList()));
      return result.stream().map(deserializer).collect(Collectors.toList());
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void writeKeyListEntities(
      NonTransactionalOperationContext ctx, List<KeyListEntity> newKeyListEntities) {
    Lock lock = dbInstance.getLock().writeLock();
    lock.lock();
    try {
      for (KeyListEntity keyListEntity : newKeyListEntities) {
        byte[] key = dbKey(keyListEntity.getId());
        db.put(dbInstance.getCfKeyList(), key, toProto(keyListEntity.getKeys()).toByteArray());
      }
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    } finally {
      lock.unlock();
    }
  }

  @Override
  protected Stream<KeyListEntity> fetchKeyLists(
      NonTransactionalOperationContext ctx, List<Hash> keyListsIds) {
    try {
      List<ColumnFamilyHandle> cf = new ArrayList<>(keyListsIds.size());
      for (int i = 0; i < keyListsIds.size(); i++) {
        cf.add(dbInstance.getCfKeyList());
      }
      List<byte[]> result =
          db.multiGetAsList(cf, keyListsIds.stream().map(this::dbKey).collect(Collectors.toList()));
      return IntStream.range(0, keyListsIds.size())
          .mapToObj(
              i -> {
                byte[] v = result.get(i);
                return KeyListEntity.of(keyListsIds.get(i), protoToKeyList(v));
              });
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected int entitySize(CommitLogEntry entry) {
    return toProto(entry).getSerializedSize();
  }

  @Override
  protected int entitySize(KeyWithType entry) {
    return toProto(entry).getSerializedSize();
  }
}
