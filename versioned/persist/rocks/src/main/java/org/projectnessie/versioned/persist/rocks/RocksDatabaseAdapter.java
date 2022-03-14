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
import static org.projectnessie.versioned.persist.serialize.ProtoSerialization.protoToRefLog;
import static org.projectnessie.versioned.persist.serialize.ProtoSerialization.protoToRepoDescription;
import static org.projectnessie.versioned.persist.serialize.ProtoSerialization.toProto;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
import org.projectnessie.versioned.persist.adapter.ContentVariantSupplier;
import org.projectnessie.versioned.persist.adapter.KeyListEntity;
import org.projectnessie.versioned.persist.adapter.KeyListEntry;
import org.projectnessie.versioned.persist.adapter.RefLog;
import org.projectnessie.versioned.persist.adapter.RepoDescription;
import org.projectnessie.versioned.persist.nontx.NonTransactionalDatabaseAdapter;
import org.projectnessie.versioned.persist.nontx.NonTransactionalDatabaseAdapterConfig;
import org.projectnessie.versioned.persist.nontx.NonTransactionalOperationContext;
import org.projectnessie.versioned.persist.serialize.AdapterTypes;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.GlobalStateLogEntry;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.GlobalStatePointer;
import org.projectnessie.versioned.persist.serialize.ProtoSerialization;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.Holder;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
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
      NonTransactionalDatabaseAdapterConfig config,
      RocksDbInstance dbInstance,
      ContentVariantSupplier contentVariantSupplier) {
    super(config, contentVariantSupplier);

    this.keyPrefix = ByteString.copyFromUtf8(config.getRepositoryId() + ':');
    this.globalPointerKey = ByteString.copyFromUtf8(config.getRepositoryId()).toByteArray();

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
  public void eraseRepo() {
    try {
      db.delete(dbInstance.getCfGlobalPointer(), globalPointerKey());
      Stream.of(
              dbInstance.getCfGlobalPointer(),
              dbInstance.getCfGlobalLog(),
              dbInstance.getCfCommitLog(),
              dbInstance.getCfRepoProps(),
              dbInstance.getCfKeyList(),
              dbInstance.getCfRefLog())
          .forEach(
              cf -> {
                try (RocksIterator iter = db.newIterator(cf)) {
                  List<ByteString> deletes = new ArrayList<>();
                  for (iter.seekToFirst(); iter.isValid(); iter.next()) {
                    ByteString key = ByteString.copyFrom(iter.key());
                    if (key.startsWith(keyPrefix)) {
                      deletes.add(key);
                    }
                  }
                  deletes.forEach(
                      key -> {
                        try {
                          db.delete(cf, key.toByteArray());
                        } catch (RocksDBException e) {
                          throw new RuntimeException(e);
                        }
                      });
                }
              });
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected GlobalStatePointer doFetchGlobalPointer(NonTransactionalOperationContext ctx) {
    try {
      byte[] serialized = db.get(dbInstance.getCfGlobalPointer(), globalPointerKey());
      return serialized != null ? GlobalStatePointer.parseFrom(serialized) : null;
    } catch (InvalidProtocolBufferException | RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void doWriteIndividualCommit(NonTransactionalOperationContext ctx, CommitLogEntry entry)
      throws ReferenceConflictException {
    Lock lock = dbInstance.getLock().writeLock();
    lock.lock();
    try {
      byte[] key = dbKey(entry.getHash());
      checkForHashCollision(dbInstance.getCfCommitLog(), key);
      db.put(dbInstance.getCfCommitLog(), key, toProto(entry).toByteArray());
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    } finally {
      lock.unlock();
    }
  }

  @Override
  protected void doWriteMultipleCommits(
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
  protected void doWriteGlobalCommit(
      NonTransactionalOperationContext ctx, GlobalStateLogEntry entry)
      throws ReferenceConflictException {
    Lock lock = dbInstance.getLock().writeLock();
    lock.lock();
    try {
      byte[] key = dbKey(entry.getId());
      checkForHashCollision(dbInstance.getCfGlobalLog(), key);
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
  protected boolean doGlobalPointerCas(
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
  protected void doCleanUpCommitCas(
      NonTransactionalOperationContext ctx,
      Hash globalId,
      Set<Hash> branchCommits,
      Set<Hash> newKeyLists,
      Hash refLogId) {
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
      batch.delete(dbInstance.getCfRefLog(), dbKey(refLogId));
      db.write(new WriteOptions(), batch);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    } finally {
      lock.unlock();
    }
  }

  @Override
  protected void doCleanUpGlobalLog(
      NonTransactionalOperationContext ctx, Collection<Hash> globalIds) {
    Lock lock = dbInstance.getLock().writeLock();
    lock.lock();
    try {
      WriteBatch batch = new WriteBatch();
      for (Hash h : globalIds) {
        batch.delete(dbInstance.getCfGlobalLog(), dbKey(h));
      }
      db.write(new WriteOptions(), batch);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    } finally {
      lock.unlock();
    }
  }

  @Override
  protected GlobalStateLogEntry doFetchFromGlobalLog(
      NonTransactionalOperationContext ctx, Hash id) {
    try {
      byte[] v = db.get(dbInstance.getCfGlobalLog(), dbKey(id));
      return v != null ? GlobalStateLogEntry.parseFrom(v) : null;
    } catch (InvalidProtocolBufferException | RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected CommitLogEntry doFetchFromCommitLog(NonTransactionalOperationContext ctx, Hash hash) {
    try {
      byte[] v = db.get(dbInstance.getCfCommitLog(), dbKey(hash));
      return protoToCommitLogEntry(v);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected List<CommitLogEntry> doFetchMultipleFromCommitLog(
      NonTransactionalOperationContext ctx, List<Hash> hashes) {
    return fetchPage(
        dbInstance.getCfCommitLog(), hashes, ProtoSerialization::protoToCommitLogEntry);
  }

  @Override
  protected List<GlobalStateLogEntry> doFetchPageFromGlobalLog(
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
  protected void doWriteKeyListEntities(
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
  protected Stream<KeyListEntity> doFetchKeyLists(
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
  protected RepoDescription doFetchRepositoryDescription(NonTransactionalOperationContext ctx) {
    try {
      byte[] bytes = db.get(dbInstance.getCfRepoProps(), globalPointerKey());
      return bytes != null ? protoToRepoDescription(bytes) : null;
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected boolean doTryUpdateRepositoryDescription(
      NonTransactionalOperationContext ctx, RepoDescription expected, RepoDescription updateTo) {
    Lock lock = dbInstance.getLock().writeLock();
    lock.lock();
    try {
      byte[] bytes = db.get(dbInstance.getCfRepoProps(), globalPointerKey());
      byte[] updatedBytes = toProto(updateTo).toByteArray();
      if ((bytes == null && expected == null)
          || (bytes != null && Arrays.equals(bytes, toProto(expected).toByteArray()))) {
        db.put(dbInstance.getCfRepoProps(), globalPointerKey(), updatedBytes);
        return true;
      }
      return false;
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    } finally {
      lock.unlock();
    }
  }

  @Override
  protected int entitySize(CommitLogEntry entry) {
    return toProto(entry).getSerializedSize();
  }

  @Override
  protected int entitySize(KeyListEntry entry) {
    return toProto(entry).getSerializedSize();
  }

  @Override
  protected void doWriteRefLog(NonTransactionalOperationContext ctx, AdapterTypes.RefLogEntry entry)
      throws ReferenceConflictException {
    Lock lock = dbInstance.getLock().writeLock();
    lock.lock();
    try {
      byte[] key = dbKey(entry.getRefLogId());
      checkForHashCollision(dbInstance.getCfRefLog(), key);
      db.put(dbInstance.getCfRefLog(), key, entry.toByteArray());
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    } finally {
      lock.unlock();
    }
  }

  @Override
  protected RefLog doFetchFromRefLog(NonTransactionalOperationContext ctx, Hash refLogId) {
    if (refLogId == null) {
      // set the current head as refLogId
      refLogId = Hash.of(fetchGlobalPointer(ctx).getRefLogId());
    }
    try {
      byte[] v = db.get(dbInstance.getCfRefLog(), dbKey(refLogId));
      return protoToRefLog(v);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected List<RefLog> doFetchPageFromRefLog(
      NonTransactionalOperationContext ctx, List<Hash> hashes) {
    return fetchPage(dbInstance.getCfRefLog(), hashes, ProtoSerialization::protoToRefLog);
  }

  private void checkForHashCollision(ColumnFamilyHandle cf, byte[] key)
      throws ReferenceConflictException, RocksDBException {
    Holder<byte[]> value = new Holder<>();
    // "may" exist is not "does really" exist, so check if a value was found
    if (db.keyMayExist(cf, key, value) && value.getValue() != null) {
      throw hashCollisionDetected();
    }
  }
}
