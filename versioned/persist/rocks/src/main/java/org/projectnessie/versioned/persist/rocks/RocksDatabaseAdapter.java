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

import static org.projectnessie.versioned.persist.adapter.serialize.ProtoSerialization.protoToCommitLogEntry;
import static org.projectnessie.versioned.persist.adapter.serialize.ProtoSerialization.protoToKeyList;
import static org.projectnessie.versioned.persist.adapter.serialize.ProtoSerialization.protoToRefLog;
import static org.projectnessie.versioned.persist.adapter.serialize.ProtoSerialization.protoToRepoDescription;
import static org.projectnessie.versioned.persist.adapter.serialize.ProtoSerialization.toProto;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.hashCollisionDetected;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.StoreWorker;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.KeyListEntity;
import org.projectnessie.versioned.persist.adapter.KeyListEntry;
import org.projectnessie.versioned.persist.adapter.RefLog;
import org.projectnessie.versioned.persist.adapter.RepoDescription;
import org.projectnessie.versioned.persist.adapter.serialize.ProtoSerialization;
import org.projectnessie.versioned.persist.nontx.NonTransactionalDatabaseAdapter;
import org.projectnessie.versioned.persist.nontx.NonTransactionalDatabaseAdapterConfig;
import org.projectnessie.versioned.persist.nontx.NonTransactionalOperationContext;
import org.projectnessie.versioned.persist.serialize.AdapterTypes;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.GlobalStateLogEntry;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.GlobalStatePointer;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.NamedReference;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.RefLogParents;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.RefPointer;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.ReferenceNames;
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
      StoreWorker<?, ?, ?> storeWorker) {
    super(config, storeWorker);

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

  private byte[] dbKey(String key) {
    return dbKey(ByteString.copyFromUtf8(key));
  }

  private byte[] dbKey(int key) {
    return dbKey(Integer.toString(key));
  }

  private byte[] globalPointerKey() {
    return globalPointerKey;
  }

  @Override
  public void eraseRepo() {
    try {
      db.delete(dbInstance.getCfGlobalPointer(), globalPointerKey());
      dbInstance
          .allExceptGlobalPointer()
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
      NonTransactionalOperationContext ctx, Set<Hash> branchCommits, Set<Hash> newKeyLists) {
    if (branchCommits.isEmpty() && newKeyLists.isEmpty()) {
      return;
    }
    Lock lock = dbInstance.getLock().writeLock();
    lock.lock();
    try {
      WriteBatch batch = new WriteBatch();
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
  protected void doCleanUpRefLogWrite(NonTransactionalOperationContext ctx, Hash refLogId) {}

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
  protected void unsafeWriteRefLogStripe(
      NonTransactionalOperationContext ctx, int stripe, RefLogParents refLogParents) {
    Lock lock = dbInstance.getLock().writeLock();
    lock.lock();
    try {
      db.put(dbInstance.getCfRefLogHeads(), dbKey(stripe), refLogParents.toByteArray());
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    } finally {
      lock.unlock();
    }
  }

  @Override
  protected boolean doRefLogParentsCas(
      NonTransactionalOperationContext ctx,
      int stripe,
      RefLogParents previousEntry,
      RefLogParents newEntry) {
    Lock lock = dbInstance.getLock().writeLock();
    lock.lock();
    try {
      byte[] bytes = db.get(dbInstance.getCfRefLogHeads(), dbKey(stripe));
      RefLogParents parents = bytes != null ? RefLogParents.parseFrom(bytes) : null;
      if (previousEntry != null) {
        if (!previousEntry.equals(parents)) {
          return false;
        }
      } else if (parents != null) {
        return false;
      }
      db.put(dbInstance.getCfRefLogHeads(), dbKey(stripe), newEntry.toByteArray());
      return true;
    } catch (InvalidProtocolBufferException | RocksDBException e) {
      throw new RuntimeException(e);
    } finally {
      lock.unlock();
    }
  }

  @Override
  protected RefLogParents doFetchRefLogParents(NonTransactionalOperationContext ctx, int stripe) {
    Lock lock = dbInstance.getLock().readLock();
    lock.lock();
    try {
      byte[] bytes = db.get(dbInstance.getCfRefLogHeads(), dbKey(stripe));
      if (bytes == null) {
        return null;
      }
      return RefLogParents.parseFrom(bytes);
    } catch (RocksDBException | InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    } finally {
      lock.unlock();
    }
  }

  @Override
  protected ReferenceNames doFetchReferenceNames(
      NonTransactionalOperationContext ctx, int segment) {
    try {
      byte[] s = db.get(dbInstance.getCfRefNames(), dbKey(segment));
      return s != null ? ReferenceNames.parseFrom(s) : null;
    } catch (RocksDBException | InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected NamedReference doFetchNamedReference(
      NonTransactionalOperationContext ctx, String refName) {
    Lock lock = dbInstance.getLock().readLock();
    lock.lock();
    try {
      byte[] bytes = db.get(dbInstance.getCfRefHeads(), dbKey(refName));
      if (bytes == null) {
        return null;
      }
      return NamedReference.parseFrom(bytes);
    } catch (RocksDBException | InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    } finally {
      lock.unlock();
    }
  }

  @Override
  protected boolean doCreateNamedReference(
      NonTransactionalOperationContext ctx, NamedReference namedReference) {
    Lock lock = dbInstance.getLock().writeLock();
    lock.lock();
    try {
      byte[] existing = db.get(dbInstance.getCfRefHeads(), dbKey(namedReference.getName()));
      if (existing != null) {
        return false;
      }

      db.put(
          dbInstance.getCfRefHeads(),
          dbKey(namedReference.getName()),
          namedReference.toByteArray());

      return true;
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    } finally {
      lock.unlock();
    }
  }

  @Override
  protected boolean doDeleteNamedReference(
      NonTransactionalOperationContext ctx, NamedRef ref, RefPointer refHead) {
    Lock lock = dbInstance.getLock().writeLock();
    lock.lock();
    try {
      byte[] existing = db.get(dbInstance.getCfRefHeads(), dbKey(ref.getName()));
      if (existing == null) {
        return false;
      }

      NamedReference expected =
          NamedReference.newBuilder().setName(ref.getName()).setRef(refHead).build();

      if (!Arrays.equals(existing, expected.toByteArray())) {
        return false;
      }

      db.delete(dbInstance.getCfRefHeads(), dbKey(ref.getName()));

      return true;

    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    } finally {
      lock.unlock();
    }
  }

  @Override
  protected void doAddToNamedReferences(
      NonTransactionalOperationContext ctx, Stream<NamedRef> refStream, int addToSegment) {
    Set<String> refNamesToAdd = refStream.map(NamedRef::getName).collect(Collectors.toSet());
    Lock lock = dbInstance.getLock().writeLock();
    lock.lock();
    try {
      byte[] refNamesBytes = db.get(dbInstance.getCfRefNames(), dbKey(addToSegment));

      ReferenceNames referenceNames;
      try {
        referenceNames =
            refNamesBytes == null
                ? ReferenceNames.getDefaultInstance()
                : ReferenceNames.parseFrom(refNamesBytes);
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }

      byte[] newRefNameBytes =
          referenceNames.toBuilder().addAllRefNames(refNamesToAdd).build().toByteArray();

      db.put(dbInstance.getCfRefNames(), dbKey(addToSegment), newRefNameBytes);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    } finally {
      lock.unlock();
    }
  }

  @Override
  protected void doRemoveFromNamedReferences(
      NonTransactionalOperationContext ctx, NamedRef ref, int removeFromSegment) {
    Lock lock = dbInstance.getLock().writeLock();
    lock.lock();
    try {
      byte[] refNamesBytes = db.get(dbInstance.getCfRefNames(), dbKey(removeFromSegment));
      if (refNamesBytes == null) {
        return;
      }

      ReferenceNames referenceNames;
      try {
        referenceNames = ReferenceNames.parseFrom(refNamesBytes);
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }

      ReferenceNames.Builder newRefNames = referenceNames.toBuilder();
      referenceNames.getRefNamesList().stream()
          .filter(n -> !n.equals(ref.getName()))
          .forEach(newRefNames::addRefNames);
      byte[] newRefNameBytes = newRefNames.build().toByteArray();

      db.put(dbInstance.getCfRefNames(), dbKey(removeFromSegment), newRefNameBytes);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    } finally {
      lock.unlock();
    }
  }

  @Override
  protected boolean doUpdateNamedReference(
      NonTransactionalOperationContext ctx, NamedRef ref, RefPointer refHead, Hash newHead) {
    Lock lock = dbInstance.getLock().writeLock();
    lock.lock();
    try {
      byte[] existing = db.get(dbInstance.getCfRefHeads(), dbKey(ref.getName()));
      if (existing == null) {
        return false;
      }

      NamedReference namedReference;
      try {
        namedReference = NamedReference.parseFrom(existing);
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }

      if (!namedReference.getRef().equals(refHead)) {
        return false;
      }

      NamedReference newNamedReference =
          namedReference.toBuilder()
              .setRef(namedReference.getRef().toBuilder().setHash(newHead.asBytes()))
              .build();

      db.put(dbInstance.getCfRefHeads(), dbKey(ref.getName()), newNamedReference.toByteArray());

      return true;
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
    Objects.requireNonNull(refLogId, "refLogId mut not be null");
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
