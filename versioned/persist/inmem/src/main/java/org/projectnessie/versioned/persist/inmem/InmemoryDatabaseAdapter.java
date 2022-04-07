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
package org.projectnessie.versioned.persist.inmem;

import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.hashCollisionDetected;
import static org.projectnessie.versioned.persist.serialize.ProtoSerialization.protoToCommitLogEntry;
import static org.projectnessie.versioned.persist.serialize.ProtoSerialization.protoToKeyList;
import static org.projectnessie.versioned.persist.serialize.ProtoSerialization.toProto;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.StoreWorker;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.KeyListEntity;
import org.projectnessie.versioned.persist.adapter.KeyListEntry;
import org.projectnessie.versioned.persist.adapter.RefLog;
import org.projectnessie.versioned.persist.adapter.RepoDescription;
import org.projectnessie.versioned.persist.nontx.NonTransactionalDatabaseAdapter;
import org.projectnessie.versioned.persist.nontx.NonTransactionalDatabaseAdapterConfig;
import org.projectnessie.versioned.persist.nontx.NonTransactionalOperationContext;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.GlobalStateLogEntry;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.GlobalStatePointer;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.RefLogEntry;
import org.projectnessie.versioned.persist.serialize.ProtoSerialization;

public class InmemoryDatabaseAdapter
    extends NonTransactionalDatabaseAdapter<NonTransactionalDatabaseAdapterConfig> {

  private final InmemoryStore store;
  private final ByteString keyPrefix;

  public InmemoryDatabaseAdapter(
      NonTransactionalDatabaseAdapterConfig config,
      InmemoryStore store,
      StoreWorker<?, ?, ?> storeWorker) {
    super(config, storeWorker);

    this.keyPrefix = ByteString.copyFromUtf8(config.getRepositoryId() + ':');

    Objects.requireNonNull(
        store, "Requires a non-null InmemoryStore from InmemoryDatabaseAdapterConfig");

    this.store = store;
  }

  private ByteString dbKey(Hash hash) {
    return keyPrefix.concat(hash.asBytes());
  }

  private ByteString dbKey(ByteString key) {
    return keyPrefix.concat(key);
  }

  @Override
  public void eraseRepo() {
    store.reinitializeRepo(keyPrefix);
  }

  @Override
  protected GlobalStatePointer doFetchGlobalPointer(NonTransactionalOperationContext ctx) {
    return globalState().get();
  }

  @Override
  protected void doWriteIndividualCommit(NonTransactionalOperationContext ctx, CommitLogEntry entry)
      throws ReferenceConflictException {
    if (store.commitLog.putIfAbsent(dbKey(entry.getHash()), toProto(entry).toByteString())
        != null) {
      throw hashCollisionDetected();
    }
  }

  @Override
  protected void doWriteMultipleCommits(
      NonTransactionalOperationContext ctx, List<CommitLogEntry> entries)
      throws ReferenceConflictException {
    for (CommitLogEntry entry : entries) {
      doWriteIndividualCommit(ctx, entry);
    }
  }

  @Override
  protected void doWriteGlobalCommit(
      NonTransactionalOperationContext ctx, GlobalStateLogEntry entry)
      throws ReferenceConflictException {
    if (store.globalStateLog.putIfAbsent(dbKey(entry.getId()), entry.toByteString()) != null) {
      throw hashCollisionDetected();
    }
  }

  @Override
  protected void unsafeWriteGlobalPointer(
      NonTransactionalOperationContext ctx, GlobalStatePointer pointer) {
    globalState().set(pointer);
  }

  @Override
  protected boolean doGlobalPointerCas(
      NonTransactionalOperationContext ctx,
      GlobalStatePointer expected,
      GlobalStatePointer newPointer) {
    return globalState().compareAndSet(expected, newPointer);
  }

  private AtomicReference<GlobalStatePointer> globalState() {
    return store.globalStatePointer.computeIfAbsent(keyPrefix, k -> new AtomicReference<>());
  }

  @Override
  protected void doCleanUpCommitCas(
      NonTransactionalOperationContext ctx,
      Hash globalId,
      Set<Hash> branchCommits,
      Set<Hash> newKeyLists,
      Hash refLogId) {
    store.globalStateLog.remove(dbKey(globalId));
    branchCommits.forEach(h -> store.commitLog.remove(dbKey(h)));
    newKeyLists.forEach(h -> store.keyLists.remove(dbKey(h)));
    store.refLog.remove(dbKey(refLogId));
  }

  @Override
  protected void doCleanUpGlobalLog(
      NonTransactionalOperationContext ctx, Collection<Hash> globalIds) {
    globalIds.forEach(h -> store.globalStateLog.remove(dbKey(h)));
  }

  @Override
  protected GlobalStateLogEntry doFetchFromGlobalLog(
      NonTransactionalOperationContext ctx, Hash id) {
    ByteString serialized = store.globalStateLog.get(dbKey(id));
    try {
      return serialized != null ? GlobalStateLogEntry.parseFrom(serialized) : null;
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected List<GlobalStateLogEntry> doFetchPageFromGlobalLog(
      NonTransactionalOperationContext ctx, List<Hash> hashes) {
    return hashes.stream()
        .map(this::dbKey)
        .map(store.globalStateLog::get)
        .map(
            serialized -> {
              try {
                return serialized != null ? GlobalStateLogEntry.parseFrom(serialized) : null;
              } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
              }
            })
        .collect(Collectors.toList());
  }

  @Override
  protected CommitLogEntry doFetchFromCommitLog(NonTransactionalOperationContext ctx, Hash hash) {
    return protoToCommitLogEntry(store.commitLog.get(dbKey(hash)));
  }

  @Override
  protected List<CommitLogEntry> doFetchMultipleFromCommitLog(
      NonTransactionalOperationContext ctx, List<Hash> hashes) {
    return hashes.stream()
        .map(this::dbKey)
        .map(store.commitLog::get)
        .map(ProtoSerialization::protoToCommitLogEntry)
        .collect(Collectors.toList());
  }

  @Override
  protected void doWriteKeyListEntities(
      NonTransactionalOperationContext ctx, List<KeyListEntity> newKeyListEntities) {
    newKeyListEntities.forEach(
        e -> store.keyLists.put(dbKey(e.getId()), toProto(e.getKeys()).toByteString()));
  }

  @Override
  protected Stream<KeyListEntity> doFetchKeyLists(
      NonTransactionalOperationContext ctx, List<Hash> keyListsIds) {
    return keyListsIds.stream()
        .map(
            hash -> {
              ByteString serialized = store.keyLists.get(dbKey(hash));
              return serialized != null ? KeyListEntity.of(hash, protoToKeyList(serialized)) : null;
            })
        .filter(Objects::nonNull);
  }

  @Override
  protected RepoDescription doFetchRepositoryDescription(NonTransactionalOperationContext ctx) {
    AtomicReference<RepoDescription> ref = store.repoDesc.get(dbKey(ByteString.EMPTY));
    return ref != null ? ref.get() : null;
  }

  @Override
  protected boolean doTryUpdateRepositoryDescription(
      NonTransactionalOperationContext ctx, RepoDescription expected, RepoDescription updateTo) {
    if (expected == null) {
      return store.repoDesc.putIfAbsent(dbKey(ByteString.EMPTY), new AtomicReference<>(updateTo))
          == null;
    }
    return store.repoDesc.get(dbKey(ByteString.EMPTY)).compareAndSet(expected, updateTo);
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
  protected void doWriteRefLog(NonTransactionalOperationContext ctx, RefLogEntry entry)
      throws ReferenceConflictException {
    if (store.refLog.putIfAbsent(dbKey(entry.getRefLogId()), entry.toByteString()) != null) {
      throw new ReferenceConflictException(" RefLog Hash collision detected");
    }
  }

  @Override
  protected RefLog doFetchFromRefLog(NonTransactionalOperationContext ctx, Hash refLogId) {
    if (refLogId == null) {
      // set the current head as refLogId
      refLogId = Hash.of(fetchGlobalPointer(ctx).getRefLogId());
    }
    return ProtoSerialization.protoToRefLog(store.refLog.get(dbKey(refLogId)));
  }

  @Override
  protected List<RefLog> doFetchPageFromRefLog(
      NonTransactionalOperationContext ctx, List<Hash> hashes) {
    return hashes.stream()
        .map(this::dbKey)
        .map(store.refLog::get)
        .map(ProtoSerialization::protoToRefLog)
        .collect(Collectors.toList());
  }
}
