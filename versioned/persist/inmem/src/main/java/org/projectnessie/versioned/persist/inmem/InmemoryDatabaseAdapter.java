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
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
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

public class InmemoryDatabaseAdapter
    extends NonTransactionalDatabaseAdapter<NonTransactionalDatabaseAdapterConfig> {

  private final InmemoryStore store;
  private final ByteString keyPrefix;

  public InmemoryDatabaseAdapter(
      NonTransactionalDatabaseAdapterConfig config, InmemoryStore store) {
    super(config);

    this.keyPrefix = ByteString.copyFromUtf8(config.getKeyPrefix() + ':');

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
  public void reinitializeRepo(String defaultBranchName) {
    store.reinitializeRepo(keyPrefix);
    super.initializeRepo(defaultBranchName);
  }

  @Override
  protected GlobalStatePointer fetchGlobalPointer(NonTransactionalOperationContext ctx) {
    return globalState().get();
  }

  @Override
  protected void writeIndividualCommit(NonTransactionalOperationContext ctx, CommitLogEntry entry)
      throws ReferenceConflictException {
    if (store.commitLog.putIfAbsent(dbKey(entry.getHash()), toProto(entry).toByteString())
        != null) {
      throw hashCollisionDetected();
    }
  }

  @Override
  protected void writeMultipleCommits(
      NonTransactionalOperationContext ctx, List<CommitLogEntry> entries)
      throws ReferenceConflictException {
    for (CommitLogEntry entry : entries) {
      writeIndividualCommit(ctx, entry);
    }
  }

  @Override
  protected void writeGlobalCommit(NonTransactionalOperationContext ctx, GlobalStateLogEntry entry)
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
  protected boolean globalPointerCas(
      NonTransactionalOperationContext ctx,
      GlobalStatePointer expected,
      GlobalStatePointer newPointer) {
    return globalState().compareAndSet(expected, newPointer);
  }

  private AtomicReference<GlobalStatePointer> globalState() {
    return store.globalStatePointer.computeIfAbsent(keyPrefix, k -> new AtomicReference<>());
  }

  @Override
  protected void cleanUpCommitCas(
      NonTransactionalOperationContext ctx,
      Hash globalId,
      Set<Hash> branchCommits,
      Set<Hash> newKeyLists) {
    store.globalStateLog.remove(dbKey(globalId));
    branchCommits.forEach(h -> store.commitLog.remove(dbKey(h)));
    newKeyLists.forEach(h -> store.keyLists.remove(dbKey(h)));
  }

  @Override
  protected GlobalStateLogEntry fetchFromGlobalLog(NonTransactionalOperationContext ctx, Hash id) {
    ByteString serialized = store.globalStateLog.get(dbKey(id));
    try {
      return serialized != null ? GlobalStateLogEntry.parseFrom(serialized) : null;
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected List<GlobalStateLogEntry> fetchPageFromGlobalLog(
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
  protected CommitLogEntry fetchFromCommitLog(NonTransactionalOperationContext ctx, Hash hash) {
    return protoToCommitLogEntry(store.commitLog.get(dbKey(hash)));
  }

  @Override
  protected List<CommitLogEntry> fetchPageFromCommitLog(
      NonTransactionalOperationContext ctx, List<Hash> hashes) {
    return hashes.stream()
        .map(this::dbKey)
        .map(store.commitLog::get)
        .map(ProtoSerialization::protoToCommitLogEntry)
        .collect(Collectors.toList());
  }

  @Override
  protected void writeKeyListEntities(
      NonTransactionalOperationContext ctx, List<KeyListEntity> newKeyListEntities) {
    newKeyListEntities.forEach(
        e -> store.keyLists.put(dbKey(e.getId()), toProto(e.getKeys()).toByteString()));
  }

  @Override
  protected Stream<KeyListEntity> fetchKeyLists(
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
  protected int entitySize(CommitLogEntry entry) {
    return toProto(entry).getSerializedSize();
  }

  @Override
  protected int entitySize(KeyWithType entry) {
    return toProto(entry).getSerializedSize();
  }
}
