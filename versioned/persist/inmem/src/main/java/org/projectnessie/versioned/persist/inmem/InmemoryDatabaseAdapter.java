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

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapterConfig;
import org.projectnessie.versioned.persist.adapter.KeyListEntity;
import org.projectnessie.versioned.persist.adapter.KeyWithBytes;
import org.projectnessie.versioned.persist.adapter.KeyWithType;
import org.projectnessie.versioned.persist.nontx.NonTransactionalDatabaseAdapter;
import org.projectnessie.versioned.persist.nontx.NonTransactionalOperationContext;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.GlobalStateLogEntry;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.GlobalStatePointer;

public class InmemoryDatabaseAdapter
    extends NonTransactionalDatabaseAdapter<DatabaseAdapterConfig> {

  private final AtomicReference<GlobalStatePointer> globalStatePointer = new AtomicReference<>();

  private final ConcurrentMap<Hash, GlobalStateLogEntry> globalStateLog = new ConcurrentHashMap<>();
  private final ConcurrentMap<Hash, CommitLogEntry> commitLog = new ConcurrentHashMap<>();
  private final ConcurrentMap<Hash, KeyListEntity> keyLists = new ConcurrentHashMap<>();

  public InmemoryDatabaseAdapter(DatabaseAdapterConfig config) {
    super(config);
  }

  @Override
  public void reinitializeRepo(String defaultBranchName) {
    globalStateLog.clear();
    commitLog.clear();
    keyLists.clear();
    globalStatePointer.set(null);
    super.initializeRepo(defaultBranchName);
  }

  @Override
  protected GlobalStatePointer fetchGlobalPointer(NonTransactionalOperationContext ctx) {
    return globalStatePointer.get();
  }

  @Override
  protected void writeIndividualCommit(NonTransactionalOperationContext ctx, CommitLogEntry entry)
      throws ReferenceConflictException {
    if (commitLog.putIfAbsent(entry.getHash(), entry) != null) {
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
    if (globalStateLog.putIfAbsent(Hash.of(entry.getId()), entry) != null) {
      throw hashCollisionDetected();
    }
  }

  @Override
  protected void unsafeWriteGlobalPointer(
      NonTransactionalOperationContext ctx, GlobalStatePointer pointer) {
    globalStatePointer.set(pointer);
  }

  @Override
  protected boolean globalPointerCas(
      NonTransactionalOperationContext ctx,
      GlobalStatePointer expected,
      GlobalStatePointer newPointer) {
    return globalStatePointer.compareAndSet(expected, newPointer);
  }

  @Override
  protected void cleanUpCommitCas(
      NonTransactionalOperationContext ctx,
      Hash globalId,
      Set<Hash> branchCommits,
      Set<Hash> newKeyLists) {
    globalStateLog.remove(globalId);
    branchCommits.forEach(commitLog::remove);
    newKeyLists.forEach(this.keyLists::remove);
  }

  @Override
  protected GlobalStateLogEntry fetchFromGlobalLog(NonTransactionalOperationContext ctx, Hash id) {
    return globalStateLog.get(id);
  }

  @Override
  protected List<GlobalStateLogEntry> fetchPageFromGlobalLog(
      NonTransactionalOperationContext ctx, List<Hash> hashes) {
    return hashes.stream().map(globalStateLog::get).collect(Collectors.toList());
  }

  @Override
  protected CommitLogEntry fetchFromCommitLog(NonTransactionalOperationContext ctx, Hash hash) {
    return commitLog.get(hash);
  }

  @Override
  protected List<CommitLogEntry> fetchPageFromCommitLog(
      NonTransactionalOperationContext ctx, List<Hash> hashes) {
    return hashes.stream().map(commitLog::get).collect(Collectors.toList());
  }

  @Override
  protected void writeKeyListEntities(
      NonTransactionalOperationContext ctx, List<KeyListEntity> newKeyListEntities) {
    newKeyListEntities.forEach(e -> keyLists.put(e.getId(), e));
  }

  @Override
  protected Stream<KeyListEntity> fetchKeyLists(
      NonTransactionalOperationContext ctx, List<Hash> keyListsIds) {
    return keyListsIds.stream().map(keyLists::get);
  }

  @Override
  protected int entitySize(CommitLogEntry entry) {
    int cnt = 0;
    cnt += entry.getHash().asString().length();

    for (KeyWithBytes put : entry.getPuts()) {
      cnt += put.getContentsId().getId().length();
      cnt += put.getKey().toString().length();
      cnt += put.getValue().size();
    }

    for (Key delete : entry.getDeletes()) {
      cnt += delete.toString().length();
    }

    cnt += entry.getMetadata().size();

    for (Hash keyListsId : entry.getKeyListsIds()) {
      cnt += keyListsId.toString().length();
    }

    return cnt;
  }

  @Override
  protected int entitySize(KeyWithType entry) {
    return entry.getKey().toString().length() + entry.getContentsId().getId().length();
  }

  @Override
  public void close() {}
}
