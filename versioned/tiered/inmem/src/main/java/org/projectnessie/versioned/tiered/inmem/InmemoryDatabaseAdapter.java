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
package org.projectnessie.versioned.tiered.inmem;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.tiered.adapter.CommitLogEntry;
import org.projectnessie.versioned.tiered.adapter.DatabaseAdapterConfig;
import org.projectnessie.versioned.tiered.nontx.GlobalStateLogEntry;
import org.projectnessie.versioned.tiered.nontx.GlobalStatePointer;
import org.projectnessie.versioned.tiered.nontx.NonTxDatabaseAdapter;
import org.projectnessie.versioned.tiered.nontx.NonTxOperationContext;

public class InmemoryDatabaseAdapter extends NonTxDatabaseAdapter<DatabaseAdapterConfig> {

  private final AtomicReference<GlobalStatePointer> globalStatePointer = new AtomicReference<>();

  private final ConcurrentMap<Hash, GlobalStateLogEntry> globalStateLog = new ConcurrentHashMap<>();
  private final ConcurrentMap<Hash, CommitLogEntry> commitLog = new ConcurrentHashMap<>();

  public InmemoryDatabaseAdapter(DatabaseAdapterConfig config) {
    super(config);
  }

  @Override
  public void reinitializeRepo() throws ReferenceConflictException {
    globalStateLog.clear();
    commitLog.clear();
    globalStatePointer.set(null);
    super.initializeRepo();
  }

  @Override
  protected GlobalStatePointer fetchGlobalPointer(NonTxOperationContext ctx) {
    return globalStatePointer.get();
  }

  @Override
  protected void writeIndividualCommit(NonTxOperationContext ctx, CommitLogEntry entry)
      throws ReferenceConflictException {
    if (commitLog.putIfAbsent(entry.getHash(), entry) != null) {
      throw hashCollisionDetected();
    }
  }

  @Override
  protected void writeIndividualCommits(NonTxOperationContext ctx, List<CommitLogEntry> entries)
      throws ReferenceConflictException {
    for (CommitLogEntry entry : entries) {
      writeIndividualCommit(ctx, entry);
    }
  }

  @Override
  protected void writeGlobalCommit(NonTxOperationContext ctx, GlobalStateLogEntry entry)
      throws ReferenceConflictException {
    if (globalStateLog.putIfAbsent(entry.getId(), entry) != null) {
      throw hashCollisionDetected();
    }
  }

  @Override
  protected void unsafeWriteGlobalPointer(NonTxOperationContext ctx, GlobalStatePointer pointer) {
    globalStatePointer.set(pointer);
  }

  @Override
  protected boolean globalPointerCas(
      NonTxOperationContext ctx, GlobalStatePointer expected, GlobalStatePointer newPointer) {
    return globalStatePointer.compareAndSet(expected, newPointer);
  }

  @Override
  protected void cleanUpCommitCas(
      NonTxOperationContext ctx, Hash globalId, Set<Hash> branchCommits) {
    globalStateLog.remove(globalId);
    branchCommits.forEach(commitLog::remove);
  }

  @Override
  protected GlobalStateLogEntry fetchFromGlobalLog(NonTxOperationContext ctx, Hash id) {
    return globalStateLog.get(id);
  }

  @Override
  protected List<GlobalStateLogEntry> fetchPageFromGlobalLog(
      NonTxOperationContext ctx, List<Hash> hashes) {
    return hashes.stream().map(globalStateLog::get).collect(Collectors.toList());
  }

  @Override
  protected CommitLogEntry fetchFromCommitLog(NonTxOperationContext ctx, Hash hash) {
    return commitLog.get(hash);
  }

  @Override
  protected List<CommitLogEntry> fetchPageFromCommitLog(
      NonTxOperationContext ctx, List<Hash> hashes) {
    return hashes.stream().map(commitLog::get).collect(Collectors.toList());
  }

  @Override
  public void close() {}
}
