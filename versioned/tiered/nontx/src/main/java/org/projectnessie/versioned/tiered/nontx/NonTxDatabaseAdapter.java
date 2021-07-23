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
package org.projectnessie.versioned.tiered.nontx;

import com.google.common.hash.Hasher;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.ContentsAndState;
import org.projectnessie.versioned.Diff;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.TagName;
import org.projectnessie.versioned.VersionStoreException;
import org.projectnessie.versioned.WithHash;
import org.projectnessie.versioned.tiered.adapter.CommitLogEntry;
import org.projectnessie.versioned.tiered.adapter.DatabaseAdapterConfig;
import org.projectnessie.versioned.tiered.adapter.KeyWithBytes;
import org.projectnessie.versioned.tiered.adapter.KeyWithType;
import org.projectnessie.versioned.tiered.adapter.spi.AbstractDatabaseAdapter;
import org.projectnessie.versioned.tiered.adapter.spi.TryLoopState;

/**
 * Non-transactional database-adapter implementation suitable for no-sql databases.
 *
 * <p>Relies on three main entities:
 *
 * <ul>
 *   <li><em>Global state pointer</em> points to the current HEAD in the <em>global state log</em>
 *       and also contains all named-references and their current HEADs.
 *   <li><em>Global state log entry</em> is organized as a linked list and contains the new global
 *       states for all contents-keys and a (list of) its parents..
 *   <li><em>Commit log entry</em> is organized as a linked list and contains the changes to
 *       content-keys, the commit-metadata and a (list of) its parents.
 * </ul>
 */
public abstract class NonTxDatabaseAdapter<CONFIG extends DatabaseAdapterConfig>
    extends AbstractDatabaseAdapter<NonTxOperationContext, CONFIG> {

  protected NonTxDatabaseAdapter(CONFIG config) {
    super(config);
  }

  @Override
  public Stream<Optional<ContentsAndState<ByteString, ByteString>>> values(
      NamedRef ref, Optional<Hash> hashOnRef, List<Key> keys) throws ReferenceNotFoundException {

    NonTxOperationContext ctx = NonTxOperationContext.DUMMY;

    GlobalStatePointer pointer = fetchGlobalPointer(ctx);
    Hash hash = hashOnRef(ctx, ref, branchHead(pointer, ref), hashOnRef);

    Map<Key, ContentsAndState<ByteString, ByteString>> result = fetchValues(ctx, hash, keys);

    return keys.stream().map(result::get).map(Optional::ofNullable);
  }

  @Override
  public Stream<CommitLogEntry> commitLog(
      NamedRef ref, Optional<Hash> offset, Optional<Hash> untilIncluding)
      throws ReferenceNotFoundException {

    NonTxOperationContext ctx = NonTxOperationContext.DUMMY;

    GlobalStatePointer pointer = fetchGlobalPointer(ctx);
    Hash hash = hashOnRef(ctx, ref, branchHead(pointer, ref), offset);

    Stream<CommitLogEntry> intLog = commitLogFetcher(ctx, hash);
    if (untilIncluding.isPresent()) {
      intLog = takeUntil(intLog, e -> e.getHash().equals(untilIncluding.get()), true);
    }
    return intLog;
  }

  @Override
  public Hash toHash(NamedRef ref) throws ReferenceNotFoundException {
    NonTxOperationContext ctx = NonTxOperationContext.DUMMY;

    GlobalStatePointer pointer = fetchGlobalPointer(ctx);
    return branchHead(pointer, ref);
  }

  @Override
  public Stream<WithHash<NamedRef>> namedRefs() {
    NonTxOperationContext ctx = NonTxOperationContext.DUMMY;

    GlobalStatePointer pointer = fetchGlobalPointer(ctx);
    return pointer.getNamedReferences().stream().map(r -> WithHash.of(r.getHash(), r.getRef()));
  }

  @Override
  public Stream<KeyWithType> keys(NamedRef ref, Optional<Hash> hashOnRef)
      throws ReferenceNotFoundException {
    NonTxOperationContext ctx = NonTxOperationContext.DUMMY;

    GlobalStatePointer pointer = fetchGlobalPointer(ctx);
    Hash hash = hashOnRef(ctx, ref, branchHead(pointer, ref), hashOnRef);
    return keysForCommitEntry(ctx, hash);
  }

  @Override
  public Hash merge(
      NamedRef from,
      Optional<Hash> fromHash,
      BranchName toBranch,
      Optional<Hash> expectedHash,
      boolean commonAncestorRequired)
      throws ReferenceNotFoundException, ReferenceConflictException {
    // The spec for 'VersionStore.merge' mentions "(...) until we arrive at a common ancestor",
    // but old implementations allowed a merge even if the "merge-from" and "merge-to" have no
    // common ancestor and did merge "everything" from the "merge-from" into "merge-to".
    //
    // This implementation requires a common-ancestor, where "beginning-of-time" is not a valid
    // common-ancestor.
    //
    // Note: "beginning-of-time" (aka creating a branch without specifying a "create-from")
    // creates a new commit-tree that is decoupled from other commit-trees.
    try {
      return casOpLoop(
          toBranch,
          false,
          (ctx, pointer, individualCommits) -> {
            Hash toHead = branchHead(pointer, toBranch);
            Hash fromHEAD = branchHead(pointer, from);

            toHead =
                mergeAttempt(
                    ctx,
                    from,
                    fromHEAD,
                    fromHash,
                    toBranch,
                    toHead,
                    expectedHash,
                    individualCommits,
                    commonAncestorRequired);

            Hash newGlobalHead =
                writeGlobalCommit(ctx, pointer.getGlobalId(), Collections.emptyList());

            return pointer.update(newGlobalHead, toBranch, toHead);

            // 8. return hash of last commit added to 'targetBranch' (via the casOpLoop)
          },
          () ->
              mergeConflictMessage(
                  "Retry-failure", from, fromHash, toBranch, expectedHash, commonAncestorRequired));
    } catch (ReferenceNotFoundException | ReferenceConflictException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("RedundantThrows")
  @Override
  public Hash transplant(
      BranchName targetBranch,
      Optional<Hash> expectedHash,
      NamedRef source,
      List<Hash> sequenceToTransplant)
      throws ReferenceNotFoundException, ReferenceConflictException {
    try {
      if (sequenceToTransplant.isEmpty()) {
        throw new IllegalArgumentException("No hashes to transplant given.");
      }

      return casOpLoop(
          targetBranch,
          false,
          (ctx, pointer, individualCommits) -> {
            Hash targetHead = branchHead(pointer, targetBranch);
            Hash sourceHead = branchHead(pointer, source);

            targetHead =
                transplantAttempt(
                    ctx,
                    targetBranch,
                    targetHead,
                    expectedHash,
                    source,
                    sourceHead,
                    sequenceToTransplant,
                    individualCommits);

            Hash newGlobalHead =
                writeGlobalCommit(ctx, pointer.getGlobalId(), Collections.emptyList());

            return pointer.update(newGlobalHead, targetBranch, targetHead);

            // 6. return hash of last commit added to 'targetBranch' (via the casOpLoop)
          },
          () ->
              transplantConflictMessage(
                  "Retry-failure", targetBranch, expectedHash, source, sequenceToTransplant));

    } catch (ReferenceNotFoundException | ReferenceConflictException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Hash commit(
      BranchName branch,
      Optional<Hash> expectedHead,
      Map<Key, ByteString> expectedStates,
      List<KeyWithBytes> puts,
      Map<Key, ByteString> global,
      List<Key> unchanged,
      List<Key> deletes,
      Set<Key> operationsKeys,
      ByteString commitMetaSerialized)
      throws ReferenceConflictException, ReferenceNotFoundException {
    try {
      return casOpLoop(
          branch,
          false,
          (ctx, pointer, x) -> {
            Hash branchHead = branchHead(pointer, branch);

            CommitLogEntry newBranchCommit =
                commitAttempt(
                    ctx,
                    branch,
                    branchHead,
                    expectedHead,
                    expectedStates,
                    puts,
                    unchanged,
                    deletes,
                    operationsKeys,
                    commitMetaSerialized);

            Hash newGlobalHead =
                writeGlobalCommit(
                    ctx,
                    pointer.getGlobalId(),
                    global.entrySet().stream()
                        .map(e -> KeyWithBytes.of(e.getKey(), (byte) 0, e.getValue()))
                        .collect(Collectors.toList()));

            return pointer.update(newGlobalHead, branch, newBranchCommit.getHash());
          },
          () -> commitConflictMessage("Retry-Failure", branch, expectedHead, operationsKeys));
    } catch (ReferenceNotFoundException | ReferenceConflictException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Hash create(NamedRef ref, Optional<NamedRef> target, Optional<Hash> targetHash)
      throws ReferenceNotFoundException, ReferenceAlreadyExistsException {
    try {
      if (ref instanceof TagName && (!target.isPresent() || !targetHash.isPresent())) {
        throw new IllegalArgumentException(
            "Tag-creation requires a target named-reference and hash.");
      }

      NamedRef realTarget = target.orElseGet(() -> BranchName.of(config.getDefaultBranch()));
      return casOpLoop(
          ref,
          false,
          (ctx, pointer, x) -> {
            if (pointer.hashForReference(ref) != null) {
              throw referenceAlreadyExists(ref);
            }

            Optional<Hash> beginning =
                targetHash.isPresent() ? targetHash : Optional.of(NO_ANCESTOR);
            Hash hash;
            if (beginning.get().equals(NO_ANCESTOR)
                && !target.isPresent()
                && ref.equals(BranchName.of(config.getDefaultBranch()))) {
              // Handle the special case when the default-branch does not exist and the current
              // request creates it. This mostly happens during tests.
              hash = NO_ANCESTOR;
            } else {
              hash = hashOnRef(ctx, realTarget, branchHead(pointer, realTarget), beginning);
            }

            // Need a new empty global-log entry to be able to CAS
            Hash newGlobalHead = noopGlobalLogEntry(ctx, pointer);

            return pointer.update(newGlobalHead, ref, hash);
          },
          () -> createConflictMessage("Retry-Failure", ref, realTarget, targetHash));
    } catch (ReferenceNotFoundException | ReferenceAlreadyExistsException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void delete(NamedRef ref, Optional<Hash> hash)
      throws ReferenceNotFoundException, ReferenceConflictException {
    try {
      casOpLoop(
          ref,
          true,
          (ctx, pointer, x) -> {
            verifyExpectedHash(branchHead(pointer, ref), ref, hash);
            Hash newGlobalHead = noopGlobalLogEntry(ctx, pointer);

            return pointer.update(newGlobalHead, ref, null);
          },
          () -> deleteConflictMessage("Retry-Failure", ref, hash));
    } catch (ReferenceNotFoundException | ReferenceConflictException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void assign(
      NamedRef ref, Optional<Hash> expectedHash, NamedRef assignTo, Optional<Hash> assignToHash)
      throws ReferenceNotFoundException, ReferenceConflictException {
    try {
      casOpLoop(
          ref,
          true,
          (ctx, pointer, x) -> {
            verifyExpectedHash(branchHead(pointer, ref), ref, expectedHash);
            Hash assignToHead =
                hashOnRef(ctx, assignTo, branchHead(pointer, assignTo), assignToHash);

            Hash newGlobalHead = noopGlobalLogEntry(ctx, pointer);

            return pointer.update(newGlobalHead, ref, assignToHead);
          },
          () -> assignConflictMessage("Retry-Failure", ref, expectedHash, assignTo, assignToHash));
    } catch (ReferenceNotFoundException | ReferenceConflictException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Stream<Diff<ByteString>> diff(
      NamedRef from, Optional<Hash> hashOnFrom, NamedRef to, Optional<Hash> hashOnTo)
      throws ReferenceNotFoundException {
    NonTxOperationContext ctx = NonTxOperationContext.DUMMY;

    GlobalStatePointer pointer = fetchGlobalPointer(ctx);

    Hash fromHead = hashOnRef(ctx, from, branchHead(pointer, from), hashOnFrom);
    Hash toHead = hashOnRef(ctx, to, branchHead(pointer, to), hashOnTo);

    return buildDiff(ctx, from, fromHead, hashOnFrom, to, toHead, hashOnTo);
  }

  @Override
  public void initializeRepo() throws ReferenceConflictException {
    NonTxOperationContext ctx = NonTxOperationContext.DUMMY;
    if (fetchGlobalPointer(ctx) == null) {
      Hash globalHead = writeGlobalCommit(ctx, NO_ANCESTOR, Collections.emptyList());

      unsafeWriteGlobalPointer(
          ctx,
          GlobalStatePointer.of(
              globalHead,
              Collections.singletonList(
                  Ref.of(NO_ANCESTOR, BranchName.of(config.getDefaultBranch())))));
    }
  }

  // /////////////////////////////////////////////////////////////////////////////////////////////
  // NMon-Transactional DatabaseAdapter subclass API (protected)
  // /////////////////////////////////////////////////////////////////////////////////////////////

  @FunctionalInterface
  public interface CasOp {
    /**
     * Applies an operation within a CAS-loop. The implementation gets the current global-state and
     * must return an updated global-state with a different global-id.
     */
    GlobalStatePointer apply(
        NonTxOperationContext ctx, GlobalStatePointer pointer, Consumer<Hash> individualCommits)
        throws VersionStoreException;
  }

  /** This is the actual CAS-loop, which applies an operation onto a named-ref. */
  protected Hash casOpLoop(
      NamedRef ref, boolean deleteRef, CasOp casOp, Supplier<String> retryErrorMessage)
      throws VersionStoreException {
    NonTxOperationContext ctx = NonTxOperationContext.DUMMY;

    try (TryLoopState tryState = newTryLoopState(retryErrorMessage)) {
      while (true) {
        GlobalStatePointer pointer = fetchGlobalPointer(ctx);
        Set<Hash> individualCommits = new HashSet<>();

        GlobalStatePointer newPointer = casOp.apply(ctx, pointer, individualCommits::add);
        if (newPointer.getGlobalId().equals(pointer.getGlobalId())) {
          return tryState.success(branchHead(pointer, ref));
        }
        Hash branchHead = deleteRef ? null : branchHead(newPointer, ref);

        if (pointer.getGlobalId().equals(newPointer.getGlobalId())) {
          throw hashCollisionDetected();
        }

        if (globalPointerCas(ctx, pointer, newPointer)) {
          return tryState.success(branchHead);
        } else {
          if (branchHead != null) {
            individualCommits.add(branchHead);
          }
          cleanUpCommitCas(ctx, newPointer.getGlobalId(), individualCommits);
        }

        tryState.retry();
      }
    }
  }

  /**
   * Write a new global-state-log-entry with a best-effort approach to prevent hash-collisions but
   * without any other consistency checks/guarantees. Some implementations however can enforce
   * strict consistency checks/guarantees.
   */
  protected abstract void writeGlobalCommit(NonTxOperationContext ctx, GlobalStateLogEntry entry)
      throws ReferenceConflictException;

  /**
   * Unsafe operation to initialize a repository: unconditionally writes the global-state-pointer.
   */
  protected abstract void unsafeWriteGlobalPointer(
      NonTxOperationContext ctx, GlobalStatePointer pointer);

  @SuppressWarnings("UnstableApiUsage")
  protected Hash writeGlobalCommit(
      NonTxOperationContext ctx, Hash parentHash, List<KeyWithBytes> globals)
      throws ReferenceConflictException {
    Hasher hasher = newHasher();
    hasher
        .putLong(GLOBAL_LOG_HASH_SEED)
        // add some randomness here to "avoid" hash-collisions for the global-state-log
        .putLong(ThreadLocalRandom.current().nextLong())
        .putBytes(parentHash.asBytes().asReadOnlyByteBuffer());
    globals.forEach(
        g -> {
          hashKey(hasher, g.getKey());
          hasher.putBytes(g.getValue().asReadOnlyByteBuffer());
        });
    Hash hash = Hash.of(UnsafeByteOperations.unsafeWrap(hasher.hash().asBytes()));

    GlobalStateLogEntry currentEntry = fetchFromGlobalLog(ctx, parentHash);

    List<Hash> newParents =
        currentEntry != null
            ? Stream.concat(
                    Stream.of(parentHash),
                    currentEntry.getParents().stream()
                        .skip(1)
                        .limit(config.getParentsPerCommit() - 1))
                .collect(Collectors.toList())
            : Collections.singletonList(parentHash);

    GlobalStateLogEntry entry =
        GlobalStateLogEntry.of(currentTimeInMicros(), hash, newParents, globals);
    writeGlobalCommit(ctx, entry);

    return hash;
  }

  /**
   * Atomically update the global-commit-pointer to the given new-global-head, if the value in the
   * database is the given expected-global-head.
   */
  protected abstract boolean globalPointerCas(
      NonTxOperationContext ctx, GlobalStatePointer expected, GlobalStatePointer newPointer);

  /**
   * If a {@link #globalPointerCas(NonTxOperationContext, GlobalStatePointer, GlobalStatePointer)}
   * failed, {@link AbstractDatabaseAdapter#commit(BranchName, Optional, Map, List, Map, List, List,
   * Set, ByteString)} calls this function to remove the optimistically written data.
   *
   * <p>Implementation notes: non-transactional implementations <em>must</em> delete entries for the
   * given keys, no-op for transactional implementations.
   */
  protected abstract void cleanUpCommitCas(
      NonTxOperationContext ctx, Hash globalId, Set<Hash> branchCommit);

  /**
   * Writes a global-state-log-entry without any operations, just to move the global-pointer
   * forwards for a "proper" CAS operation.
   */
  // TODO maybe replace with a 2nd-ary value in global-state-pointer to prevent the empty
  //  global-log-entry
  protected Hash noopGlobalLogEntry(NonTxOperationContext ctx, GlobalStatePointer pointer)
      throws ReferenceConflictException {
    // Need a new empty global-log entry to be able to CAS
    return writeGlobalCommit(ctx, pointer.getGlobalId(), Collections.emptyList());
  }

  protected static Hash branchHead(GlobalStatePointer pointer, NamedRef ref)
      throws ReferenceNotFoundException {
    Hash branchHead = pointer.hashForReference(ref);
    if (branchHead == null) {
      throw refNotFound(ref);
    }
    return branchHead;
  }

  /** Load the current global-state-pointer. */
  protected abstract GlobalStatePointer fetchGlobalPointer(NonTxOperationContext ctx);

  /** Fetches the global-state information for the given keys. */
  @Override
  protected Map<Key, ByteString> fetchGlobalStates(
      NonTxOperationContext ctx, Collection<Key> keys) {
    if (keys.isEmpty()) {
      return Collections.emptyMap();
    }

    Set<Key> remainingKeys = new HashSet<>(keys);
    Map<Key, ByteString> result = new HashMap<>();
    Hash globalHead = fetchGlobalPointer(ctx).getGlobalId();

    Stream<GlobalStateLogEntry> log = globalLogFetcher(ctx, globalHead);
    log = takeUntil(log, x -> remainingKeys.isEmpty());
    log.forEach(
        entry -> {
          for (KeyWithBytes put : entry.getPuts()) {
            if (remainingKeys.remove(put.getKey())) {
              result.put(put.getKey(), put.getValue());
            }
          }
        });
    return result;
  }

  /** Reads from the global-state-log starting at the given global-state-log-ID. */
  private Stream<GlobalStateLogEntry> globalLogFetcher(NonTxOperationContext ctx, Hash initialId) {
    return logFetcher(
        ctx, initialId, this::fetchPageFromGlobalLog, GlobalStateLogEntry::getParents);
  }

  /** Load the global-log entry with the given id. */
  protected abstract GlobalStateLogEntry fetchFromGlobalLog(NonTxOperationContext ctx, Hash id);

  protected abstract List<GlobalStateLogEntry> fetchPageFromGlobalLog(
      NonTxOperationContext ctx, List<Hash> hashes);
}
