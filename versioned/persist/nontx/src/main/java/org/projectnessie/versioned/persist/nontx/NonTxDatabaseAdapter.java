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
package org.projectnessie.versioned.persist.nontx;

import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.assignConflictMessage;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.commitConflictMessage;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.createConflictMessage;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.deleteConflictMessage;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.hashCollisionDetected;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.mergeConflictMessage;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.newHasher;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.randomHash;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.referenceAlreadyExists;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.referenceNotFound;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.takeUntilExcludeLast;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.takeUntilIncludeLast;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.transplantConflictMessage;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.verifyExpectedHash;
import static org.projectnessie.versioned.persist.adapter.spi.TryLoopState.newTryLoopState;

import com.google.common.hash.Hasher;
import com.google.protobuf.ByteString;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.projectnessie.versioned.BranchName;
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
import org.projectnessie.versioned.persist.adapter.CommitAttempt;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.ContentsAndState;
import org.projectnessie.versioned.persist.adapter.ContentsId;
import org.projectnessie.versioned.persist.adapter.ContentsIdAndBytes;
import org.projectnessie.versioned.persist.adapter.ContentsIdWithType;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapterConfig;
import org.projectnessie.versioned.persist.adapter.KeyFilterPredicate;
import org.projectnessie.versioned.persist.adapter.KeyListEntity;
import org.projectnessie.versioned.persist.adapter.KeyWithBytes;
import org.projectnessie.versioned.persist.adapter.KeyWithType;
import org.projectnessie.versioned.persist.adapter.spi.AbstractDatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.spi.TryLoopState;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.ContentsIdWithBytes;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.GlobalStateLogEntry;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.GlobalStatePointer;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.RefPointer;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.RefPointer.Type;
import org.projectnessie.versioned.persist.serialize.ProtoSerialization;

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
  public Hash hashOnReference(NamedRef namedReference, Optional<Hash> hashOnReference)
      throws ReferenceNotFoundException {
    NonTxOperationContext ctx = NonTxOperationContext.DUMMY;

    return hashOnRef(ctx, namedReference, hashOnReference);
  }

  @Override
  public Stream<Optional<ContentsAndState<ByteString>>> values(
      Hash commit, List<Key> keys, KeyFilterPredicate keyFilter) throws ReferenceNotFoundException {
    NonTxOperationContext ctx = NonTxOperationContext.DUMMY;

    Map<Key, ContentsAndState<ByteString>> result = fetchValues(ctx, commit, keys, keyFilter);

    return keys.stream().map(result::get).map(Optional::ofNullable);
  }

  @Override
  public Stream<CommitLogEntry> commitLog(Hash offset) throws ReferenceNotFoundException {
    NonTxOperationContext ctx = NonTxOperationContext.DUMMY;

    return readCommitLogStream(ctx, offset);
  }

  @Override
  public Stream<WithHash<NamedRef>> namedRefs() {
    NonTxOperationContext ctx = NonTxOperationContext.DUMMY;

    GlobalStatePointer pointer = fetchGlobalPointer(ctx);
    return pointer.getNamedReferencesMap().entrySet().stream()
        .map(
            r ->
                WithHash.of(
                    Hash.of(r.getValue().getHash()),
                    toNamedRef(r.getValue().getType(), r.getKey())));
  }

  protected static NamedRef toNamedRef(Type type, String key) {
    switch (type) {
      case Branch:
        return BranchName.of(key);
      case Tag:
        return TagName.of(key);
      default:
        throw new IllegalArgumentException(type.name());
    }
  }

  @Override
  public Stream<KeyWithType> keys(Hash commit, KeyFilterPredicate keyFilter)
      throws ReferenceNotFoundException {
    NonTxOperationContext ctx = NonTxOperationContext.DUMMY;

    return keysForCommitEntry(ctx, commit, keyFilter);
  }

  @Override
  public Hash merge(Hash from, BranchName toBranch, Optional<Hash> expectedHead)
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
          true,
          (ctx, pointer, branchCommits, newKeyLists) -> {
            Hash toHead = branchHead(pointer, toBranch);

            long timeInMicros = commitTimeInMicros();

            toHead =
                mergeAttempt(
                    ctx,
                    timeInMicros,
                    from,
                    toBranch,
                    expectedHead,
                    toHead,
                    branchCommits,
                    newKeyLists);

            Hash newGlobalHead =
                writeGlobalCommit(
                    ctx, timeInMicros, Hash.of(pointer.getGlobalId()), Collections.emptyList());

            return updateNamedRef(toBranch, pointer, toHead, newGlobalHead);

            // 8. return hash of last commit added to 'targetBranch' (via the casOpLoop)
          },
          () -> mergeConflictMessage("Retry-failure", from, toBranch, expectedHead));
    } catch (ReferenceNotFoundException | ReferenceConflictException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected GlobalStatePointer updateNamedRef(
      NamedRef target, GlobalStatePointer pointer, Hash toHead, Hash newGlobalHead) {
    GlobalStatePointer.Builder newPointer =
        GlobalStatePointer.newBuilder().setGlobalId(newGlobalHead.asBytes());

    newPointer.putAllNamedReferences(pointer.getNamedReferencesMap());

    RefPointer.Type type;
    if (target instanceof BranchName) {
      type = RefPointer.Type.Branch;
    } else if (target instanceof TagName) {
      type = RefPointer.Type.Tag;
    } else {
      throw new IllegalArgumentException(target.getClass().getSimpleName());
    }
    newPointer.putNamedReferences(
        target.getName(), RefPointer.newBuilder().setType(type).setHash(toHead.asBytes()).build());

    return newPointer.build();
  }

  @SuppressWarnings("RedundantThrows")
  @Override
  public Hash transplant(
      BranchName targetBranch, Optional<Hash> expectedHead, List<Hash> sequenceToTransplant)
      throws ReferenceNotFoundException, ReferenceConflictException {
    try {
      return casOpLoop(
          targetBranch,
          false,
          true,
          (ctx, pointer, branchCommits, newKeyLists) -> {
            Hash targetHead = branchHead(pointer, targetBranch);

            long timeInMicros = commitTimeInMicros();

            targetHead =
                transplantAttempt(
                    ctx,
                    timeInMicros,
                    targetBranch,
                    expectedHead,
                    targetHead,
                    sequenceToTransplant,
                    branchCommits,
                    newKeyLists);

            Hash newGlobalHead =
                writeGlobalCommit(
                    ctx, timeInMicros, Hash.of(pointer.getGlobalId()), Collections.emptyList());

            return updateNamedRef(targetBranch, pointer, targetHead, newGlobalHead);

            // 6. return hash of last commit added to 'targetBranch' (via the casOpLoop)
          },
          () ->
              transplantConflictMessage(
                  "Retry-failure", targetBranch, expectedHead, sequenceToTransplant));

    } catch (ReferenceNotFoundException | ReferenceConflictException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Hash commit(CommitAttempt commitAttempt)
      throws ReferenceConflictException, ReferenceNotFoundException {
    try {
      return casOpLoop(
          commitAttempt.getCommitToBranch(),
          false,
          true,
          (ctx, pointer, x, newKeyLists) -> {
            Hash branchHead = branchHead(pointer, commitAttempt.getCommitToBranch());

            long timeInMicros = commitTimeInMicros();

            CommitLogEntry newBranchCommit =
                commitAttempt(ctx, timeInMicros, branchHead, commitAttempt, newKeyLists);

            Hash newGlobalHead =
                writeGlobalCommit(
                    ctx,
                    timeInMicros,
                    Hash.of(pointer.getGlobalId()),
                    commitAttempt.getGlobal().entrySet().stream()
                        .map(e -> ContentsIdAndBytes.of(e.getKey(), (byte) 0, e.getValue()))
                        .collect(Collectors.toList()));

            return updateNamedRef(
                commitAttempt.getCommitToBranch(),
                pointer,
                newBranchCommit.getHash(),
                newGlobalHead);
          },
          () ->
              commitConflictMessage(
                  "Retry-Failure",
                  commitAttempt.getCommitToBranch(),
                  commitAttempt.getExpectedHead()));
    } catch (ReferenceNotFoundException | ReferenceConflictException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Hash create(NamedRef ref, Hash target)
      throws ReferenceAlreadyExistsException, ReferenceNotFoundException {
    try {
      return casOpLoop(
          ref,
          false,
          false,
          (ctx, pointer, branchCommits, newKeyLists) -> {
            if (pointer.getNamedReferencesMap().containsKey(ref.getName())) {
              throw referenceAlreadyExists(ref);
            }

            Hash hash = target;
            if (hash == null) {
              // Special case: Don't validate, if the 'target' parameter is null.
              // This is mostly used for tests that re-create the default-branch.
              hash = NO_ANCESTOR;
            }

            validateHashExists(ctx, hash);

            // Need a new empty global-log entry to be able to CAS
            Hash newGlobalHead = noopGlobalLogEntry(ctx, pointer);

            return updateNamedRef(ref, pointer, hash, newGlobalHead);
          },
          () -> createConflictMessage("Retry-Failure", ref, target));
    } catch (ReferenceAlreadyExistsException | ReferenceNotFoundException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void delete(NamedRef reference, Optional<Hash> expectedHead)
      throws ReferenceNotFoundException, ReferenceConflictException {
    try {
      casOpLoop(
          reference,
          true,
          false,
          (ctx, pointer, branchCommits, newKeyLists) -> {
            verifyExpectedHash(branchHead(pointer, reference), reference, expectedHead);
            Hash newGlobalHead = noopGlobalLogEntry(ctx, pointer);

            GlobalStatePointer.Builder newPointer =
                GlobalStatePointer.newBuilder().setGlobalId(newGlobalHead.asBytes());
            newPointer.putAllNamedReferences(pointer.getNamedReferencesMap());
            newPointer.removeNamedReferences(reference.getName());
            return newPointer.build();
          },
          () -> deleteConflictMessage("Retry-Failure", reference, expectedHead));
    } catch (ReferenceNotFoundException | ReferenceConflictException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void assign(NamedRef assignee, Optional<Hash> expectedHead, Hash assignTo)
      throws ReferenceNotFoundException, ReferenceConflictException {
    try {
      casOpLoop(
          assignee,
          false,
          false,
          (ctx, pointer, branchCommits, newKeyLists) -> {
            verifyExpectedHash(branchHead(pointer, assignee), assignee, expectedHead);

            validateHashExists(ctx, assignTo);

            Hash newGlobalHead = noopGlobalLogEntry(ctx, pointer);

            return updateNamedRef(assignee, pointer, assignTo, newGlobalHead);
          },
          () -> assignConflictMessage("Retry-Failure", assignee, expectedHead, assignTo));
    } catch (ReferenceNotFoundException | ReferenceConflictException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Stream<Diff<ByteString>> diff(Hash from, Hash to, KeyFilterPredicate keyFilter)
      throws ReferenceNotFoundException {
    NonTxOperationContext ctx = NonTxOperationContext.DUMMY;

    return buildDiff(ctx, from, to, keyFilter);
  }

  @Override
  public void initializeRepo(String defaultBranchName) {
    NonTxOperationContext ctx = NonTxOperationContext.DUMMY;
    if (fetchGlobalPointer(ctx) == null) {
      Hash globalHead;
      try {
        long timeInMicros = commitTimeInMicros();
        globalHead = writeGlobalCommit(ctx, timeInMicros, NO_ANCESTOR, Collections.emptyList());
      } catch (ReferenceConflictException e) {
        throw new RuntimeException(e);
      }

      unsafeWriteGlobalPointer(
          ctx,
          GlobalStatePointer.newBuilder()
              .setGlobalId(globalHead.asBytes())
              .putNamedReferences(
                  defaultBranchName,
                  RefPointer.newBuilder()
                      .setType(RefPointer.Type.Branch)
                      .setHash(NO_ANCESTOR.asBytes())
                      .build())
              .build());
    }
  }

  @Override
  public Stream<ContentsIdWithType> globalKeys(ToIntFunction<ByteString> contentsTypeExtractor) {
    NonTxOperationContext ctx = NonTxOperationContext.DUMMY;

    GlobalStatePointer pointer = fetchGlobalPointer(ctx);

    return globalLogFetcher(ctx, Hash.of(pointer.getGlobalId()))
        .flatMap(e -> e.getPutsList().stream())
        .map(ProtoSerialization::protoToContentsIdAndBytes)
        .map(ContentsIdAndBytes::asIdWithType)
        .distinct();
  }

  @Override
  public Stream<ContentsIdAndBytes> globalLog(
      Set<ContentsIdWithType> keys, ToIntFunction<ByteString> contentsTypeExtractor) {
    NonTxOperationContext ctx = NonTxOperationContext.DUMMY;

    GlobalStatePointer pointer = fetchGlobalPointer(ctx);

    HashSet<ContentsIdWithType> remaining = new HashSet<>(keys);

    Stream<GlobalStateLogEntry> stream = globalLogFetcher(ctx, Hash.of(pointer.getGlobalId()));

    return takeUntilIncludeLast(stream, x -> remaining.isEmpty())
        .flatMap(e -> e.getPutsList().stream())
        .map(ProtoSerialization::protoToContentsIdAndBytes)
        .filter(kct -> remaining.remove(kct.asIdWithType()));
  }

  @Override
  public Stream<KeyWithBytes> allContents(
      BiFunction<NamedRef, CommitLogEntry, Boolean> continueOnRefPredicate) {
    NonTxOperationContext ctx = NonTxOperationContext.DUMMY;

    GlobalStatePointer pointer = fetchGlobalPointer(ctx);

    Stream<KeyWithBytes> result = Stream.empty();

    Set<Hash> visistedHashes = new HashSet<>();

    for (Entry<String, RefPointer> e : pointer.getNamedReferencesMap().entrySet()) {
      Stream<KeyWithBytes> perRef =
          allContentsPerRefFetcher(
              ctx,
              continueOnRefPredicate,
              visistedHashes,
              toNamedRef(e.getValue().getType(), e.getKey()),
              Hash.of(e.getValue().getHash()));

      result = Stream.concat(result, perRef);
    }

    return result;
  }

  // /////////////////////////////////////////////////////////////////////////////////////////////
  // NMon-Transactional DatabaseAdapter subclass API (protected)
  // /////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Convenience method for {@link AbstractDatabaseAdapter#hashOnRef(Object, NamedRef, Optional,
   * Hash) hashOnRef(ctx, reference.getReference(), branchHead(fetchGlobalPointer(ctx), reference),
   * reference.getHashOnReference())}.
   */
  protected Hash hashOnRef(NonTxOperationContext ctx, NamedRef reference, Optional<Hash> hashOnRef)
      throws ReferenceNotFoundException {
    return hashOnRef(ctx, reference, hashOnRef, fetchGlobalPointer(ctx));
  }

  /**
   * Convenience method for {@link AbstractDatabaseAdapter#hashOnRef(Object, NamedRef, Optional,
   * Hash) hashOnRef(ctx, reference.getReference(), branchHead(pointer, reference),
   * reference.getHashOnReference())}.
   */
  protected Hash hashOnRef(
      NonTxOperationContext ctx,
      NamedRef reference,
      Optional<Hash> hashOnRef,
      GlobalStatePointer pointer)
      throws ReferenceNotFoundException {
    return hashOnRef(ctx, reference, hashOnRef, branchHead(pointer, reference));
  }

  /**
   * "Body" of a Compare-And-Swap loop that returns the value to apply. {@link #casOpLoop(NamedRef,
   * boolean, boolean, CasOp, Supplier)} then tries to perform the Compare-And-Swap using the known
   * "current value", as passed via the {@code pointer} parameter to {@link
   * #apply(NonTxOperationContext, GlobalStatePointer, Consumer, Consumer)}, and the "new value"
   * from the return value.
   */
  @FunctionalInterface
  public interface CasOp {
    /**
     * Applies an operation within a CAS-loop. The implementation gets the current global-state and
     * must return an updated global-state with a different global-id.
     *
     * @param ctx operation context
     * @param pointer "current value"
     * @param branchCommits if more commits than the one returned via the return value were
     *     optimistically written, those must be passed to this consumer.
     * @param newKeyLists IDs of optimistically written {@link KeyListEntity} entities must be
     *     passed to this consumer.
     * @return "new value" that {@link #casOpLoop(NamedRef, boolean, boolean, CasOp, Supplier)}
     *     tries to apply
     */
    GlobalStatePointer apply(
        NonTxOperationContext ctx,
        GlobalStatePointer pointer,
        Consumer<Hash> branchCommits,
        Consumer<Hash> newKeyLists)
        throws VersionStoreException;
  }

  /**
   * This is the actual CAS-loop, which applies an operation onto a named-ref.
   *
   * @param ref named-reference on which the operation happens
   * @param deleteRef whether the operation is a "delete-reference" operation, which means that this
   *     function cannot return the new branch's HEAD, because it no longer exists.
   * @param commitOp whether the hash returned by {@code casOp} will be a new commit and/or {@code
   *     casOp} produced more commits (think: merge+transplant) via the {@code individualCommits}
   *     argument to {@link CasOp#apply(NonTxOperationContext, GlobalStatePointer, Consumer,
   *     Consumer)}. Those commits will be unconditionally deleted, if this {@code commitOp} flag is
   *     {@code true}.
   * @param casOp the implementation of the CAS-operation
   * @param retryErrorMessage provides an error-message for a {@link ReferenceConflictException}
   *     when the CAS operation failed to complete within the configured time and number of retries.
   */
  protected Hash casOpLoop(
      NamedRef ref,
      boolean deleteRef,
      boolean commitOp,
      CasOp casOp,
      Supplier<String> retryErrorMessage)
      throws VersionStoreException {
    NonTxOperationContext ctx = NonTxOperationContext.DUMMY;

    try (TryLoopState tryState = newTryLoopState(retryErrorMessage, config)) {
      while (true) {
        GlobalStatePointer pointer = fetchGlobalPointer(ctx);
        Set<Hash> individualCommits = new HashSet<>();
        Set<Hash> individualKeyLists = new HashSet<>();

        GlobalStatePointer newPointer =
            casOp.apply(ctx, pointer, individualCommits::add, individualKeyLists::add);
        if (newPointer.getGlobalId().equals(pointer.getGlobalId())) {
          return tryState.success(branchHead(pointer, ref));
        }
        Hash branchHead = deleteRef ? null : branchHead(newPointer, ref);

        if (pointer.getGlobalId().equals(newPointer.getGlobalId())) {
          throw hashCollisionDetected();
        }

        if (globalPointerCas(ctx, pointer, newPointer)) {
          return tryState.success(branchHead);
        } else if (commitOp) {
          if (branchHead != null) {
            individualCommits.add(branchHead);
          }
          cleanUpCommitCas(
              ctx, Hash.of(newPointer.getGlobalId()), individualCommits, individualKeyLists);
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
      NonTxOperationContext ctx,
      long timeInMicros,
      Hash parentHash,
      List<ContentsIdAndBytes> globals)
      throws ReferenceConflictException {
    Hasher hasher = newHasher();
    hasher
        .putLong(GLOBAL_LOG_HASH_SEED)
        // add some randomness here to "avoid" hash-collisions for the global-state-log
        .putLong(ThreadLocalRandom.current().nextLong())
        .putBytes(parentHash.asBytes().asReadOnlyByteBuffer());

    GlobalStateLogEntry currentEntry = fetchFromGlobalLog(ctx, parentHash);

    Stream<Hash> newParents = Stream.of(parentHash);
    if (currentEntry != null) {
      newParents =
          Stream.concat(
              newParents,
              currentEntry.getParentsList().stream()
                  .skip(1)
                  .limit(config.getParentsPerCommit() - 1)
                  .map(Hash::of));
    }

    Hash hash = randomHash();
    GlobalStateLogEntry.Builder entry =
        GlobalStateLogEntry.newBuilder().setCreatedTime(timeInMicros).setId(hash.asBytes());
    newParents.forEach(p -> entry.addParents(p.asBytes()));
    globals.forEach(g -> entry.addPuts(ProtoSerialization.toProto(g)));
    writeGlobalCommit(ctx, entry.build());

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
   * failed, {@link
   * org.projectnessie.versioned.persist.adapter.DatabaseAdapter#commit(CommitAttempt)} calls this
   * function to remove the optimistically written data.
   *
   * <p>Implementation notes: non-transactional implementations <em>must</em> delete entries for the
   * given keys, no-op for transactional implementations.
   */
  protected abstract void cleanUpCommitCas(
      NonTxOperationContext ctx, Hash globalId, Set<Hash> branchCommits, Set<Hash> newKeyLists);

  /**
   * Writes a global-state-log-entry without any operations, just to move the global-pointer
   * forwards for a "proper" CAS operation.
   */
  // TODO maybe replace with a 2nd-ary value in global-state-pointer to prevent the empty
  //  global-log-entry
  protected Hash noopGlobalLogEntry(NonTxOperationContext ctx, GlobalStatePointer pointer)
      throws ReferenceConflictException {
    // Need a new empty global-log entry to be able to CAS
    long timeInMicros = commitTimeInMicros();
    return writeGlobalCommit(
        ctx, timeInMicros, Hash.of(pointer.getGlobalId()), Collections.emptyList());
  }

  /**
   * Retrieves the current HEAD of {@code ref} using the given "global state pointer".
   *
   * @param pointer current global state pointer
   * @param ref reference to retrieve the current HEAD for
   * @return current HEAD, not {@code null}
   * @throws ReferenceNotFoundException if {@code ref} does not exist.
   */
  protected static Hash branchHead(GlobalStatePointer pointer, NamedRef ref)
      throws ReferenceNotFoundException {
    RefPointer branchHead = pointer.getNamedReferencesMap().get(ref.getName());
    if (branchHead == null || !ref.equals(toNamedRef(branchHead.getType(), ref.getName()))) {
      throw referenceNotFound(ref);
    }
    return Hash.of(branchHead.getHash());
  }

  /** Load the current global-state-pointer. */
  protected abstract GlobalStatePointer fetchGlobalPointer(NonTxOperationContext ctx);

  @Override
  protected Map<ContentsId, ByteString> fetchGlobalStates(
      NonTxOperationContext ctx, Set<ContentsId> contentsIds) {
    if (contentsIds.isEmpty()) {
      return Collections.emptyMap();
    }

    Set<ContentsId> remainingIds = new HashSet<>(contentsIds);
    Hash globalHead = Hash.of(fetchGlobalPointer(ctx).getGlobalId());

    Stream<GlobalStateLogEntry> log = globalLogFetcher(ctx, globalHead);

    return takeUntilExcludeLast(log, x -> remainingIds.isEmpty())
        .flatMap(e -> e.getPutsList().stream())
        .filter(put -> remainingIds.remove(ContentsId.of(put.getContentsId().getId())))
        .collect(
            Collectors.toMap(
                e -> ContentsId.of(e.getContentsId().getId()), ContentsIdWithBytes::getValue));
  }

  /** Reads from the global-state-log starting at the given global-state-log-ID. */
  private Stream<GlobalStateLogEntry> globalLogFetcher(NonTxOperationContext ctx, Hash initialId) {
    GlobalStateLogEntry initial = fetchFromGlobalLog(ctx, initialId);
    if (initial == null) {
      throw new RuntimeException(
          new ReferenceNotFoundException(
              String.format("Global log entry '%s' not does not exist.", initialId.asString())));
    }
    return logFetcher(
        ctx,
        initial,
        this::fetchPageFromGlobalLog,
        e -> e.getParentsList().stream().map(Hash::of).collect(Collectors.toList()));
  }

  /** Load the global-log entry with the given id. */
  protected abstract GlobalStateLogEntry fetchFromGlobalLog(NonTxOperationContext ctx, Hash id);

  protected abstract List<GlobalStateLogEntry> fetchPageFromGlobalLog(
      NonTxOperationContext ctx, List<Hash> hashes);
}
