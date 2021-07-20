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
package org.projectnessie.versioned.tiered.adapter;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.Spliterators.AbstractSpliterator;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
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

/** Base database-adapter class. */
public abstract class AbstractDatabaseAdapter<OP_CONTEXT extends AutoCloseable>
    implements DatabaseAdapter {

  protected final DatabaseAdapterConfiguration config;

  @SuppressWarnings("UnstableApiUsage")
  public static final Hash NO_ANCESTOR =
      Hash.of(
          UnsafeByteOperations.unsafeWrap(
              newHasher().putString("empty", StandardCharsets.UTF_8).hash().asBytes()));

  protected static long GLOBAL_LOG_HASH_SEED = -3031091797576167804L;

  protected static long COMMIT_LOG_HASH_SEED = 946928273206945677L;

  protected AbstractDatabaseAdapter(DatabaseAdapterConfiguration config) {
    this.config = config;
  }

  @Override
  public Stream<Optional<ContentsAndState<ByteString, ByteString>>> values(
      NamedRef ref, Optional<Hash> hashOnRef, List<Key> keys) throws ReferenceNotFoundException {
    OP_CONTEXT ctx = operationContext();
    boolean failed = true;
    try {
      GlobalStatePointer pointer = fetchGlobalPointer(ctx);
      Hash hash = hashOnRef(ctx, pointer, Optional.of(ref), hashOnRef);

      Stream<Optional<ContentsAndState<ByteString, ByteString>>> r = fetchValues(ctx, hash, keys);
      failed = false;
      return r.onClose(() -> ctxClose(ctx));
    } finally {
      if (failed) {
        ctxClose(ctx);
      }
    }
  }

  @Override
  public Stream<CommitLogEntry> commitLog(
      NamedRef ref, Optional<Hash> offset, Optional<Hash> untilIncluding)
      throws ReferenceNotFoundException {
    OP_CONTEXT ctx = operationContext();
    boolean failed = true;
    try {
      GlobalStatePointer pointer = fetchGlobalPointer(ctx);
      Hash hash = hashOnRef(ctx, pointer, Optional.of(ref), offset);

      Stream<CommitLogEntry> intLog = commitLogFetcher(ctx, hash);
      if (untilIncluding.isPresent()) {
        intLog = takeUntil(intLog, e -> e.getHash().equals(untilIncluding.get()), true);
      }

      failed = false;
      return intLog.onClose(() -> ctxClose(ctx));
    } finally {
      if (failed) {
        ctxClose(ctx);
      }
    }
  }

  @Override
  public Stream<ByteString> entries(NamedRef ref, Optional<Hash> hash)
      throws ReferenceNotFoundException {
    OP_CONTEXT ctx = operationContext();
    try {
      throw new UnsupportedOperationException();
    } finally {
      ctxClose(ctx);
    }
  }

  @Override
  public Hash toHash(NamedRef ref) throws ReferenceNotFoundException {
    OP_CONTEXT ctx = operationContext();
    try {
      return fetchGlobalAndRefHeads(ctx, ref).getBranchHead();
    } finally {
      ctxClose(ctx);
    }
  }

  @Override
  public Stream<WithHash<NamedRef>> namedRefs() {
    OP_CONTEXT ctx = operationContext();
    try {
      GlobalStatePointer pointer = fetchGlobalPointer(ctx);
      return pointer.getNamedReferences().entrySet().stream()
          .map(e -> WithHash.of(e.getValue(), e.getKey()));
    } finally {
      ctxClose(ctx);
    }
  }

  @Override
  public Stream<KeyWithType> keys(NamedRef ref, Optional<Hash> hashOnRef)
      throws ReferenceNotFoundException {
    OP_CONTEXT ctx = operationContext();
    boolean failed = true;
    try {
      GlobalStatePointer pointer = fetchGlobalPointer(ctx);
      Hash hash = hashOnRef(ctx, pointer, Optional.of(ref), hashOnRef);
      Stream<KeyWithType> r = keysForCommitEntry(ctx, hash);
      failed = false;
      return r.onClose(() -> ctxClose(ctx));
    } finally {
      if (failed) {
        ctxClose(ctx);
      }
    }
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
            Hash toHead = pointer.branchHead(toBranch);

            // 1. ensure 'expectedHash' is a parent of HEAD-of-'toBranch'
            hashOnRef(ctx, pointer, Optional.of(toBranch), expectedHash);

            // 2. ensure 'fromHash' is reachable from 'from'
            Hash fromHead = hashOnRef(ctx, pointer, Optional.of(from), fromHash);

            // 3. find nearest common-ancestor between 'from' + 'fromHash'
            Hash commonAncestor =
                findCommonAncestor(ctx, from, fromHead, toBranch, toHead, commonAncestorRequired);

            // 4. Collect commit-log-entries
            List<CommitLogEntry> toEntriesReverseChronological =
                takeUntil(commitLogFetcher(ctx, toHead), e -> e.getHash().equals(commonAncestor))
                    .collect(Collectors.toList());
            Collections.reverse(toEntriesReverseChronological);
            List<CommitLogEntry> commitsToMergeChronological =
                takeUntil(commitLogFetcher(ctx, fromHead), e -> e.getHash().equals(commonAncestor))
                    .collect(Collectors.toList());

            // 4. Collect modified keys.
            Set<Key> keysTouchedOnTarget = collectModifiedKeys(toEntriesReverseChronological);

            // 5. check for key-collisions
            checkForKeyCollisions(keysTouchedOnTarget, commitsToMergeChronological);

            // (no need to verify the global states during a transplant)
            // 6. re-apply commits in 'sequenceToTransplant' onto 'targetBranch'
            toHead = copyCommits(ctx, toHead, commitsToMergeChronological);

            // 7. CAS

            commitsToMergeChronological.stream()
                .map(CommitLogEntry::getHash)
                .forEach(individualCommits);
            writeIndividualCommits(ctx, commitsToMergeChronological);

            Hash newGlobalHead =
                writeGlobalCommit(ctx, pointer.getGlobalId(), Collections.emptyMap());

            Map<NamedRef, Hash> refs = new HashMap<>(pointer.getNamedReferences());
            refs.put(toBranch, toHead);
            return GlobalStatePointer.of(newGlobalHead, refs);

            // 8. return hash of last commit added to 'targetBranch' (via the casOpLoop)
          });
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
            Hash targetHead = pointer.branchHead(targetBranch);

            // 1. ensure 'expectedHash' is a parent of HEAD-of-'targetBranch' & collect keys
            List<CommitLogEntry> targetEntriesReverseChronological = new ArrayList<>();
            hashOnRef(
                ctx,
                pointer,
                Optional.of(targetBranch),
                expectedHash,
                targetEntriesReverseChronological::add);
            Collections.reverse(targetEntriesReverseChronological);

            // 2. Collect modified keys.
            Set<Key> keysTouchedOnTarget = collectModifiedKeys(targetEntriesReverseChronological);

            // 3. verify last hash in 'sequenceToTransplant' is in 'source'
            Hash lastHash = sequenceToTransplant.get(sequenceToTransplant.size() - 1);
            hashOnRef(ctx, pointer, Optional.of(source), Optional.of(lastHash));

            // 4. ensure 'sequenceToTransplant' is sequential
            int[] index = new int[] {sequenceToTransplant.size() - 1};
            List<CommitLogEntry> commitsToTransplantChronological =
                takeUntil(
                        commitLogFetcher(ctx, lastHash),
                        e -> {
                          int i = index[0]--;
                          if (i == -1) {
                            return true;
                          }
                          if (!e.getHash().equals(sequenceToTransplant.get(i))) {
                            throw new IllegalArgumentException(
                                "Sequence of hashes is not contiguous.");
                          }
                          return false;
                        })
                    .collect(Collectors.toList());

            // 5. check for key-collisions
            checkForKeyCollisions(keysTouchedOnTarget, commitsToTransplantChronological);

            // (no need to verify the global states during a transplant)
            // 6. re-apply commits in 'sequenceToTransplant' onto 'targetBranch'
            targetHead = copyCommits(ctx, targetHead, commitsToTransplantChronological);

            // 7. CAS

            commitsToTransplantChronological.stream()
                .map(CommitLogEntry::getHash)
                .forEach(individualCommits);
            writeIndividualCommits(ctx, commitsToTransplantChronological);

            Hash newGlobalHead =
                writeGlobalCommit(ctx, pointer.getGlobalId(), Collections.emptyMap());

            Map<NamedRef, Hash> refs = new HashMap<>(pointer.getNamedReferences());
            refs.put(targetBranch, targetHead);
            return GlobalStatePointer.of(newGlobalHead, refs);

            // 6. return hash of last commit added to 'targetBranch' (via the casOpLoop)
          });

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
            Heads currentHeads = headsFromGlobalState(branch, pointer);

            List<String> mismatches = new ArrayList<>();

            // verify expected global-states
            checkExpectedGlobalStates(ctx, expectedStates, mismatches);

            checkForModifiedKeysBetweenExpectedAndCurrentCommit(
                branch, expectedHead, operationsKeys, ctx, currentHeads, mismatches);

            if (!mismatches.isEmpty()) {
              throw new ReferenceConflictException(String.join("\n", mismatches));
            }

            CommitLogEntry currentBranchEntry =
                fetchFromCommitLog(ctx, currentHeads.getBranchHead());

            int parentsPerCommit = PARENTS_PER_COMMIT.get(config);
            List<Hash> newParents = new ArrayList<>(parentsPerCommit);
            newParents.add(currentHeads.getBranchHead());
            if (currentBranchEntry != null) {
              List<Hash> p = currentBranchEntry.getParents();
              newParents.addAll(p.subList(0, Math.min(p.size(), parentsPerCommit - 1)));
            }

            CommitLogEntry newBranchCommit =
                buildIndividualCommit(
                    ctx,
                    newParents,
                    commitMetaSerialized,
                    puts,
                    unchanged,
                    deletes,
                    currentBranchEntry != null ? currentBranchEntry.getKeyListDistance() : 0);
            writeIndividualCommit(ctx, newBranchCommit);

            Hash newGlobalHead = writeGlobalCommit(ctx, currentHeads.getGlobalId(), global);

            Map<NamedRef, Hash> refs = new HashMap<>(pointer.getNamedReferences());
            refs.put(branch, newBranchCommit.getHash());
            return GlobalStatePointer.of(newGlobalHead, refs);
          });
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

      return casOpLoop(
          ref,
          false,
          (ctx, pointer, x) -> {
            if (pointer.getNamedReferences().containsKey(ref)) {
              throw new ReferenceAlreadyExistsException(
                  String.format("Named reference '%s' already exists.", ref));
            }

            Optional<Hash> beginning =
                targetHash.isPresent() ? targetHash : Optional.of(NO_ANCESTOR);
            Hash hash;
            if (beginning.get().equals(NO_ANCESTOR)
                && !target.isPresent()
                && ref.equals(BranchName.of(DEFAULT_BRANCH.get(config)))) {
              // Handle the special case when the default-branch does not exist and the current
              // request creates it. This mostly happens during tests.
              hash = NO_ANCESTOR;
            } else {
              hash = hashOnRef(ctx, pointer, target, beginning);
            }

            // Need a new empty global-log entry to be able to CAS
            Hash newGlobalHead = noopGlobalLogEntry(ctx, pointer);

            Map<NamedRef, Hash> refs = new HashMap<>(pointer.getNamedReferences());
            refs.put(ref, hash);
            return GlobalStatePointer.of(newGlobalHead, refs);
          });
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
            verifyExpectedHash(pointer, ref, hash);
            Hash newGlobalHead = noopGlobalLogEntry(ctx, pointer);

            Map<NamedRef, Hash> refs = new HashMap<>(pointer.getNamedReferences());
            refs.remove(ref);
            return GlobalStatePointer.of(newGlobalHead, refs);
          });
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
            verifyExpectedHash(pointer, ref, expectedHash);
            Hash assignToHead = hashOnRef(ctx, pointer, Optional.of(assignTo), assignToHash);

            Hash newGlobalHead = noopGlobalLogEntry(ctx, pointer);

            Map<NamedRef, Hash> refs = new HashMap<>(pointer.getNamedReferences());
            refs.put(ref, assignToHead);
            return GlobalStatePointer.of(newGlobalHead, refs);
          });
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
    OP_CONTEXT ctx = operationContext();
    try {

      // TODO this implementation works, but is definitely not the most efficient one.

      GlobalStatePointer pointer = fetchGlobalPointer(ctx);

      Hash fromHash = hashOnRef(ctx, pointer, Optional.of(from), hashOnFrom);
      Hash toHash = hashOnRef(ctx, pointer, Optional.of(to), hashOnTo);

      Set<Key> allKeys = new HashSet<>();
      try (Stream<Key> s = keysForCommitEntry(ctx, fromHash).map(KeyWithType::getKey)) {
        s.forEach(allKeys::add);
      }
      try (Stream<Key> s = keysForCommitEntry(ctx, toHash).map(KeyWithType::getKey)) {
        s.forEach(allKeys::add);
      }

      List<Key> allKeysList = new ArrayList<>(allKeys);
      List<Optional<ByteString>> fromContents =
          fetchValues(ctx, fromHash, allKeysList)
              .map(o -> o.map(ContentsAndState::getContents))
              .collect(Collectors.toList());
      List<Optional<ByteString>> toContents =
          fetchValues(ctx, toHash, allKeysList)
              .map(o -> o.map(ContentsAndState::getContents))
              .collect(Collectors.toList());

      return IntStream.range(0, allKeys.size())
          .mapToObj(
              index -> {
                Key k = allKeysList.get(index);
                Optional<ByteString> f = fromContents.get(index);
                Optional<ByteString> t = toContents.get(index);
                return f.equals(t) ? null : Diff.of(k, f, t);
              })
          .filter(Objects::nonNull);
    } finally {
      ctxClose(ctx);
    }
  }

  @Override
  public void initializeRepo() throws ReferenceConflictException {
    OP_CONTEXT ctx = operationContext();
    try {
      Hash globalHead = writeGlobalCommit(ctx, NO_ANCESTOR, Collections.emptyMap());

      Map<NamedRef, Hash> refMap =
          Collections.singletonMap(BranchName.of(DEFAULT_BRANCH.get(config)), NO_ANCESTOR);

      unsafeWriteGlobalPointer(ctx, GlobalStatePointer.of(globalHead, refMap));

      ctxCommit(ctx);
    } finally {
      ctxClose(ctx);
    }
  }

  // /////////////////////////////////////////////////////////////////////////////////////////////
  // DatabaseAdapter subclass API (protected)
  // /////////////////////////////////////////////////////////////////////////////////////////////

  protected abstract OP_CONTEXT operationContext();

  /**
   * Implementation notes: transactional implementations must commit changes, no-op for
   * non-transactional implementations.
   */
  protected abstract void ctxCommit(OP_CONTEXT ctx);

  /** Implementation notes: transactional implementations must rollback changes. */
  private void ctxClose(OP_CONTEXT ctx) {
    try {
      ctx.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("UnstableApiUsage")
  protected static Hasher newHasher() {
    return Hashing.sha256().newHasher();
  }

  protected ReferenceConflictException hashCollisionDetected() {
    return new ReferenceConflictException("Hash collision detected");
  }

  protected void verifyExpectedHash(
      GlobalStatePointer pointer, NamedRef ref, Optional<Hash> expectedHash)
      throws ReferenceConflictException, ReferenceNotFoundException {
    Hash refHead = pointer.branchHead(ref);
    if (expectedHash.isPresent() && !refHead.equals(expectedHash.get())) {
      throw new ReferenceConflictException(
          String.format(
              "Named-reference '%s' is not at expected hash '%s', but at '%s'.",
              ref.getName(), expectedHash.get().asString(), refHead.asString()));
    }
  }

  protected Hash hashOnRef(
      OP_CONTEXT ctx, GlobalStatePointer pointer, Optional<NamedRef> ref, Optional<Hash> hashOnRef)
      throws ReferenceNotFoundException {
    return hashOnRef(ctx, pointer, ref, hashOnRef, null);
  }

  protected Hash hashOnRef(
      OP_CONTEXT ctx,
      GlobalStatePointer pointer,
      Optional<NamedRef> refOpt,
      Optional<Hash> hashOnRef,
      Consumer<CommitLogEntry> commitLogVisitor)
      throws ReferenceNotFoundException {
    NamedRef ref = refOpt.orElseGet(() -> BranchName.of(DEFAULT_BRANCH.get(config)));
    if (hashOnRef.isPresent()) {
      Hash knownHead = pointer.branchHead(ref);
      Hash suspect = hashOnRef.get();
      if (suspect.equals(NO_ANCESTOR)) {
        // If the client requests 'NO_ANCESTOR' (== beginning of time), skip the existence-check.
        if (commitLogVisitor != null) {
          commitLogFetcher(ctx, knownHead).forEach(commitLogVisitor);
        }
        return suspect;
      }

      Stream<CommitLogEntry> checker = commitLogFetcher(ctx, knownHead);
      // TODO only need to fetch each CommitLogEntry if 'commitLogVisitor != null', otherwise
      //  the implementation can operate on the list of parents via CommitLogEntry.getParents().
      if (commitLogVisitor != null) {
        checker = checker.peek(commitLogVisitor);
      }
      if (checker.map(CommitLogEntry::getHash).noneMatch(suspect::equals)) {
        throw hashNotFound(ref, suspect);
      }
      return suspect;
    } else {
      return pointer.branchHead(ref);
    }
  }

  @FunctionalInterface
  public interface CasOp<OP_CONTEXT> {
    /**
     * Applies an operation within a CAS-loop. The implementation gets the current global-state and
     * must return an updated global-state with a different global-id.
     */
    GlobalStatePointer apply(
        OP_CONTEXT ctx, GlobalStatePointer pointer, Consumer<Hash> individualCommits)
        throws VersionStoreException;
  }

  /** This is the actual CAS-loop, which applies an operation onto a named-ref. */
  protected Hash casOpLoop(NamedRef ref, boolean deleteRef, CasOp<OP_CONTEXT> casOp)
      throws VersionStoreException {
    // TODO this loop should be bounded (time + #attempts)
    while (true) {
      OP_CONTEXT ctx = operationContext();
      try {
        GlobalStatePointer pointer = fetchGlobalPointer(ctx);
        Set<Hash> individualCommits = new HashSet<>();

        GlobalStatePointer newPointer = casOp.apply(ctx, pointer, individualCommits::add);
        if (newPointer.getGlobalId().equals(pointer.getGlobalId())) {
          return pointer.branchHead(ref);
        }
        Hash branchHead = deleteRef ? null : newPointer.branchHead(ref);

        if (pointer.getGlobalId().equals(newPointer.getGlobalId())) {
          throw hashCollisionDetected();
        }

        if (pointer.getGlobalId().equals(pointer.getGlobalId())
            && globalPointerCas(ctx, pointer, newPointer)) {
          ctxCommit(ctx);
          return branchHead;
        } else {
          if (branchHead != null) {
            individualCommits.add(branchHead);
          }
          cleanUpCommitCas(ctx, newPointer.getGlobalId(), individualCommits);
        }
      } finally {
        ctxClose(ctx);
      }
    }
  }

  /**
   * Writes a global-state-log-entry without any operations, just to move the global-pointer
   * forwards for a "proper" CAS operation.
   */
  // TODO maybe replace with a 2nd-ary value in global-state-pointer to prevent the empty
  //  global-log-entry
  protected Hash noopGlobalLogEntry(OP_CONTEXT ctx, GlobalStatePointer pointer)
      throws ReferenceConflictException {
    // Need a new empty global-log entry to be able to CAS
    return writeGlobalCommit(ctx, pointer.getGlobalId(), Collections.emptyMap());
  }

  /**
   * Fetches the HEADs of both the global-state log and of the requested branch. Implementations
   * shall fetch both values concurrently.
   */
  protected Heads fetchGlobalAndRefHeads(OP_CONTEXT ctx, NamedRef ref)
      throws ReferenceNotFoundException {
    GlobalStatePointer pointer = fetchGlobalPointer(ctx);
    return headsFromGlobalState(ref, pointer);
  }

  protected Heads headsFromGlobalState(NamedRef ref, GlobalStatePointer pointer)
      throws ReferenceNotFoundException {
    Hash branchHead = pointer.branchHead(ref);
    return Heads.of(pointer.getGlobalId(), branchHead);
  }

  /** Load the current global-state-pointer. */
  protected abstract GlobalStatePointer fetchGlobalPointer(OP_CONTEXT ctx);

  /** Fetches the global-state information for the given keys. */
  protected Map<Key, ByteString> fetchGlobalStates(OP_CONTEXT ctx, Collection<Key> keys) {
    Set<Key> remainingKeys = new HashSet<>(keys);
    Map<Key, ByteString> result = new HashMap<>();
    Hash globalHead = fetchGlobalPointer(ctx).getGlobalId();

    Stream<GlobalStateLogEntry> log = globalLog(ctx, globalHead);
    log = takeUntil(log, x -> remainingKeys.isEmpty());
    log.forEach(
        entry -> {
          for (Entry<Key, ByteString> state : entry.getStatePuts().entrySet()) {
            if (remainingKeys.remove(state.getKey())) {
              result.put(state.getKey(), state.getValue());
            }
          }
        });
    return result;
  }

  /** Load the global-log entry with the given id. */
  protected abstract GlobalStateLogEntry fetchFromGlobalLog(OP_CONTEXT ctx, Hash id);

  protected abstract List<GlobalStateLogEntry> fetchPageFromGlobalLog(
      OP_CONTEXT ctx, List<Hash> hashes);

  /**
   * Fetch the global-state and per-ref contents for the given {@link Key}s and {@link Hash
   * commitSha}.
   */
  protected Stream<Optional<ContentsAndState<ByteString, ByteString>>> fetchValues(
      OP_CONTEXT ctx, Hash refHead, List<Key> keys) throws ReferenceNotFoundException {
    Map<Key, ContentsAndState<ByteString, ByteString>> result = new HashMap<>(keys.size() * 2);
    Map<Key, Integer> remainingKeys = new HashMap<>(keys.size() * 2);
    for (int i = 0; i < keys.size(); i++) {
      remainingKeys.put(keys.get(i), i);
    }

    Map<Key, ByteString> globals = fetchGlobalStates(ctx, keys);

    try (Stream<CommitLogEntry> log =
        takeUntil(commitLogFetcher(ctx, refHead), e -> remainingKeys.isEmpty())) {
      log.forEach(
          entry -> {
            entry.getDeletes().forEach(remainingKeys::remove);
            for (KeyWithBytes put : entry.getPuts()) {
              Integer index = remainingKeys.remove(put.getKey());
              if (index != null) {
                ByteString global = globals.get(put.getKey());
                if (global != null) {
                  result.put(put.getKey(), ContentsAndState.of(put.getValue(), global));
                }
                // TODO do what if there's no global state for that key??
              }
            }
          });
    }

    return keys.stream().map(result::get).map(Optional::ofNullable);
  }

  /** Load the commit-log entry for the given hash. */
  protected abstract CommitLogEntry fetchFromCommitLog(OP_CONTEXT ctx, Hash hash);

  protected abstract List<CommitLogEntry> fetchPageFromCommitLog(OP_CONTEXT ctx, List<Hash> hashes);

  private Stream<CommitLogEntry> commitLogFetcher(OP_CONTEXT ctx, Hash initialHash)
      throws ReferenceNotFoundException {
    return logFetcher(ctx, initialHash, this::fetchPageFromCommitLog, CommitLogEntry::getParents);
  }

  private Stream<GlobalStateLogEntry> globalLog(OP_CONTEXT ctx, Hash initialHash) {
    return logFetcher(
        ctx, initialHash, this::fetchPageFromGlobalLog, GlobalStateLogEntry::getParents);
  }

  protected <T> Stream<T> logFetcher(
      OP_CONTEXT ctx,
      Hash initialHash,
      BiFunction<OP_CONTEXT, List<Hash>, List<T>> fetcher,
      Function<T, List<Hash>> nextPage) {
    AbstractSpliterator<T> split =
        new AbstractSpliterator<T>(Long.MAX_VALUE, 0) {
          private Iterator<T> currentBatch;
          private boolean eof;
          private T previous;

          @Override
          public boolean tryAdvance(Consumer<? super T> consumer) {
            if (eof) {
              return false;
            } else if (currentBatch == null) {
              currentBatch = fetcher.apply(ctx, Collections.singletonList(initialHash)).iterator();
            } else if (!currentBatch.hasNext()) {
              if (previous == null) {
                eof = true;
                return false;
              }
              List<Hash> page = nextPage.apply(previous);
              previous = null;
              if (!page.isEmpty()) {
                currentBatch = fetcher.apply(ctx, page).iterator();
              } else {
                eof = true;
                return false;
              }
            }
            T v = currentBatch.next();
            if (v != null) {
              consumer.accept(v);
              previous = v;
            }
            return true;
          }
        };
    return StreamSupport.stream(split, false);
  }

  private <T> Stream<T> takeUntil(Stream<T> s, Predicate<T> predicate) {
    return takeUntil(s, predicate, false);
  }

  private <T> Stream<T> takeUntil(Stream<T> s, Predicate<T> predicate, boolean including) {
    Spliterator<T> src = s.spliterator();
    AbstractSpliterator<T> split =
        new AbstractSpliterator<T>(src.estimateSize(), 0) {
          boolean done = false;

          @Override
          public boolean tryAdvance(Consumer<? super T> consumer) {
            if (done) {
              return false;
            }
            return src.tryAdvance(
                elem -> {
                  boolean t = predicate.test(elem);
                  if (t && !including) {
                    done = true;
                  }
                  if (!done) {
                    consumer.accept(elem);
                  }
                  if (t && including) {
                    done = true;
                  }
                });
          }
        };
    return StreamSupport.stream(split, false).onClose(s::close);
  }

  /**
   * Unsafe operation to initialize a repository: unconditionally writes the global-state-pointer.
   */
  protected abstract void unsafeWriteGlobalPointer(OP_CONTEXT ctx, GlobalStatePointer pointer);

  @SuppressWarnings("UnstableApiUsage")
  protected Hash writeGlobalCommit(OP_CONTEXT ctx, Hash parentHash, Map<Key, ByteString> globals)
      throws ReferenceConflictException {
    Hasher hasher = newHasher();
    hasher
        .putLong(GLOBAL_LOG_HASH_SEED)
        // add some randomness here to "avoid" hash-collisions for the global-state-log
        .putLong(ThreadLocalRandom.current().nextLong())
        .putBytes(parentHash.asBytes().asReadOnlyByteBuffer());
    globals.forEach(
        (key, value) -> {
          hashKey(hasher, key);
          hasher.putBytes(value.asReadOnlyByteBuffer());
        });
    Hash hash = Hash.of(UnsafeByteOperations.unsafeWrap(hasher.hash().asBytes()));

    GlobalStateLogEntry currentEntry = fetchFromGlobalLog(ctx, parentHash);

    List<Hash> newParents =
        currentEntry != null
            ? Stream.concat(
                    Stream.of(parentHash),
                    currentEntry.getParents().stream()
                        .skip(1)
                        .limit(PARENTS_PER_COMMIT.get(config) - 1))
                .collect(Collectors.toList())
            : Collections.singletonList(parentHash);

    writeGlobalCommit(ctx, hash, newParents, globals);

    return hash;
  }

  @SuppressWarnings("UnstableApiUsage")
  private static void hashKey(Hasher hasher, Key k) {
    k.getElements().forEach(e -> hasher.putString(e, StandardCharsets.UTF_8));
  }

  protected CommitLogEntry buildIndividualCommit(
      OP_CONTEXT ctx,
      List<Hash> parentHashes,
      ByteString commitMeta,
      List<KeyWithBytes> puts,
      List<Key> unchanged,
      List<Key> deletes,
      int currentKeyListDistance)
      throws ReferenceNotFoundException {
    Hash commitHash = individualCommitHash(parentHashes, commitMeta, puts, unchanged, deletes);

    int keyListDistance = currentKeyListDistance + 1;

    CommitLogEntry entry =
        CommitLogEntry.of(
            commitHash, parentHashes, commitMeta, puts, unchanged, deletes, keyListDistance, null);

    if (keyListDistance >= KEY_LIST_INTERVAL.get(config)) {
      entry = buildKeyList(ctx, entry);
    }
    return entry;
  }

  @SuppressWarnings("UnstableApiUsage")
  private Hash individualCommitHash(
      List<Hash> parentHashes,
      ByteString commitMeta,
      List<KeyWithBytes> puts,
      List<Key> unchanged,
      List<Key> deletes) {
    Hasher hasher = newHasher();
    hasher.putLong(COMMIT_LOG_HASH_SEED);
    parentHashes.forEach(h -> hasher.putBytes(h.asBytes().asReadOnlyByteBuffer()));
    hasher.putBytes(commitMeta.asReadOnlyByteBuffer());
    puts.forEach(
        e -> {
          hashKey(hasher, e.getKey());
          hasher.putBytes(e.getValue().asReadOnlyByteBuffer());
        });
    unchanged.forEach(e -> hashKey(hasher, e));
    deletes.forEach(e -> hashKey(hasher, e));
    return Hash.of(UnsafeByteOperations.unsafeWrap(hasher.hash().asBytes()));
  }

  protected CommitLogEntry buildKeyList(OP_CONTEXT ctx, CommitLogEntry unwrittenEntry)
      throws ReferenceNotFoundException {
    // Read commit-log until the previous persisted key-list

    Hash startHash = unwrittenEntry.getParents().get(0);
    List<KeyWithType> keys = new ArrayList<>();
    keysForCommitEntry(ctx, startHash).forEach(keys::add);

    EmbeddedKeyList newKeyList = EmbeddedKeyList.of(keys);

    return ImmutableCommitLogEntry.builder()
        .from(unwrittenEntry)
        .keyListDistance(0)
        .keyList(newKeyList)
        .build();
  }

  /**
   * If the current HEAD of the target branch for a commit/transplant/merge is not equal to the
   * expected/reference HEAD, verify that there is no conflict, like keys in the operations of the
   * commit(s) contained in keys of the commits 'expectedHead (excluding) .. currentHead
   * (including)'.
   */
  protected void checkForModifiedKeysBetweenExpectedAndCurrentCommit(
      NamedRef ref,
      Optional<Hash> expectedHead,
      Set<Key> operationsKeys,
      OP_CONTEXT ctx,
      Heads currentHeads,
      List<String> mismatches)
      throws ReferenceConflictException, ReferenceNotFoundException {

    if (expectedHead.isPresent() && !expectedHead.get().equals(currentHeads.getBranchHead())) {
      boolean[] sinceSeen = new boolean[1];
      mismatches.addAll(
          checkConflictingKeysForCommit(
                  ctx, currentHeads.getBranchHead(), expectedHead.get(), operationsKeys, sinceSeen)
              .values());

      // If the expectedHead is the special value NO_ANCESTOR, which is not persisted,
      // ignore the fact that it has not been seen. Otherwise, raise a
      // ReferenceNotFoundException that the expected-hash does not exist on the target
      // branch.
      if (!sinceSeen[0] && !expectedHead.get().equals(NO_ANCESTOR)) {
        throw hashNotFound(ref, expectedHead.get());
      }
    }
  }

  protected Stream<KeyWithType> keysForCommitEntry(OP_CONTEXT ctx, Hash startHash)
      throws ReferenceNotFoundException {
    Stream<CommitLogEntry> log = commitLogFetcher(ctx, startHash);
    log = takeUntil(log, e -> e.getKeyList() != null);
    List<CommitLogEntry> list = log.collect(Collectors.toList());

    // walk the commit-logs in reverse order - starting with the last persisted key-list
    Set<KeyWithType> keys = null;
    for (int i = list.size() - 1; i >= 0; i--) {
      CommitLogEntry e = list.get(i);
      if (keys == null) {
        EmbeddedKeyList prevList = e.getKeyList();
        keys = prevList != null ? new HashSet<>(e.getKeyList().getKeys()) : new HashSet<>();
      }
      e.getPuts().stream().map(KeyWithBytes::asKeyWithType).forEach(keys::add);
      // Only have 'Key' in deletes, so map it to a 'KeyWithType' and it can be used to remove
      // the entry from 'keys'.
      e.getDeletes().stream().map(k -> KeyWithType.of(k, (byte) 0)).forEach(keys::remove);
    }

    return keys != null ? keys.stream() : Stream.empty();
  }

  /**
   * Write a new commit with a best-effort approach to prevent hash-collisions but without any
   * consistency other checks.
   */
  protected abstract void writeIndividualCommit(OP_CONTEXT ctx, CommitLogEntry entry)
      throws ReferenceConflictException;

  protected abstract void writeIndividualCommits(OP_CONTEXT ctx, List<CommitLogEntry> entries)
      throws ReferenceConflictException;

  /**
   * Write a global-commit-log with a best-effort approach to prevent id-collisions but without any
   * consistency other checks.
   */
  protected void writeGlobalCommit(
      OP_CONTEXT ctx, Hash id, List<Hash> parents, Map<Key, ByteString> puts)
      throws ReferenceConflictException {
    GlobalStateLogEntry entry = GlobalStateLogEntry.of(id, parents, puts);
    writeGlobalCommit(ctx, entry);
  }

  protected abstract void writeGlobalCommit(OP_CONTEXT ctx, GlobalStateLogEntry entry)
      throws ReferenceConflictException;

  /**
   * Check whether the commits in the range {@code sinceCommitExcluding] .. [upToCommitIncluding}
   * contain any of the given {@link Key}s.
   *
   * <p>If it takes "too long", think: requires too many reads, to reach {@code
   * sinceCommitExcluding}, the implementation shall abort and throw a {@link
   * ReferenceConflictException}..
   *
   * <p>If the implementation finds a commit that touches any of the given {@link Key}s, it must
   * throw a {@link ReferenceConflictException}. The thrown {@link ReferenceConflictException} shall
   * include as much information as possible to give users enough information, but should not
   * perform more read operations than necessary.
   */
  protected Map<Key, String> checkConflictingKeysForCommit(
      OP_CONTEXT ctx,
      Hash upToCommitIncluding,
      Hash sinceCommitExcluding,
      Set<Key> keys,
      boolean[] sinceSeen)
      throws ReferenceNotFoundException {
    Stream<CommitLogEntry> log = commitLogFetcher(ctx, upToCommitIncluding);
    log =
        takeUntil(
            log,
            e -> {
              if (e.getHash().equals(sinceCommitExcluding)) {
                sinceSeen[0] = true;
                return true;
              }
              return false;
            });

    Map<Key, String> mismatches = new HashMap<>();
    log.forEach(
        e -> {
          e.getPuts()
              .forEach(
                  a -> {
                    if (keys.contains(a.getKey())) {
                      mismatches.putIfAbsent(
                          a.getKey(), String.format("Key '%s' has put-operation.", a.getKey()));
                    }
                  });
          e.getDeletes()
              .forEach(
                  a -> {
                    if (keys.contains(a)) {
                      mismatches.putIfAbsent(a, String.format("Key '%s' has delete-operation.", a));
                    }
                  });
        });
    return mismatches;
  }

  protected Hash findCommonAncestor(
      OP_CONTEXT ctx,
      NamedRef from,
      Hash fromHead,
      BranchName toBranch,
      Hash toHead,
      boolean commonAncestorRequired)
      throws ReferenceConflictException, ReferenceNotFoundException {

    // TODO this implementation requires guardrails:
    //  max number of "to"-commits to fetch, max number of "from"-commits to fetch,
    //  both impact the cost (CPU, memory, I/O) of a merge operation.

    int fetchCount = PARENTS_PER_COMMIT.get(config);
    Iterator<Hash> toLog =
        Spliterators.iterator(
            commitLogFetcher(ctx, toHead).map(CommitLogEntry::getHash).spliterator());
    Iterator<Hash> fromLog =
        Spliterators.iterator(
            commitLogFetcher(ctx, fromHead).map(CommitLogEntry::getHash).spliterator());
    Set<Hash> toCommitHashes = new HashSet<>();
    List<Hash> fromCommitHashes = new ArrayList<>();
    while (true) {
      boolean anyFetched = false;
      for (int i = 0; i < fetchCount; i++) {
        if (toLog.hasNext()) {
          toCommitHashes.add(toLog.next());
          anyFetched = true;
        }
        if (fromLog.hasNext()) {
          fromCommitHashes.add(fromLog.next());
          anyFetched = true;
        }
      }
      if (!anyFetched) {
        if (!commonAncestorRequired) {
          return NO_ANCESTOR;
        }
        throw new ReferenceConflictException(
            String.format(
                "No common ancestor found for merge of " + "'%s' into branch '%s'",
                from.getName(), toBranch.getName()));
      }

      for (Hash f : fromCommitHashes) {
        if (toCommitHashes.contains(f)) {
          return f;
        }
      }
    }
  }

  /**
   * For merge/transplant, verifies that the given commits do not touch any of the given keys.
   *
   * @param commitsChronological list of commit-log-entries, in order of commit-operations,
   *     chronological order
   */
  protected void checkForKeyCollisions(
      Set<Key> keysTouchedOnTarget, List<CommitLogEntry> commitsChronological)
      throws ReferenceConflictException {
    Set<Key> keyCollisions = new HashSet<>();
    for (int i = commitsChronological.size() - 1; i >= 0; i--) {
      CommitLogEntry sourceCommit = commitsChronological.get(i);
      Stream.concat(
              sourceCommit.getPuts().stream().map(KeyWithBytes::getKey),
              sourceCommit.getDeletes().stream())
          .filter(keysTouchedOnTarget::contains)
          .forEach(keyCollisions::add);
    }
    if (!keyCollisions.isEmpty()) {
      throw new ReferenceConflictException(
          String.format(
              "The following keys have been changed in conflict: %s",
              keyCollisions.stream()
                  .map(k -> String.format("'%s'", k.toString()))
                  .collect(Collectors.joining(", "))));
    }
  }

  /**
   * For merge/transplant, collect the content-keys that were modified in the given list of entries.
   *
   * @param commitsReverseChronological list of commit-log-entries, in <em>reverse</em> order of
   *     commit-operations, <em>reverse</em> chronological order
   */
  protected Set<Key> collectModifiedKeys(List<CommitLogEntry> commitsReverseChronological)
      throws ReferenceNotFoundException {
    Set<Key> keysTouchedOnTarget = new HashSet<>();
    commitsReverseChronological.forEach(
        e -> {
          e.getPuts().stream().map(KeyWithBytes::getKey).forEach(keysTouchedOnTarget::add);
          e.getDeletes().forEach(keysTouchedOnTarget::remove);
        });
    return keysTouchedOnTarget;
  }

  /** For merge/transplant, applies the given commits onto the target-hash. */
  protected Hash copyCommits(
      OP_CONTEXT ctx, Hash targetHead, List<CommitLogEntry> commitsChronological)
      throws ReferenceNotFoundException {
    int parentsPerCommit = PARENTS_PER_COMMIT.get(config);

    List<Hash> parents = new ArrayList<>(parentsPerCommit);
    CommitLogEntry targetHeadCommit = fetchFromCommitLog(ctx, targetHead);
    if (targetHeadCommit != null) {
      parents.addAll(targetHeadCommit.getParents());
    }

    int keyListDistance = targetHeadCommit != null ? targetHeadCommit.getKeyListDistance() : 0;

    // Rewrite commits to transplant and store those in 'commitsToTransplantReverse'
    for (int i = commitsChronological.size() - 1; i >= 0; i--) {
      CommitLogEntry sourceCommit = commitsChronological.get(i);

      while (parents.size() > parentsPerCommit - 1) {
        parents.remove(parentsPerCommit - 1);
      }
      if (parents.isEmpty()) {
        parents.add(targetHead);
      } else {
        parents.add(0, targetHead);
      }

      CommitLogEntry newEntry =
          buildIndividualCommit(
              ctx,
              parents,
              sourceCommit.getMetadata(),
              sourceCommit.getPuts(),
              sourceCommit.getUnchanged(),
              sourceCommit.getDeletes(),
              keyListDistance);
      keyListDistance = newEntry.getKeyListDistance();

      if (!newEntry.getHash().equals(sourceCommit.getHash())) {
        commitsChronological.set(i, newEntry);
      } else {
        // Newly built CommitLogEntry is equal to the CommitLogEntry to transplant.
        // This can happen, if the commit to transplant has NO_ANCESTOR as its parent.
        commitsChronological.remove(i);
      }

      targetHead = newEntry.getHash();
    }
    return targetHead;
  }

  protected void checkExpectedGlobalStates(
      OP_CONTEXT ctx, Map<Key, ByteString> expectedStates, List<String> mismatches)
      throws ReferenceNotFoundException {
    Map<Key, ByteString> globalStates = fetchGlobalStates(ctx, expectedStates.keySet());
    for (Entry<Key, ByteString> expectedState : expectedStates.entrySet()) {
      ByteString currentState = globalStates.get(expectedState.getKey());
      if (currentState == null) {
        mismatches.add(
            String.format("No current global-state for key '%s'.", expectedState.getKey()));
      } else if (!currentState.equals(expectedState.getValue())) {
        mismatches.add(
            String.format("Mismatch in global-state for key '%s'.", expectedState.getKey()));
      }
    }
  }

  /**
   * Atomically update the global-commit-pointer to the given new-global-head, if the value in the
   * database is the given expected-global-head.
   */
  protected abstract boolean globalPointerCas(
      OP_CONTEXT ctx, GlobalStatePointer expected, GlobalStatePointer newPointer);

  /**
   * If a {@link #globalPointerCas(AutoCloseable, GlobalStatePointer, GlobalStatePointer)} failed,
   * {@link AbstractDatabaseAdapter#commit(BranchName, Optional, Map, List, Map, List, List, Set,
   * ByteString)} calls this function to remove the optimistically written data.
   *
   * <p>Implementation notes: non-transactional implementations <em>must</em> delete entries for the
   * given keys, no-op for transactional implementations.
   */
  protected abstract void cleanUpCommitCas(OP_CONTEXT ctx, Hash globalId, Set<Hash> branchCommit);

  protected static ReferenceNotFoundException hashNotFound(NamedRef ref, Hash hash) {
    return new ReferenceNotFoundException(
        String.format(
            "Could not find commit '%s' in reference '%s'.", hash.asString(), ref.getName()));
  }
}
