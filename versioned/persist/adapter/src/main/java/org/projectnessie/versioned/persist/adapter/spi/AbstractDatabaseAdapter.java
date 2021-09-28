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
package org.projectnessie.versioned.persist.adapter.spi;

import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.hashKey;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.hashNotFound;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.newHasher;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.randomHash;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.referenceNotFound;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.takeUntilExcludeLast;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.takeUntilIncludeLast;

import com.google.common.hash.Hasher;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
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
import java.util.Spliterators;
import java.util.Spliterators.AbstractSpliterator;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Diff;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.persist.adapter.CommitAttempt;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.ContentsAndState;
import org.projectnessie.versioned.persist.adapter.ContentsId;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapterConfig;
import org.projectnessie.versioned.persist.adapter.Difference;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitLogEntry;
import org.projectnessie.versioned.persist.adapter.ImmutableKeyList;
import org.projectnessie.versioned.persist.adapter.KeyFilterPredicate;
import org.projectnessie.versioned.persist.adapter.KeyList;
import org.projectnessie.versioned.persist.adapter.KeyListEntity;
import org.projectnessie.versioned.persist.adapter.KeyWithBytes;
import org.projectnessie.versioned.persist.adapter.KeyWithType;

/**
 * Contains all the database-independent logic for a Database-adapter.
 *
 * <p>This class does not implement everything from {@link
 * org.projectnessie.versioned.persist.adapter.DatabaseAdapter}.
 *
 * <p>Implementations must consider that production environments may use instances of this class in
 * JAX-RS {@code RequestScope}, which means that it must be very cheap to create new instances of
 * the implementations.
 *
 * <p>Managed resources like a connection-pool must be managed outside of {@link
 * AbstractDatabaseAdapter} implementations. The recommended way to "inject" such managed resources
 * into short-lived {@link AbstractDatabaseAdapter} implementations is via a special configuration
 * attribute.
 *
 * @param <OP_CONTEXT> context for each operation, so for each operation in {@link
 *     org.projectnessie.versioned.persist.adapter.DatabaseAdapter} that requires database access.
 *     For example, used to have one "borrowed" database connection per database-adapter operation.
 * @param <CONFIG> configuration interface type for the concrete implementation
 */
public abstract class AbstractDatabaseAdapter<OP_CONTEXT, CONFIG extends DatabaseAdapterConfig>
    implements DatabaseAdapter {

  protected final CONFIG config;

  @SuppressWarnings("UnstableApiUsage")
  public static final Hash NO_ANCESTOR =
      Hash.of(
          UnsafeByteOperations.unsafeWrap(
              newHasher().putString("empty", StandardCharsets.UTF_8).hash().asBytes()));

  protected static long COMMIT_LOG_HASH_SEED = 946928273206945677L;

  protected AbstractDatabaseAdapter(CONFIG config) {
    Objects.requireNonNull(config, "config parameter must not be null");
    this.config = config;
  }

  @Override
  public Hash toHash(NamedRef ref) throws ReferenceNotFoundException {
    return hashOnReference(ref, Optional.empty());
  }

  @Override
  public Hash noAncestorHash() {
    return NO_ANCESTOR;
  }

  // /////////////////////////////////////////////////////////////////////////////////////////////
  // DatabaseAdapter subclass API (protected)
  // /////////////////////////////////////////////////////////////////////////////////////////////

  /** Returns the current time in microseconds since epoch. */
  protected long commitTimeInMicros() {
    Instant instant = config.getClock().instant();
    long time = instant.getEpochSecond();
    long nano = instant.getNano();
    return TimeUnit.SECONDS.toMicros(time) + TimeUnit.NANOSECONDS.toMicros(nano);
  }

  /**
   * Logic implementation of a commit-attempt.
   *
   * @param ctx technical operation-context
   * @param commitAttempt commit parameters
   * @param branchHead current HEAD of {@code branch}
   * @param newKeyLists consumer for optimistically written {@link KeyListEntity}s
   * @return optimistically written commit-log-entry
   */
  protected CommitLogEntry commitAttempt(
      OP_CONTEXT ctx,
      long timeInMicros,
      Hash branchHead,
      CommitAttempt commitAttempt,
      Consumer<Hash> newKeyLists)
      throws ReferenceNotFoundException, ReferenceConflictException {
    List<String> mismatches = new ArrayList<>();

    // verify expected global-states
    checkExpectedGlobalStates(ctx, commitAttempt, mismatches::add);

    checkForModifiedKeysBetweenExpectedAndCurrentCommit(ctx, commitAttempt, branchHead, mismatches);

    if (!mismatches.isEmpty()) {
      throw new ReferenceConflictException(String.join("\n", mismatches));
    }

    CommitLogEntry currentBranchEntry = fetchFromCommitLog(ctx, branchHead);

    int parentsPerCommit = config.getParentsPerCommit();
    List<Hash> newParents = new ArrayList<>(parentsPerCommit);
    newParents.add(branchHead);
    if (currentBranchEntry != null) {
      List<Hash> p = currentBranchEntry.getParents();
      newParents.addAll(p.subList(0, Math.min(p.size(), parentsPerCommit - 1)));
    }

    CommitLogEntry newBranchCommit =
        buildIndividualCommit(
            ctx,
            timeInMicros,
            newParents,
            commitAttempt.getCommitMetaSerialized(),
            commitAttempt.getPuts(),
            commitAttempt.getDeletes(),
            currentBranchEntry != null ? currentBranchEntry.getKeyListDistance() : 0,
            newKeyLists);
    writeIndividualCommit(ctx, newBranchCommit);
    return newBranchCommit;
  }

  /**
   * Logic implementation of a merge-attempt.
   *
   * @param ctx technical operation context
   * @param from merge-from commit
   * @param toBranch merge-into reference with expected hash of HEAD
   * @param expectedHead if present, {@code toBranch}'s current HEAD must be equal to this value
   * @param toHead current HEAD of {@code toBranch}
   * @param branchCommits consumer for the individual commits to merge
   * @param newKeyLists consumer for optimistically written {@link KeyListEntity}s
   * @return hash of the last commit-log-entry written to {@code toBranch}
   */
  protected Hash mergeAttempt(
      OP_CONTEXT ctx,
      long timeInMicros,
      Hash from,
      BranchName toBranch,
      Optional<Hash> expectedHead,
      Hash toHead,
      Consumer<Hash> branchCommits,
      Consumer<Hash> newKeyLists)
      throws ReferenceNotFoundException, ReferenceConflictException {

    validateHashExists(ctx, from);

    // 1. ensure 'expectedHash' is a parent of HEAD-of-'toBranch'
    hashOnRef(ctx, toBranch, expectedHead, toHead);

    // 2. find nearest common-ancestor between 'from' + 'fromHash'
    Hash commonAncestor = findCommonAncestor(ctx, from, toBranch, toHead);

    // 3. Collect commit-log-entries
    List<CommitLogEntry> toEntriesReverseChronological =
        takeUntilExcludeLast(
                readCommitLogStream(ctx, toHead), e -> e.getHash().equals(commonAncestor))
            .collect(Collectors.toList());
    Collections.reverse(toEntriesReverseChronological);
    List<CommitLogEntry> commitsToMergeChronological =
        takeUntilExcludeLast(
                readCommitLogStream(ctx, from), e -> e.getHash().equals(commonAncestor))
            .collect(Collectors.toList());

    if (commitsToMergeChronological.isEmpty()) {
      // Nothing to merge, shortcut
      throw new IllegalArgumentException(
          String.format(
              "No hashes to merge from '%s' onto '%s' @ '%s'.",
              from.asString(), toBranch.getName(), toHead));
    }

    // 4. Collect modified keys.
    Set<Key> keysTouchedOnTarget = collectModifiedKeys(toEntriesReverseChronological);

    // 5. check for key-collisions
    checkForKeyCollisions(keysTouchedOnTarget, commitsToMergeChronological);

    // (no need to verify the global states during a transplant)
    // 6. re-apply commits in 'sequenceToTransplant' onto 'targetBranch'
    toHead = copyCommits(ctx, timeInMicros, toHead, commitsToMergeChronological, newKeyLists);

    // 7. Write commits

    commitsToMergeChronological.stream().map(CommitLogEntry::getHash).forEach(branchCommits);
    writeMultipleCommits(ctx, commitsToMergeChronological);
    return toHead;
  }

  /**
   * Logic implementation of a transplant-attempt.
   *
   * @param ctx technical operation context
   * @param targetBranch target reference with expected HEAD
   * @param expectedHead if present, {@code targetBranch}'s current HEAD must be equal to this value
   * @param targetHead current HEAD of {@code targetBranch}
   * @param sequenceToTransplant sequential list of commits to transplant from {@code source}
   * @param branchCommits consumer for the individual commits to merge
   * @param newKeyLists consumer for optimistically written {@link KeyListEntity}s
   * @return hash of the last commit-log-entry written to {@code targetBranch}
   */
  protected Hash transplantAttempt(
      OP_CONTEXT ctx,
      long timeInMicros,
      BranchName targetBranch,
      Optional<Hash> expectedHead,
      Hash targetHead,
      List<Hash> sequenceToTransplant,
      Consumer<Hash> branchCommits,
      Consumer<Hash> newKeyLists)
      throws ReferenceNotFoundException, ReferenceConflictException {
    if (sequenceToTransplant.isEmpty()) {
      throw new IllegalArgumentException("No hashes to transplant given.");
    }

    // 1. ensure 'expectedHash' is a parent of HEAD-of-'targetBranch' & collect keys
    List<CommitLogEntry> targetEntriesReverseChronological = new ArrayList<>();
    hashOnRef(ctx, targetHead, targetBranch, expectedHead, targetEntriesReverseChronological::add);

    // Exclude the expected-hash on the target-branch from key-collisions check
    if (!targetEntriesReverseChronological.isEmpty()
        && expectedHead.isPresent()
        && targetEntriesReverseChronological.get(0).getHash().equals(expectedHead.get())) {
      targetEntriesReverseChronological.remove(0);
    }
    Collections.reverse(targetEntriesReverseChronological);

    // 2. Collect modified keys.
    Set<Key> keysTouchedOnTarget = collectModifiedKeys(targetEntriesReverseChronological);

    // 4. ensure 'sequenceToTransplant' is sequential
    int[] index = new int[] {sequenceToTransplant.size() - 1};
    Hash lastHash = sequenceToTransplant.get(sequenceToTransplant.size() - 1);
    List<CommitLogEntry> commitsToTransplantChronological =
        takeUntilExcludeLast(
                readCommitLogStream(ctx, lastHash),
                e -> {
                  int i = index[0]--;
                  if (i == -1) {
                    return true;
                  }
                  if (!e.getHash().equals(sequenceToTransplant.get(i))) {
                    throw new IllegalArgumentException("Sequence of hashes is not contiguous.");
                  }
                  return false;
                })
            .collect(Collectors.toList());

    // 5. check for key-collisions
    checkForKeyCollisions(keysTouchedOnTarget, commitsToTransplantChronological);

    // (no need to verify the global states during a transplant)
    // 6. re-apply commits in 'sequenceToTransplant' onto 'targetBranch'
    targetHead =
        copyCommits(ctx, timeInMicros, targetHead, commitsToTransplantChronological, newKeyLists);

    // 7. Write commits

    commitsToTransplantChronological.stream().map(CommitLogEntry::getHash).forEach(branchCommits);
    writeMultipleCommits(ctx, commitsToTransplantChronological);
    return targetHead;
  }

  /**
   * Compute the diff between two references.
   *
   * @param ctx technical operation context
   * @param from "from" reference to compute the difference from, appears on the "from" side in
   *     {@link Diff} with hash in {@code from} to compute the diff for, must exist in {@code from}
   * @param to "to" reference to compute the difference from, appears on the "to" side in {@link
   *     Diff} with hash in {@code to} to compute the diff for, must exist in {@code to}
   * @param keyFilter optional filter on key + contents-id + contents-type
   * @return computed difference
   */
  protected Stream<Difference> buildDiff(
      OP_CONTEXT ctx, Hash from, Hash to, KeyFilterPredicate keyFilter)
      throws ReferenceNotFoundException {
    // TODO this implementation works, but is definitely not the most efficient one.

    Set<Key> allKeys = new HashSet<>();
    try (Stream<Key> s = keysForCommitEntry(ctx, from, keyFilter).map(KeyWithType::getKey)) {
      s.forEach(allKeys::add);
    }
    try (Stream<Key> s = keysForCommitEntry(ctx, to, keyFilter).map(KeyWithType::getKey)) {
      s.forEach(allKeys::add);
    }

    if (allKeys.isEmpty()) {
      // no keys, shortcut
      return Stream.empty();
    }

    List<Key> allKeysList = new ArrayList<>(allKeys);
    Map<Key, ContentsAndState<ByteString>> fromValues =
        fetchValues(ctx, from, allKeysList, keyFilter);
    Map<Key, ContentsAndState<ByteString>> toValues = fetchValues(ctx, to, allKeysList, keyFilter);

    Function<ContentsAndState<ByteString>, Optional<ByteString>> valToContents =
        cs -> cs != null ? Optional.of(cs.getRefState()) : Optional.empty();

    return IntStream.range(0, allKeys.size())
        .mapToObj(allKeysList::get)
        .map(
            k -> {
              ContentsAndState<ByteString> fromVal = fromValues.get(k);
              ContentsAndState<ByteString> toVal = toValues.get(k);
              Optional<ByteString> f = valToContents.apply(fromVal);
              Optional<ByteString> t = valToContents.apply(toVal);
              if (f.equals(t)) {
                return null;
              }
              Optional<ByteString> g =
                  Optional.ofNullable(
                      fromVal != null
                          ? fromVal.getGlobalState()
                          : (toVal != null ? toVal.getGlobalState() : null));
              return Difference.of(k, g, f, t);
            })
        .filter(Objects::nonNull);
  }

  /**
   * Convenience for {@link #hashOnRef(Object, Hash, NamedRef, Optional, Consumer) hashOnRef(ctx,
   * knownHead, ref.getReference(), ref.getHashOnReference(), null)}.
   */
  protected Hash hashOnRef(
      OP_CONTEXT ctx, NamedRef reference, Optional<Hash> hashOnRef, Hash knownHead)
      throws ReferenceNotFoundException {
    return hashOnRef(ctx, knownHead, reference, hashOnRef, null);
  }

  /**
   * Ensures that {@code ref} exists and that the hash in {@code hashOnRef} exists in that
   * reference.
   *
   * @param ctx technical operation context
   * @param ref reference that must contain {@code hashOnRef}
   * @param knownHead current HEAD of {@code ref}
   * @param hashOnRef hash to verify whether it exists in {@code ref}
   * @param commitLogVisitor optional consumer that will receive all visited {@link
   *     CommitLogEntry}s, can be {@code null}.
   * @return value of {@code hashOnRef} or, if {@code hashOnRef} is empty, {@code knownHead}
   * @throws ReferenceNotFoundException if either {@code ref} does not exist or {@code hashOnRef}
   *     does not exist on {@code ref}
   */
  protected Hash hashOnRef(
      OP_CONTEXT ctx,
      Hash knownHead,
      NamedRef ref,
      Optional<Hash> hashOnRef,
      Consumer<CommitLogEntry> commitLogVisitor)
      throws ReferenceNotFoundException {
    if (hashOnRef.isPresent()) {
      Hash suspect = hashOnRef.get();

      // If the client requests 'NO_ANCESTOR' (== beginning of time), skip the existence-check.
      if (suspect.equals(NO_ANCESTOR)) {
        if (commitLogVisitor != null) {
          readCommitLogStream(ctx, knownHead).forEach(commitLogVisitor);
        }
        return suspect;
      }

      Stream<Hash> hashes;
      if (commitLogVisitor != null) {
        hashes =
            readCommitLogStream(ctx, knownHead).peek(commitLogVisitor).map(CommitLogEntry::getHash);
      } else {
        hashes = readCommitLogHashesStream(ctx, knownHead);
      }
      if (hashes.noneMatch(suspect::equals)) {
        throw hashNotFound(ref, suspect);
      }
      return suspect;
    } else {
      return knownHead;
    }
  }

  protected void validateHashExists(OP_CONTEXT ctx, Hash hash) throws ReferenceNotFoundException {
    if (!NO_ANCESTOR.equals(hash) && fetchFromCommitLog(ctx, hash) == null) {
      throw referenceNotFound(hash);
    }
  }

  /** Load the commit-log entry for the given hash, return {@code null}, if not found. */
  protected abstract CommitLogEntry fetchFromCommitLog(OP_CONTEXT ctx, Hash hash);

  /**
   * Fetch multiple {@link CommitLogEntry commit-log-entries} from the commit-log. The returned list
   * must have exactly as many elements as in the parameter {@code hashes}. Non-existing hashes are
   * returned as {@code null}.
   */
  protected abstract List<CommitLogEntry> fetchPageFromCommitLog(OP_CONTEXT ctx, List<Hash> hashes);

  /** Reads from the commit-log starting at the given commit-log-hash. */
  protected Stream<CommitLogEntry> readCommitLogStream(OP_CONTEXT ctx, Hash initialHash)
      throws ReferenceNotFoundException {
    if (NO_ANCESTOR.equals(initialHash)) {
      return Stream.empty();
    }
    CommitLogEntry initial = fetchFromCommitLog(ctx, initialHash);
    if (initial == null) {
      throw referenceNotFound(initialHash);
    }
    return logFetcher(ctx, initial, this::fetchPageFromCommitLog, CommitLogEntry::getParents);
  }

  /**
   * Like {@link #readCommitLogStream(Object, Hash)}, but only returns the {@link Hash
   * commit-log-entry hashes}, which can be taken from {@link CommitLogEntry#getParents()}, thus no
   * need to perform a read-operation against every hash.
   */
  private Stream<Hash> readCommitLogHashesStream(OP_CONTEXT ctx, Hash initialHash) {
    return logFetcher(
        ctx,
        initialHash,
        (c, hashes) -> hashes,
        hash -> {
          CommitLogEntry entry = fetchFromCommitLog(ctx, hash);
          if (entry == null) {
            return Collections.emptyList();
          }
          return entry.getParents();
        });
  }

  /**
   * Constructs a {@link Stream} of entries for either the global-state-log or a commit-log. Use
   * {@link #readCommitLogStream(Object, Hash)} or the similar implementation for the global-log for
   * non-transactional adapters.
   */
  protected <T> Stream<T> logFetcher(
      OP_CONTEXT ctx,
      T initial,
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
              currentBatch = Collections.singletonList(initial).iterator();
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

  /**
   * Builds a {@link CommitLogEntry} using the given values. This function also includes a {@link
   * KeyList}, if triggered by the values of {@code currentKeyListDistance} and {@link
   * DatabaseAdapterConfig#getKeyListDistance()}, so read operations may happen.
   */
  protected CommitLogEntry buildIndividualCommit(
      OP_CONTEXT ctx,
      long timeInMicros,
      List<Hash> parentHashes,
      ByteString commitMeta,
      List<KeyWithBytes> puts,
      List<Key> deletes,
      int currentKeyListDistance,
      Consumer<Hash> newKeyLists)
      throws ReferenceNotFoundException {
    Hash commitHash = individualCommitHash(parentHashes, commitMeta, puts, deletes);

    int keyListDistance = currentKeyListDistance + 1;

    CommitLogEntry entry =
        CommitLogEntry.of(
            timeInMicros,
            commitHash,
            parentHashes,
            commitMeta,
            puts,
            deletes,
            keyListDistance,
            null,
            Collections.emptyList());

    if (keyListDistance >= config.getKeyListDistance()) {
      entry = buildKeyList(ctx, entry, newKeyLists);
    }
    return entry;
  }

  /** Calculate the hash for the contents of a {@link CommitLogEntry}. */
  @SuppressWarnings("UnstableApiUsage")
  protected Hash individualCommitHash(
      List<Hash> parentHashes, ByteString commitMeta, List<KeyWithBytes> puts, List<Key> deletes) {
    Hasher hasher = newHasher();
    hasher.putLong(COMMIT_LOG_HASH_SEED);
    parentHashes.forEach(h -> hasher.putBytes(h.asBytes().asReadOnlyByteBuffer()));
    hasher.putBytes(commitMeta.asReadOnlyByteBuffer());
    puts.forEach(
        e -> {
          hashKey(hasher, e.getKey());
          hasher.putString(e.getContentsId().getId(), StandardCharsets.UTF_8);
          hasher.putBytes(e.getValue().asReadOnlyByteBuffer());
        });
    deletes.forEach(e -> hashKey(hasher, e));
    return Hash.of(UnsafeByteOperations.unsafeWrap(hasher.hash().asBytes()));
  }

  /** Helper object for {@link #buildKeyList(Object, CommitLogEntry, Consumer)}. */
  private static class KeyListBuildState {
    final ImmutableCommitLogEntry.Builder newCommitEntry;
    /** Builder for {@link CommitLogEntry#getKeyList()}. */
    ImmutableKeyList.Builder embeddedBuilder = ImmutableKeyList.builder();
    /** Builder for {@link KeyListEntity}. */
    ImmutableKeyList.Builder currentKeyList;
    /** Already built {@link KeyListEntity}s. */
    List<KeyListEntity> newKeyListEntities = new ArrayList<>();

    /** Flag whether {@link CommitLogEntry#getKeyList()} is being filled. */
    boolean embedded = true;

    /** Current size of either the {@link CommitLogEntry} or current {@link KeyListEntity}. */
    int currentSize;

    KeyListBuildState(int initialSize, ImmutableCommitLogEntry.Builder newCommitEntry) {
      this.currentSize = initialSize;
      this.newCommitEntry = newCommitEntry;
    }

    void finishKeyListEntity() {
      Hash id = randomHash();
      newKeyListEntities.add(KeyListEntity.of(id, currentKeyList.build()));
      newCommitEntry.addKeyListsIds(id);
    }

    void newKeyListEntity() {
      currentSize = 0;
      currentKeyList = ImmutableKeyList.builder();
    }

    void addToKeyListEntity(KeyWithType keyWithType, int keyTypeSize) {
      currentSize += keyTypeSize;
      currentKeyList.addKeys(keyWithType);
    }

    void addToEmbedded(KeyWithType keyWithType, int keyTypeSize) {
      currentSize += keyTypeSize;
      embeddedBuilder.addKeys(keyWithType);
    }
  }

  /**
   * Adds a complete key-list to the given {@link CommitLogEntry}, will read from the database.
   *
   * <p>The implementation fills {@link CommitLogEntry#getKeyList()} with the most recently updated
   * {@link Key}s.
   *
   * <p>If the calculated size of the database-object/row gets larger than {@link
   * DatabaseAdapterConfig#getMaxKeyListSize()}, the next {@link Key}s will be added to new {@link
   * KeyListEntity}s, each with a maximum size of {@link DatabaseAdapterConfig#getMaxKeyListSize()}.
   *
   * <p>The current implementation fetches all keys and "blindly" populated {@link
   * CommitLogEntry#getKeyList()} and nested {@link KeyListEntity} via {@link
   * CommitLogEntry#getKeyListsIds()}. So this implementation does not yet reuse previous {@link
   * KeyListEntity}s. A follow-up improvement should check if already existing {@link
   * KeyListEntity}s contain the same keys. This proposed optimization should be accompanied by an
   * optimized read of the keys: for example, if the set of changed keys only affects {@link
   * CommitLogEntry#getKeyList()} but not the keys via {@link KeyListEntity}, it is just unnecessary
   * to both read and re-write those rows for {@link KeyListEntity}.
   */
  protected CommitLogEntry buildKeyList(
      OP_CONTEXT ctx, CommitLogEntry unwrittenEntry, Consumer<Hash> newKeyLists)
      throws ReferenceNotFoundException {
    // Read commit-log until the previous persisted key-list

    Hash startHash = unwrittenEntry.getParents().get(0);

    // Return the new commit-log-entry with the complete-key-list
    ImmutableCommitLogEntry.Builder newCommitEntry =
        ImmutableCommitLogEntry.builder().from(unwrittenEntry).keyListDistance(0);

    KeyListBuildState buildState =
        new KeyListBuildState(entitySize(unwrittenEntry), newCommitEntry);

    keysForCommitEntry(ctx, startHash)
        .forEach(
            keyWithType -> {
              int keyTypeSize = entitySize(keyWithType);
              if (buildState.embedded) {
                // filling the embedded key-list in CommitLogEntry

                if (buildState.currentSize + keyTypeSize < config.getMaxKeyListSize()) {
                  // CommitLogEntry.keyList still has room
                  buildState.addToEmbedded(keyWithType, keyTypeSize);
                } else {
                  // CommitLogEntry.keyList is "full", switch to the first KeyListEntity
                  buildState.embedded = false;
                  buildState.newKeyListEntity();
                  buildState.addToKeyListEntity(keyWithType, keyTypeSize);
                }
              } else {
                // filling linked key-lists via CommitLogEntry.keyListIds

                if (buildState.currentSize + keyTypeSize > config.getMaxKeyListSize()) {
                  // current KeyListEntity is "full", switch to a new one
                  buildState.finishKeyListEntity();
                  buildState.newKeyListEntity();
                }
                buildState.addToKeyListEntity(keyWithType, keyTypeSize);
              }
            });

    // If there's an "unfinished" KeyListEntity, build it.
    if (buildState.currentKeyList != null) {
      buildState.finishKeyListEntity();
    }

    // Inform the (CAS)-op-loop about the IDs of the KeyListEntities being optimistically written.
    buildState.newKeyListEntities.stream().map(KeyListEntity::getId).forEach(newKeyLists);

    // Write the new KeyListEntities
    if (!buildState.newKeyListEntities.isEmpty()) {
      writeKeyListEntities(ctx, buildState.newKeyListEntities);
    }

    // Return the new commit-log-entry with the complete-key-list
    return newCommitEntry.keyList(buildState.embeddedBuilder.build()).build();
  }

  /** Calculate the expected size of the given {@link CommitLogEntry} in the database. */
  protected abstract int entitySize(CommitLogEntry entry);

  /** Calculate the expected size of the given {@link CommitLogEntry} in the database. */
  protected abstract int entitySize(KeyWithType entry);

  /**
   * If the current HEAD of the target branch for a commit/transplant/merge is not equal to the
   * expected/reference HEAD, verify that there is no conflict, like keys in the operations of the
   * commit(s) contained in keys of the commits 'expectedHead (excluding) .. currentHead
   * (including)'.
   */
  protected void checkForModifiedKeysBetweenExpectedAndCurrentCommit(
      OP_CONTEXT ctx, CommitAttempt commitAttempt, Hash branchHead, List<String> mismatches)
      throws ReferenceNotFoundException {

    if (commitAttempt.getExpectedHead().isPresent()) {
      Hash expectedHead = commitAttempt.getExpectedHead().get();
      if (!expectedHead.equals(branchHead)) {
        Set<Key> operationKeys = new HashSet<>();
        operationKeys.addAll(commitAttempt.getDeletes());
        operationKeys.addAll(commitAttempt.getUnchanged());
        commitAttempt.getPuts().stream().map(KeyWithBytes::getKey).forEach(operationKeys::add);

        boolean sinceSeen =
            checkConflictingKeysForCommit(
                ctx, branchHead, expectedHead, operationKeys, mismatches::add);

        // If the expectedHead is the special value NO_ANCESTOR, which is not persisted,
        // ignore the fact that it has not been seen. Otherwise, raise a
        // ReferenceNotFoundException that the expected-hash does not exist on the target
        // branch.
        if (!sinceSeen && !expectedHead.equals(NO_ANCESTOR)) {
          throw hashNotFound(commitAttempt.getCommitToBranch(), expectedHead);
        }
      }
    }
  }

  /** Retrieve the contents-keys and their types for the commit-log-entry with the given hash. */
  protected Stream<KeyWithType> keysForCommitEntry(
      OP_CONTEXT ctx, Hash hash, KeyFilterPredicate keyFilter) throws ReferenceNotFoundException {
    return keysForCommitEntry(ctx, hash)
        .filter(kt -> keyFilter.check(kt.getKey(), kt.getContentsId(), kt.getType()));
  }

  /** Retrieve the contents-keys and their types for the commit-log-entry with the given hash. */
  protected Stream<KeyWithType> keysForCommitEntry(OP_CONTEXT ctx, Hash hash)
      throws ReferenceNotFoundException {
    // walk the commit-logs in reverse order - starting with the last persisted key-list

    Set<Key> seen = new HashSet<>();
    Stream<CommitLogEntry> log = readCommitLogStream(ctx, hash);
    log = takeUntilIncludeLast(log, e -> e.getKeyList() != null);
    return log.flatMap(
        e -> {

          // Add CommitLogEntry.deletes to "seen" so these keys won't be returned
          seen.addAll(e.getDeletes());

          // Return from CommitLogEntry.puts first
          Stream<KeyWithType> stream =
              e.getPuts().stream()
                  .filter(kt -> seen.add(kt.getKey()))
                  .map(KeyWithBytes::asKeyWithType);

          if (e.getKeyList() != null) {

            // Return from CommitLogEntry.keyList after the keys in CommitLogEntry.puts
            Stream<KeyWithType> embedded =
                e.getKeyList().getKeys().stream().filter(kt -> seen.add(kt.getKey()));

            stream = Stream.concat(stream, embedded);

            if (!e.getKeyListsIds().isEmpty()) {
              // If there are nested key-lists, retrieve those and add the keys from these
              stream =
                  Stream.concat(
                      stream,
                      // "lazily" fetch key-lists
                      Stream.of(e.getKeyListsIds())
                          .flatMap(ids -> fetchKeyLists(ctx, ids))
                          .map(KeyListEntity::getKeys)
                          .flatMap(k -> k.getKeys().stream())
                          .filter(kt -> seen.add(kt.getKey())));
            }
          }

          return stream;
        });
  }

  /**
   * Fetch the global-state and per-ref contents for the given {@link Key}s and {@link Hash
   * commitSha}. Non-existing keys must not be present in the returned map.
   */
  protected Map<Key, ContentsAndState<ByteString>> fetchValues(
      OP_CONTEXT ctx, Hash refHead, List<Key> keys, KeyFilterPredicate keyFilter)
      throws ReferenceNotFoundException {
    Set<Key> remainingKeys = new HashSet<>(keys);

    Map<Key, ByteString> nonGlobal = new HashMap<>();
    Map<Key, ContentsId> keyToContentsIds = new HashMap<>();
    Set<ContentsId> contentsIds = new HashSet<>();
    try (Stream<CommitLogEntry> log =
        takeUntilExcludeLast(readCommitLogStream(ctx, refHead), e -> remainingKeys.isEmpty())) {
      log.peek(entry -> entry.getDeletes().forEach(remainingKeys::remove))
          .flatMap(entry -> entry.getPuts().stream())
          .filter(put -> remainingKeys.remove(put.getKey()))
          .filter(put -> keyFilter.check(put.getKey(), put.getContentsId(), put.getType()))
          .forEach(
              put -> {
                nonGlobal.put(put.getKey(), put.getValue());
                keyToContentsIds.put(put.getKey(), put.getContentsId());
                contentsIds.add(put.getContentsId());
              });
    }

    Map<ContentsId, ByteString> globals = fetchGlobalStates(ctx, contentsIds);

    return nonGlobal.entrySet().stream()
        .collect(
            Collectors.toMap(
                Entry::getKey,
                e ->
                    ContentsAndState.of(
                        e.getValue(), globals.get(keyToContentsIds.get(e.getKey())))));
  }

  /**
   * Fetches the global-state information for the given content-ids.
   *
   * @param ctx technical context
   * @param contentIds the contents-ids to fetch
   * @return map of content-id to state
   */
  protected abstract Map<ContentsId, ByteString> fetchGlobalStates(
      OP_CONTEXT ctx, Set<ContentsId> contentIds) throws ReferenceNotFoundException;

  protected abstract Stream<KeyListEntity> fetchKeyLists(OP_CONTEXT ctx, List<Hash> keyListsIds);

  /**
   * Write a new commit-entry, the given commit entry is to be persisted as is. All values of the
   * given {@link CommitLogEntry} can be considered valid and consistent.
   *
   * <p>Implementations however can enforce strict consistency checks/guarantees, like a best-effort
   * approach to prevent hash-collisions but without any other consistency checks/guarantees.
   */
  protected abstract void writeIndividualCommit(OP_CONTEXT ctx, CommitLogEntry entry)
      throws ReferenceConflictException;

  /**
   * Write multiple new commit-entries, the given commit entries are to be persisted as is. All
   * values of the * given {@link CommitLogEntry} can be considered valid and consistent.
   *
   * <p>Implementations however can enforce strict consistency checks/guarantees, like a best-effort
   * approach to prevent hash-collisions but without any other consistency checks/guarantees.
   */
  protected abstract void writeMultipleCommits(OP_CONTEXT ctx, List<CommitLogEntry> entries)
      throws ReferenceConflictException;

  protected abstract void writeKeyListEntities(
      OP_CONTEXT ctx, List<KeyListEntity> newKeyListEntities);

  /**
   * Check whether the commits in the range {@code sinceCommitExcluding] .. [upToCommitIncluding}
   * contain any of the given {@link Key}s.
   *
   * <p>Conflicts are reported via {@code mismatches}.
   */
  protected boolean checkConflictingKeysForCommit(
      OP_CONTEXT ctx,
      Hash upToCommitIncluding,
      Hash sinceCommitExcluding,
      Set<Key> keys,
      Consumer<String> mismatches)
      throws ReferenceNotFoundException {
    boolean[] sinceSeen = new boolean[1];

    Stream<CommitLogEntry> log = readCommitLogStream(ctx, upToCommitIncluding);
    log =
        takeUntilExcludeLast(
            log,
            e -> {
              if (e.getHash().equals(sinceCommitExcluding)) {
                sinceSeen[0] = true;
                return true;
              }
              return false;
            });

    Set<Key> handled = new HashSet<>();
    log.forEach(
        e -> {
          e.getPuts()
              .forEach(
                  a -> {
                    if (keys.contains(a.getKey()) && handled.add(a.getKey())) {
                      mismatches.accept(
                          String.format(
                              "Key '%s' has conflicting put-operation from another commit.",
                              a.getKey()));
                    }
                  });
          e.getDeletes()
              .forEach(
                  a -> {
                    if (keys.contains(a) && handled.add(a)) {
                      mismatches.accept(
                          String.format(
                              "Key '%s' has conflicting delete-operation from another commit.", a));
                    }
                  });
        });

    return sinceSeen[0];
  }

  /**
   * Finds the common-ancestor of two commit-log-entries. If no common-ancestor is found, throws a
   * {@link ReferenceConflictException} or. Otherwise this method returns the hash of the
   * common-ancestor.
   */
  protected Hash findCommonAncestor(OP_CONTEXT ctx, Hash from, NamedRef toBranch, Hash toHead)
      throws ReferenceConflictException {

    // TODO this implementation requires guardrails:
    //  max number of "to"-commits to fetch, max number of "from"-commits to fetch,
    //  both impact the cost (CPU, memory, I/O) of a merge operation.

    Iterator<Hash> toLog =
        Spliterators.iterator(readCommitLogHashesStream(ctx, toHead).spliterator());
    Iterator<Hash> fromLog =
        Spliterators.iterator(readCommitLogHashesStream(ctx, from).spliterator());
    Set<Hash> toCommitHashes = new HashSet<>();
    List<Hash> fromCommitHashes = new ArrayList<>();
    while (true) {
      boolean anyFetched = false;
      for (int i = 0; i < config.getParentsPerCommit(); i++) {
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
        throw new ReferenceConflictException(
            String.format(
                "No common ancestor found for merge of '%s' into branch '%s'",
                from, toBranch.getName()));
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
  protected Set<Key> collectModifiedKeys(List<CommitLogEntry> commitsReverseChronological) {
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
      OP_CONTEXT ctx,
      long timeInMicros,
      Hash targetHead,
      List<CommitLogEntry> commitsChronological,
      Consumer<Hash> newKeyLists)
      throws ReferenceNotFoundException {
    int parentsPerCommit = config.getParentsPerCommit();

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
              timeInMicros,
              parents,
              sourceCommit.getMetadata(),
              sourceCommit.getPuts(),
              sourceCommit.getDeletes(),
              keyListDistance,
              newKeyLists);
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

  /**
   * Verifies that the current global-states match the {@code expectedStates}, produces human
   * readable messages for the violations.
   */
  protected void checkExpectedGlobalStates(
      OP_CONTEXT ctx, CommitAttempt commitAttempt, Consumer<String> mismatches)
      throws ReferenceNotFoundException {
    Map<ContentsId, ByteString> globalStates =
        fetchGlobalStates(ctx, commitAttempt.getExpectedStates().keySet());
    for (Entry<ContentsId, Optional<ByteString>> expectedState :
        commitAttempt.getExpectedStates().entrySet()) {
      ByteString currentState = globalStates.get(expectedState.getKey());
      if (currentState == null) {
        if (expectedState.getValue().isPresent()) {
          mismatches.accept(
              String.format(
                  "No current global-state for contents-id '%s'.", expectedState.getKey()));
        }
      } else {
        if (!expectedState.getValue().isPresent()) {
          // This happens, when a table's being created on a branch, but that table already exists.
          mismatches.accept(
              String.format(
                  "Global-state for contents-id '%s' already exists.", expectedState.getKey()));
        } else if (!currentState.equals(expectedState.getValue().get())) {
          mismatches.accept(
              String.format(
                  "Mismatch in global-state for contents-id '%s'.", expectedState.getKey()));
        }
      }
    }
  }
}
