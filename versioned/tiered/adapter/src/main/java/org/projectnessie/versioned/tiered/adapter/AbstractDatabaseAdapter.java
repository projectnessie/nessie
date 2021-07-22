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
import java.util.concurrent.TimeUnit;
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
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;

/** Tiered-Version-Store database-adapter implementing all the required logic. */
public abstract class AbstractDatabaseAdapter<
        OP_CONTEXT extends AutoCloseable, CONFIG extends DatabaseAdapterConfig>
    implements DatabaseAdapter {

  protected final CONFIG config;

  @SuppressWarnings("UnstableApiUsage")
  public static final Hash NO_ANCESTOR =
      Hash.of(
          UnsafeByteOperations.unsafeWrap(
              newHasher().putString("empty", StandardCharsets.UTF_8).hash().asBytes()));

  protected static long GLOBAL_LOG_HASH_SEED = -3031091797576167804L;

  protected static long COMMIT_LOG_HASH_SEED = 946928273206945677L;

  protected AbstractDatabaseAdapter(CONFIG config) {
    Objects.requireNonNull(config, "config parameter must not be null");
    this.config = config;
  }

  // /////////////////////////////////////////////////////////////////////////////////////////////
  // DatabaseAdapter subclass API (protected)
  // /////////////////////////////////////////////////////////////////////////////////////////////

  protected CommitLogEntry commitAttempt(
      BranchName branch,
      Optional<Hash> expectedHead,
      Map<Key, ByteString> expectedStates,
      List<KeyWithBytes> puts,
      List<Key> unchanged,
      List<Key> deletes,
      Set<Key> operationsKeys,
      ByteString commitMetaSerialized,
      OP_CONTEXT ctx,
      Hash branchHead)
      throws ReferenceNotFoundException, ReferenceConflictException {
    List<String> mismatches = new ArrayList<>();

    // verify expected global-states
    checkExpectedGlobalStates(ctx, expectedStates, mismatches);

    checkForModifiedKeysBetweenExpectedAndCurrentCommit(
        branch, expectedHead, operationsKeys, ctx, branchHead, mismatches);

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
            newParents,
            commitMetaSerialized,
            puts,
            unchanged,
            deletes,
            currentBranchEntry != null ? currentBranchEntry.getKeyListDistance() : 0);
    writeIndividualCommit(ctx, newBranchCommit);
    return newBranchCommit;
  }

  protected Hash mergeAttempt(
      OP_CONTEXT ctx,
      NamedRef from,
      Hash fromHead,
      Optional<Hash> fromHash,
      BranchName toBranch,
      Hash toHead,
      Optional<Hash> expectedHash,
      Consumer<Hash> individualCommits,
      boolean commonAncestorRequired)
      throws ReferenceNotFoundException, ReferenceConflictException {
    // 1. ensure 'expectedHash' is a parent of HEAD-of-'toBranch'
    hashOnRef(ctx, toHead, toBranch, expectedHash);

    // 2. ensure 'fromHash' is reachable from 'from'
    fromHead = hashOnRef(ctx, fromHead, from, fromHash);

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

    // 7. Write commits

    commitsToMergeChronological.stream().map(CommitLogEntry::getHash).forEach(individualCommits);
    writeIndividualCommits(ctx, commitsToMergeChronological);
    return toHead;
  }

  protected Hash transplantAttempt(
      OP_CONTEXT ctx,
      BranchName targetBranch,
      Hash targetHead,
      Optional<Hash> expectedHash,
      NamedRef source,
      Hash sourceHead,
      List<Hash> sequenceToTransplant,
      Consumer<Hash> individualCommits)
      throws ReferenceNotFoundException, ReferenceConflictException {
    // 1. ensure 'expectedHash' is a parent of HEAD-of-'targetBranch' & collect keys
    List<CommitLogEntry> targetEntriesReverseChronological = new ArrayList<>();
    hashOnRef(ctx, targetHead, targetBranch, expectedHash, targetEntriesReverseChronological::add);
    Collections.reverse(targetEntriesReverseChronological);

    // 2. Collect modified keys.
    Set<Key> keysTouchedOnTarget = collectModifiedKeys(targetEntriesReverseChronological);

    // 3. verify last hash in 'sequenceToTransplant' is in 'source'
    Hash lastHash = sequenceToTransplant.get(sequenceToTransplant.size() - 1);
    hashOnRef(ctx, sourceHead, source, Optional.of(lastHash));

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
                    throw new IllegalArgumentException("Sequence of hashes is not contiguous.");
                  }
                  return false;
                })
            .collect(Collectors.toList());

    // 5. check for key-collisions
    checkForKeyCollisions(keysTouchedOnTarget, commitsToTransplantChronological);

    // (no need to verify the global states during a transplant)
    // 6. re-apply commits in 'sequenceToTransplant' onto 'targetBranch'
    targetHead = copyCommits(ctx, targetHead, commitsToTransplantChronological);

    // 7. Write commits

    commitsToTransplantChronological.stream()
        .map(CommitLogEntry::getHash)
        .forEach(individualCommits);
    writeIndividualCommits(ctx, commitsToTransplantChronological);
    return targetHead;
  }

  protected Stream<Diff<ByteString>> buildDiff(
      OP_CONTEXT ctx,
      NamedRef from,
      Hash fromCurrent,
      Optional<Hash> hashOnFrom,
      NamedRef to,
      Hash toCurrent,
      Optional<Hash> hashOnTo)
      throws ReferenceNotFoundException {
    // TODO this implementation works, but is definitely not the most efficient one.

    Hash fromHash = hashOnRef(ctx, fromCurrent, from, hashOnFrom);
    Hash toHash = hashOnRef(ctx, toCurrent, to, hashOnTo);

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
  }

  @SuppressWarnings("UnstableApiUsage")
  protected static Hasher newHasher() {
    return Hashing.sha256().newHasher();
  }

  protected static ReferenceConflictException hashCollisionDetected() {
    return new ReferenceConflictException("Hash collision detected");
  }

  /** Builds a {@link ReferenceNotFoundException} exception with a human-readable message. */
  protected static ReferenceNotFoundException hashNotFound(NamedRef ref, Hash hash) {
    return new ReferenceNotFoundException(
        String.format(
            "Could not find commit '%s' in reference '%s'.", hash.asString(), ref.getName()));
  }

  /** Returns the microseconds since epoch. */
  protected static long currentTimeInMicros() {
    // We only have System.currentTimeMillis() as the current wall-clock-value, but want
    // microsecond "precision" here, which is implemented by taking the microsecond-part from
    // System.nanoTime().
    long microsFraction = TimeUnit.NANOSECONDS.toMicros(System.nanoTime());
    microsFraction %= 1000L;
    return TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()) + microsFraction;
  }

  protected void verifyExpectedHash(Hash refHead, NamedRef ref, Optional<Hash> expectedHash)
      throws ReferenceConflictException, ReferenceNotFoundException {
    if (expectedHash.isPresent() && !refHead.equals(expectedHash.get())) {
      throw new ReferenceConflictException(
          String.format(
              "Named-reference '%s' is not at expected hash '%s', but at '%s'.",
              ref.getName(), expectedHash.get().asString(), refHead.asString()));
    }
  }

  @SuppressWarnings("UnstableApiUsage")
  protected static void hashKey(Hasher hasher, Key k) {
    k.getElements().forEach(e -> hasher.putString(e, StandardCharsets.UTF_8));
  }

  /**
   * Same as {@link #takeUntil(Stream, Predicate, boolean)} with the {@code includeLast == false}.
   */
  protected <T> Stream<T> takeUntil(Stream<T> s, Predicate<T> predicate) {
    return takeUntil(s, predicate, false);
  }

  /** Lets the given {@link Stream} stop when {@link Predicate predicate} returns {@code true}. */
  protected <T> Stream<T> takeUntil(Stream<T> s, Predicate<T> predicate, boolean includeLast) {
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
                  if (t && !includeLast) {
                    done = true;
                  }
                  if (!done) {
                    consumer.accept(elem);
                  }
                  if (t && includeLast) {
                    done = true;
                  }
                });
          }
        };
    return StreamSupport.stream(split, false).onClose(s::close);
  }

  protected Hash hashOnRef(OP_CONTEXT ctx, Hash knownHead, NamedRef ref, Optional<Hash> hashOnRef)
      throws ReferenceNotFoundException {
    return hashOnRef(ctx, knownHead, ref, hashOnRef, null);
  }

  protected Hash hashOnRef(
      OP_CONTEXT ctx,
      Hash knownHead,
      NamedRef ref,
      Optional<Hash> hashOnRef,
      Consumer<CommitLogEntry> commitLogVisitor)
      throws ReferenceNotFoundException {
    if (hashOnRef.isPresent()) {
      Hash suspect = hashOnRef.get();
      if (suspect.equals(NO_ANCESTOR)) {
        // If the client requests 'NO_ANCESTOR' (== beginning of time), skip the existence-check.
        if (commitLogVisitor != null) {
          commitLogFetcher(ctx, knownHead).forEach(commitLogVisitor);
        }
        return suspect;
      }

      Stream<Hash> hashes;
      if (commitLogVisitor != null) {
        hashes =
            commitLogFetcher(ctx, knownHead).peek(commitLogVisitor).map(CommitLogEntry::getHash);
      } else {
        hashes = commitLogHashFetcher(ctx, knownHead);
      }
      if (hashes.noneMatch(suspect::equals)) {
        throw hashNotFound(ref, suspect);
      }
      return suspect;
    } else {
      return knownHead;
    }
  }

  /** Load the commit-log entry for the given hash. */
  protected abstract CommitLogEntry fetchFromCommitLog(OP_CONTEXT ctx, Hash hash);

  /** Fetch multiple {@link CommitLogEntry commit-log-entries} from the commit-log. */
  protected abstract List<CommitLogEntry> fetchPageFromCommitLog(OP_CONTEXT ctx, List<Hash> hashes);

  /** Reads from the commit-log starting at the given commit-log-hash. */
  protected Stream<CommitLogEntry> commitLogFetcher(OP_CONTEXT ctx, Hash initialHash) {
    return logFetcher(ctx, initialHash, this::fetchPageFromCommitLog, CommitLogEntry::getParents);
  }

  /**
   * Like {@link #commitLogFetcher(AutoCloseable, Hash)}, but only returns the {@link Hash
   * commit-log-entry hashes}, which can be taken from {@link CommitLogEntry#getParents()}, thus no
   * need to perform a read-operation against every hash.
   */
  private Stream<Hash> commitLogHashFetcher(OP_CONTEXT ctx, Hash initialHash) {
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
   * {@link #commitLogFetcher(AutoCloseable, Hash)} or the similar implementation for the global-log
   * for non-transactional adapters.
   */
  protected <T> Stream<T> logFetcher(
      OP_CONTEXT ctx,
      Hash startAt,
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
              currentBatch = fetcher.apply(ctx, Collections.singletonList(startAt)).iterator();
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

  protected CommitLogEntry buildIndividualCommit(
      OP_CONTEXT ctx,
      List<Hash> parentHashes,
      ByteString commitMeta,
      List<KeyWithBytes> puts,
      List<Key> unchanged,
      List<Key> deletes,
      int currentKeyListDistance) {
    Hash commitHash = individualCommitHash(parentHashes, commitMeta, puts, unchanged, deletes);

    int keyListDistance = currentKeyListDistance + 1;

    CommitLogEntry entry =
        CommitLogEntry.of(
            currentTimeInMicros(),
            commitHash,
            parentHashes,
            commitMeta,
            puts,
            unchanged,
            deletes,
            keyListDistance,
            null);

    if (keyListDistance >= config.getKeyListDistance()) {
      entry = buildKeyList(ctx, entry);
    }
    return entry;
  }

  /** Calculate the hash for the contents of a {@link CommitLogEntry}. */
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

  /** Adds a complete key-list to the given {@link CommitLogEntry}, will read from the database. */
  protected CommitLogEntry buildKeyList(OP_CONTEXT ctx, CommitLogEntry unwrittenEntry) {
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
      Hash branchHead,
      List<String> mismatches)
      throws ReferenceNotFoundException {

    if (expectedHead.isPresent() && !expectedHead.get().equals(branchHead)) {
      boolean[] sinceSeen = new boolean[1];
      mismatches.addAll(
          checkConflictingKeysForCommit(
                  ctx, branchHead, expectedHead.get(), operationsKeys, sinceSeen)
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

  /** Retrieve the contents-keys and their types for the commit-log-entry with the given hash. */
  protected Stream<KeyWithType> keysForCommitEntry(OP_CONTEXT ctx, Hash hash) {
    Stream<CommitLogEntry> log = commitLogFetcher(ctx, hash);
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
   * Fetch the global-state and per-ref contents for the given {@link Key}s and {@link Hash
   * commitSha}.
   */
  protected Stream<Optional<ContentsAndState<ByteString, ByteString>>> fetchValues(
      OP_CONTEXT ctx, Hash refHead, List<Key> keys) {
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

  /** Fetches the global-state information for the given keys. */
  protected abstract Map<Key, ByteString> fetchGlobalStates(OP_CONTEXT ctx, Collection<Key> keys);

  /**
   * Write a new commit-entry with a best-effort approach to prevent hash-collisions but without any
   * other consistency checks/guarantees. Some implementations however can enforce strict
   * consistency checks/guarantees.
   */
  protected abstract void writeIndividualCommit(OP_CONTEXT ctx, CommitLogEntry entry)
      throws ReferenceConflictException;

  /**
   * Write multiple new commit-entries with a best-effort approach to prevent hash-collisions but
   * without any other consistency checks/guarantees. Some implementations however can enforce
   * strict consistency checks/guarantees.
   */
  protected abstract void writeIndividualCommits(OP_CONTEXT ctx, List<CommitLogEntry> entries)
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
      boolean[] sinceSeen) {
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

  /**
   * Finds the common-ancestor of two commit-log-entries. If no common-ancestor is found, throws a
   * {@link ReferenceConflictException} or, if {@code commonAncestorRequired} is {@code false},
   * returns {@link #NO_ANCESTOR}. Otherwise this method returns the hash of the common-ancestor.
   */
  protected Hash findCommonAncestor(
      OP_CONTEXT ctx,
      NamedRef from,
      Hash fromHead,
      BranchName toBranch,
      Hash toHead,
      boolean commonAncestorRequired)
      throws ReferenceConflictException {

    // TODO this implementation requires guardrails:
    //  max number of "to"-commits to fetch, max number of "from"-commits to fetch,
    //  both impact the cost (CPU, memory, I/O) of a merge operation.

    Iterator<Hash> toLog = Spliterators.iterator(commitLogHashFetcher(ctx, toHead).spliterator());
    Iterator<Hash> fromLog =
        Spliterators.iterator(commitLogHashFetcher(ctx, fromHead).spliterator());
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
      OP_CONTEXT ctx, Hash targetHead, List<CommitLogEntry> commitsChronological) {
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
      OP_CONTEXT ctx, Map<Key, ByteString> expectedStates, List<String> mismatches) {
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
}
