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

import com.google.common.hash.Hasher;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import java.nio.charset.StandardCharsets;
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
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.Spliterators.AbstractSpliterator;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
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
import org.projectnessie.versioned.persist.adapter.DatabaseAdapterConfig;
import org.projectnessie.versioned.persist.adapter.EmbeddedKeyList;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitLogEntry;
import org.projectnessie.versioned.persist.adapter.ImmutableEmbeddedKeyList;
import org.projectnessie.versioned.persist.adapter.KeyFilterPredicate;
import org.projectnessie.versioned.persist.adapter.KeyWithBytes;
import org.projectnessie.versioned.persist.adapter.KeyWithContentsIdAndBytes;
import org.projectnessie.versioned.persist.adapter.KeyWithType;

/**
 * Tiered-Version-Store database-adapter implementing all the required logic.
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
    extends DatabaseAdapterUtil {

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

  @Override
  public Stream<KeyWithBytes> entries(
      NamedRef ref, Optional<Hash> hashOnRef, KeyFilterPredicate keyFilter)
      throws ReferenceNotFoundException {
    throw new UnsupportedOperationException("entries() is not used anywhere");
  }

  // /////////////////////////////////////////////////////////////////////////////////////////////
  // DatabaseAdapter subclass API (protected)
  // /////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Logic implementation of a commit-attempt.
   *
   * @param ctx technical operation-context
   * @param commitAttempt commit parameters
   * @param branchHead current HEAD of {@code branch}
   * @return optimistically written commit-log-entry
   */
  protected CommitLogEntry commitAttempt(
      OP_CONTEXT ctx, Hash branchHead, CommitAttempt commitAttempt)
      throws ReferenceNotFoundException, ReferenceConflictException {
    List<String> mismatches = new ArrayList<>();

    // verify expected global-states
    checkExpectedGlobalStates(ctx, commitAttempt, mismatches::add);

    checkForModifiedKeysBetweenExpectedAndCurrentCommit(commitAttempt, ctx, branchHead, mismatches);

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
            commitAttempt.getCommitMetaSerialized(),
            commitAttempt.getPuts().stream()
                .map(
                    kb ->
                        KeyWithContentsIdAndBytes.of(
                            kb.getKey(),
                            commitAttempt.getKeyToContentsId().getOrDefault(kb.getKey(), ""),
                            kb.getType(),
                            kb.getValue()))
                .collect(Collectors.toList()),
            commitAttempt.getUnchanged(),
            commitAttempt.getDeletes(),
            currentBranchEntry != null ? currentBranchEntry.getKeyListDistance() : 0);
    writeIndividualCommit(ctx, newBranchCommit);
    return newBranchCommit;
  }

  /**
   * Logic implementation of a merge-attempt.
   *
   * @param ctx technical operation context
   * @param from merge-from reference
   * @param fromHead current HEAD of {@code from}
   * @param fromHash known HEAD of {@code from}
   * @param toBranch merge-into reference
   * @param toHead current HEAD of {@code toBranch}
   * @param expectedHash known HEAD of {@code toBranch}
   * @param individualCommits consumer for the individual commits to merge
   * @return hash of the last commit-log-entry written to {@code toBranch}
   */
  protected Hash mergeAttempt(
      OP_CONTEXT ctx,
      NamedRef from,
      Hash fromHead,
      Optional<Hash> fromHash,
      BranchName toBranch,
      Hash toHead,
      Optional<Hash> expectedHash,
      Consumer<Hash> individualCommits)
      throws ReferenceNotFoundException, ReferenceConflictException {
    // 1. ensure 'expectedHash' is a parent of HEAD-of-'toBranch'
    hashOnRef(ctx, toBranch, toHead, expectedHash);

    // 2. ensure 'fromHash' is reachable from 'from'
    fromHead = hashOnRef(ctx, from, fromHead, fromHash);

    // 3. find nearest common-ancestor between 'from' + 'fromHash'
    Hash commonAncestor = findCommonAncestor(ctx, from, fromHead, toBranch, toHead);

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

  /**
   * Logic implementation of a transplant-attempt.
   *
   * @param ctx technical operation context
   * @param targetBranch target reference
   * @param targetHead current HEAD of {@code targetBranch}
   * @param expectedHash known HEAD of {@code targetBranch}
   * @param source source reference containing the commits in {@code sequenceToTransplant}
   * @param sourceHead current HEAD of {@code source}
   * @param sequenceToTransplant sequential list of commits to transplant from {@code source}
   * @param individualCommits consumer for the individual commits to merge
   * @return hash of the last commit-log-entry written to {@code targetBranch}
   */
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
    hashOnRef(ctx, source, sourceHead, Optional.of(lastHash));

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

  /**
   * Compute the diff between two references.
   *
   * @param ctx technical operation context
   * @param from "from" reference to compute the difference from, appears on the "from" side in
   *     {@link Diff}
   * @param fromHead current HEAD of {@code from}
   * @param hashOnFrom hash in {@code from} to compute the diff for, must exist in {@code from}
   * @param to "to" reference to compute the difference from, appears on the "to" side in {@link
   *     Diff}
   * @param toHead current HEAD of {@code to}
   * @param hashOnTo hash in {@code to} to compute the diff for, must exist in {@code to}
   * @param keyFilter optional filter on key + contents-id + contents-type
   * @return computed difference
   */
  protected Stream<Diff<ByteString>> buildDiff(
      OP_CONTEXT ctx,
      NamedRef from,
      Hash fromHead,
      Optional<Hash> hashOnFrom,
      NamedRef to,
      Hash toHead,
      Optional<Hash> hashOnTo,
      KeyFilterPredicate keyFilter)
      throws ReferenceNotFoundException {
    // TODO this implementation works, but is definitely not the most efficient one.

    Hash fromHash = hashOnRef(ctx, from, fromHead, hashOnFrom);
    Hash toHash = hashOnRef(ctx, to, toHead, hashOnTo);

    Set<Key> allKeys = new HashSet<>();
    try (Stream<Key> s = keysForCommitEntry(ctx, fromHash, keyFilter).map(KeyWithType::getKey)) {
      s.forEach(allKeys::add);
    }
    try (Stream<Key> s = keysForCommitEntry(ctx, toHash, keyFilter).map(KeyWithType::getKey)) {
      s.forEach(allKeys::add);
    }

    List<Key> allKeysList = new ArrayList<>(allKeys);
    Map<Key, ContentsAndState<ByteString>> fromValues =
        fetchValues(ctx, fromHash, allKeysList, keyFilter);
    Map<Key, ContentsAndState<ByteString>> toValues =
        fetchValues(ctx, toHash, allKeysList, keyFilter);

    Function<ContentsAndState<ByteString>, Optional<ByteString>> valToContents =
        cs -> cs != null ? Optional.of(cs.getRefState()) : Optional.empty();

    return IntStream.range(0, allKeys.size())
        .mapToObj(allKeysList::get)
        .map(
            k -> {
              Optional<ByteString> f = valToContents.apply(fromValues.get(k));
              Optional<ByteString> t = valToContents.apply(toValues.get(k));
              return f.equals(t) ? null : Diff.of(k, f, t);
            })
        .filter(Objects::nonNull);
  }

  /**
   * Verifies that {@code expectedHash}, if present, is equal to {@code refHead}. Throws a {@link
   * ReferenceConflictException} if not.
   */
  protected void verifyExpectedHash(Hash refHead, NamedRef ref, Optional<Hash> expectedHash)
      throws ReferenceConflictException {
    if (expectedHash.isPresent() && !refHead.equals(expectedHash.get())) {
      throw new ReferenceConflictException(
          String.format(
              "Named-reference '%s' is not at expected hash '%s', but at '%s'.",
              ref.getName(), expectedHash.get().asString(), refHead.asString()));
    }
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

  /**
   * Ensures that {@code ref} exists and that the hash in {@code hashOnRef} exists in that
   * reference.
   *
   * @param ctx technical operation context
   * @param ref reference that must contain {@code hashOnRef}
   * @param knownHead current HEAD of {@code ref}
   * @param hashOnRef hash to verify whether it exists in {@code ref}
   * @return value of {@code hashOnRef} or, if {@code hashOnRef} is empty, {@code knownHead}
   * @throws ReferenceNotFoundException if either {@code ref} does not exist or {@code hashOnRef}
   *     does not exist on {@code ref}
   */
  protected Hash hashOnRef(OP_CONTEXT ctx, NamedRef ref, Hash knownHead, Optional<Hash> hashOnRef)
      throws ReferenceNotFoundException {
    return hashOnRef(ctx, knownHead, ref, hashOnRef, null);
  }

  /**
   * Same as {@link #hashOnRef(Object, NamedRef, Hash, Optional)}, but allows consuming the visited
   * {@link CommitLogEntry} objects.
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
   * Like {@link #commitLogFetcher(Object, Hash)}, but only returns the {@link Hash commit-log-entry
   * hashes}, which can be taken from {@link CommitLogEntry#getParents()}, thus no need to perform a
   * read-operation against every hash.
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
   * {@link #commitLogFetcher(Object, Hash)} or the similar implementation for the global-log for
   * non-transactional adapters.
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

  /**
   * Builds a {@link CommitLogEntry} using the given values. This function also includes an {@link
   * EmbeddedKeyList}, if triggered by the values of {@code currentKeyListDistance} and {@link
   * DatabaseAdapterConfig#getKeyListDistance()}, so read operations may happen.
   */
  protected CommitLogEntry buildIndividualCommit(
      OP_CONTEXT ctx,
      List<Hash> parentHashes,
      ByteString commitMeta,
      List<KeyWithContentsIdAndBytes> puts,
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
  protected Hash individualCommitHash(
      List<Hash> parentHashes,
      ByteString commitMeta,
      List<KeyWithContentsIdAndBytes> puts,
      List<Key> unchanged,
      List<Key> deletes) {
    Hasher hasher = newHasher();
    hasher.putLong(COMMIT_LOG_HASH_SEED);
    parentHashes.forEach(h -> hasher.putBytes(h.asBytes().asReadOnlyByteBuffer()));
    hasher.putBytes(commitMeta.asReadOnlyByteBuffer());
    puts.forEach(
        e -> {
          hashKey(hasher, e.getKey());
          hasher.putString(e.getContentsId(), StandardCharsets.UTF_8);
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
    ImmutableEmbeddedKeyList.Builder newKeyList = ImmutableEmbeddedKeyList.builder();
    keysForCommitEntry(ctx, startHash, KeyFilterPredicate.ALLOW_ALL).forEach(newKeyList::addKeys);

    return ImmutableCommitLogEntry.builder()
        .from(unwrittenEntry)
        .keyListDistance(0)
        .keyList(newKeyList.build())
        .build();
  }

  /**
   * If the current HEAD of the target branch for a commit/transplant/merge is not equal to the
   * expected/reference HEAD, verify that there is no conflict, like keys in the operations of the
   * commit(s) contained in keys of the commits 'expectedHead (excluding) .. currentHead
   * (including)'.
   */
  protected void checkForModifiedKeysBetweenExpectedAndCurrentCommit(
      CommitAttempt commitAttempt, OP_CONTEXT ctx, Hash branchHead, List<String> mismatches)
      throws ReferenceNotFoundException {

    if (commitAttempt.getExpectedHead().isPresent()) {
      Hash expectedHead = commitAttempt.getExpectedHead().get();
      if (!expectedHead.equals(branchHead)) {
        Set<Key> operationKeys = new HashSet<>();
        operationKeys.addAll(commitAttempt.getDeletes());
        operationKeys.addAll(commitAttempt.getUnchanged());
        commitAttempt.getPuts().stream().map(KeyWithBytes::getKey).forEach(operationKeys::add);

        boolean[] sinceSeen = new boolean[1];
        checkConflictingKeysForCommit(
            ctx, branchHead, expectedHead, operationKeys, sinceSeen, mismatches::add);

        // If the expectedHead is the special value NO_ANCESTOR, which is not persisted,
        // ignore the fact that it has not been seen. Otherwise, raise a
        // ReferenceNotFoundException that the expected-hash does not exist on the target
        // branch.
        if (!sinceSeen[0] && !expectedHead.equals(NO_ANCESTOR)) {
          throw hashNotFound(commitAttempt.getBranch(), expectedHead);
        }
      }
    }
  }

  /** Retrieve the contents-keys and their types for the commit-log-entry with the given hash. */
  protected Stream<KeyWithType> keysForCommitEntry(
      OP_CONTEXT ctx, Hash hash, KeyFilterPredicate keyFilter) {
    // walk the commit-logs in reverse order - starting with the last persisted key-list
    Set<KeyWithType> keys = new HashSet<>();

    // Commit log is processed in
    Set<Key> seen = new HashSet<>();
    Set<Key> removes = new HashSet<>();

    Stream<CommitLogEntry> log = commitLogFetcher(ctx, hash);
    log = takeUntil(log, e -> e.getKeyList() != null, true);
    log.forEach(
        e -> {
          if (e.getKeyList() != null) {
            e.getKeyList().getKeys().stream()
                .filter(kt -> seen.add(kt.getKey()))
                .filter(kt -> keyFilter.check(kt.getKey(), kt.getContentsId(), kt.getType()))
                .forEach(keys::add);
          }
          e.getPuts().stream()
              .filter(kt -> seen.add(kt.getKey()))
              .filter(kt -> keyFilter.check(kt.getKey(), kt.getContentsId(), kt.getType()))
              .map(KeyWithContentsIdAndBytes::asKeyWithType)
              .forEach(keys::add);
          // Only have 'Key' in deletes, so map it to a 'KeyWithType' and it can be used to remove
          // the entry from 'keys'.
          e.getDeletes().stream().filter(seen::add).forEach(removes::add);
        });

    return keys.stream().filter(kt -> !removes.contains(kt.getKey()));
  }

  /**
   * Fetch the global-state and per-ref contents for the given {@link Key}s and {@link Hash
   * commitSha}.
   */
  protected Map<Key, ContentsAndState<ByteString>> fetchValues(
      OP_CONTEXT ctx, Hash refHead, List<Key> keys, KeyFilterPredicate keyFilter) {
    Set<Key> remainingKeys = new HashSet<>(keys);

    Map<Key, ByteString> nonGlobal = new HashMap<>();
    Map<Key, String> keyToContentsIds = new HashMap<>();
    Set<String> contentsIds = new HashSet<>();
    try (Stream<CommitLogEntry> log =
        takeUntil(commitLogFetcher(ctx, refHead), e -> remainingKeys.isEmpty())) {
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

    Map<String, ByteString> globals = fetchGlobalStates(ctx, contentsIds);

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
   * @return map of content-id to state
   */
  protected abstract Map<String, ByteString> fetchGlobalStates(
      OP_CONTEXT ctx, Set<String> contentIds);

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
   * <p>Conflicts are reported via {@code mismatches}.
   */
  protected void checkConflictingKeysForCommit(
      OP_CONTEXT ctx,
      Hash upToCommitIncluding,
      Hash sinceCommitExcluding,
      Set<Key> keys,
      boolean[] sinceSeen,
      Consumer<String> mismatches) {
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

    Set<Key> handled = new HashSet<>();
    log.forEach(
        e -> {
          e.getPuts()
              .forEach(
                  a -> {
                    if (keys.contains(a.getKey()) && handled.add(a.getKey())) {
                      mismatches.accept(String.format("Key '%s' has put-operation.", a.getKey()));
                    }
                  });
          e.getDeletes()
              .forEach(
                  a -> {
                    if (keys.contains(a) && handled.add(a)) {
                      mismatches.accept(String.format("Key '%s' has delete-operation.", a));
                    }
                  });
        });
  }

  /**
   * Finds the common-ancestor of two commit-log-entries. If no common-ancestor is found, throws a
   * {@link ReferenceConflictException} or. Otherwise this method returns the hash of the
   * common-ancestor.
   */
  protected Hash findCommonAncestor(
      OP_CONTEXT ctx, NamedRef from, Hash fromHead, BranchName toBranch, Hash toHead)
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
        throw new ReferenceConflictException(
            String.format(
                "No common ancestor found for merge of '%s' into branch '%s'",
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
              sourceCommit.getPuts().stream().map(KeyWithContentsIdAndBytes::getKey),
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
          e.getPuts().stream()
              .map(KeyWithContentsIdAndBytes::getKey)
              .forEach(keysTouchedOnTarget::add);
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

  /**
   * Verifies that the current global-states match the {@code expectedStates}, produces human
   * readable messages for the violations.
   */
  protected void checkExpectedGlobalStates(
      OP_CONTEXT ctx, CommitAttempt commitAttempt, Consumer<String> mismatches) {
    Map<String, ByteString> globalStates =
        fetchGlobalStates(ctx, commitAttempt.getExpectedStates().keySet());
    for (Entry<String, Optional<ByteString>> expectedState :
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

  protected TryLoopState newTryLoopState(Supplier<String> retryErrorMessage) {
    return TryLoopState.newTryLoopState(retryErrorMessage, config);
  }

  /** Supporting functionality for {@link #allContents(BiFunction)}. */
  protected Stream<KeyWithContentsIdAndBytes> allContentsPerRefFetcher(
      OP_CONTEXT ctx,
      BiFunction<NamedRef, CommitLogEntry, Boolean> continueOnRefPredicate,
      Set<Hash> visistedHashes,
      NamedRef ref,
      Hash refHash) {
    return takeUntil(
            commitLogFetcher(ctx, refHash),
            // Take until ...
            e ->
                // ... we reach an already seen commit-hash, op ...
                !visistedHashes.add(e.getHash())
                    // ... the 'continueOnRefPredicate' tells us to stop visiting commits on
                    // this named-reference
                    && continueOnRefPredicate.apply(ref, e),
            false)
        .flatMap(e -> e.getPuts().stream());
  }
}
