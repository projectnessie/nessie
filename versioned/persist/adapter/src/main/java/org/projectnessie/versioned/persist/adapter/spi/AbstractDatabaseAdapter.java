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

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.projectnessie.model.Conflict.ConflictType.NAMESPACE_ABSENT;
import static org.projectnessie.model.Conflict.ConflictType.NAMESPACE_NOT_EMPTY;
import static org.projectnessie.model.Conflict.ConflictType.NOT_A_NAMESPACE;
import static org.projectnessie.model.Conflict.conflict;
import static org.projectnessie.versioned.persist.adapter.KeyFilterPredicate.ALLOW_ALL;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterMetrics.tryLoopFinished;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.hashKey;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.hashNotFound;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.newHasher;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.referenceNotFound;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.takeUntilExcludeLast;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.takeUntilIncludeLast;
import static org.projectnessie.versioned.persist.adapter.spi.Traced.trace;
import static org.projectnessie.versioned.store.DefaultStoreWorker.contentTypeForPayload;
import static org.projectnessie.versioned.store.DefaultStoreWorker.payloadForContent;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.hash.Hasher;
import com.google.errorprone.annotations.MustBeClosed;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.log.Fields;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.Spliterators.AbstractSpliterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;
import org.projectnessie.model.Conflict;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.MergeBehavior;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.nessie.relocated.protobuf.UnsafeByteOperations;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Diff;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.GetNamedRefsParams.RetrieveOptions;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ImmutableKeyDetails;
import org.projectnessie.versioned.ImmutableMergeResult;
import org.projectnessie.versioned.ImmutableReferenceInfo;
import org.projectnessie.versioned.MergeConflictException;
import org.projectnessie.versioned.MergeResult;
import org.projectnessie.versioned.MergeResult.ConflictType;
import org.projectnessie.versioned.MergeResult.KeyDetails;
import org.projectnessie.versioned.MergeType;
import org.projectnessie.versioned.MetadataRewriter;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.ReferenceInfo.CommitsAheadBehind;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.StoreWorker;
import org.projectnessie.versioned.TagName;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry.KeyListVariant;
import org.projectnessie.versioned.persist.adapter.CommitParams;
import org.projectnessie.versioned.persist.adapter.ContentAndState;
import org.projectnessie.versioned.persist.adapter.ContentId;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapterConfig;
import org.projectnessie.versioned.persist.adapter.Difference;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitLogEntry;
import org.projectnessie.versioned.persist.adapter.KeyFilterPredicate;
import org.projectnessie.versioned.persist.adapter.KeyList;
import org.projectnessie.versioned.persist.adapter.KeyListEntity;
import org.projectnessie.versioned.persist.adapter.KeyListEntry;
import org.projectnessie.versioned.persist.adapter.KeyWithBytes;
import org.projectnessie.versioned.persist.adapter.MergeParams;
import org.projectnessie.versioned.persist.adapter.MetadataRewriteParams;
import org.projectnessie.versioned.persist.adapter.TransplantParams;
import org.projectnessie.versioned.persist.adapter.events.AdapterEvent;
import org.projectnessie.versioned.persist.adapter.events.AdapterEventConsumer;
import org.projectnessie.versioned.store.DefaultStoreWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public abstract class AbstractDatabaseAdapter<
        OP_CONTEXT extends AutoCloseable, CONFIG extends DatabaseAdapterConfig>
    implements DatabaseAdapter {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractDatabaseAdapter.class);

  protected static final String TAG_HASH = "hash";
  protected static final String TAG_COUNT = "count";
  protected final CONFIG config;
  protected static final StoreWorker STORE_WORKER = DefaultStoreWorker.instance();
  private final AdapterEventConsumer eventConsumer;

  @SuppressWarnings("UnstableApiUsage")
  public static final Hash NO_ANCESTOR =
      Hash.of(
          UnsafeByteOperations.unsafeWrap(
              newHasher().putString("empty", StandardCharsets.UTF_8).hash().asBytes()));

  protected static long COMMIT_LOG_HASH_SEED = 946928273206945677L;

  protected AbstractDatabaseAdapter(CONFIG config, AdapterEventConsumer eventConsumer) {
    Objects.requireNonNull(config, "config parameter must not be null");
    this.config = config;
    this.eventConsumer = eventConsumer;
  }

  @Override
  public CONFIG getConfig() {
    return config;
  }

  @VisibleForTesting
  public AdapterEventConsumer getEventConsumer() {
    return eventConsumer;
  }

  @VisibleForTesting
  public abstract OP_CONTEXT borrowConnection();

  @Override
  public Hash noAncestorHash() {
    return NO_ANCESTOR;
  }

  @Override
  public Stream<CommitLogEntry> fetchCommitLogEntries(Stream<Hash> hashes) {
    OP_CONTEXT ctx = borrowConnection();

    BatchSpliterator<Hash, CommitLogEntry> commitFetcher =
        new BatchSpliterator<>(
            config.getParentsPerCommit(),
            hashes,
            h -> fetchMultipleFromCommitLog(ctx, h, ignore -> null).spliterator(),
            0);

    return StreamSupport.stream(commitFetcher, false)
        .onClose(
            () -> {
              try {
                ctx.close();
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
  }

  @Override
  public CommitLogEntry rebuildKeyList(
      CommitLogEntry entry,
      @Nonnull @jakarta.annotation.Nonnull Function<Hash, CommitLogEntry> inMemoryCommits)
      throws ReferenceNotFoundException {
    try (OP_CONTEXT ctx = borrowConnection()) {
      return buildKeyList(ctx, entry, h -> {}, inMemoryCommits);
    } catch (ReferenceNotFoundException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // /////////////////////////////////////////////////////////////////////////////////////////////
  // DatabaseAdapter subclass API (protected)
  // /////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Logic implementation of a commit-attempt.
   *
   * @param ctx technical operation-context
   * @param commitParams commit parameters
   * @param branchHead current HEAD of {@code branch}
   * @param newKeyLists consumer for optimistically written {@link KeyListEntity}s
   * @return optimistically written commit-log-entry
   */
  protected CommitLogEntry commitAttempt(
      OP_CONTEXT ctx,
      long timeInMicros,
      Hash branchHead,
      CommitParams commitParams,
      Consumer<Hash> newKeyLists)
      throws ReferenceNotFoundException, ReferenceConflictException {
    List<String> mismatches = new ArrayList<>();

    checkContentKeysUnique(commitParams);

    // verify expected global-states
    checkExpectedGlobalStates(ctx, commitParams, mismatches::add);

    CommitLogEntry currentBranchEntry =
        checkForModifiedKeysBetweenExpectedAndCurrentCommit(
            ctx, commitParams, branchHead, mismatches);

    if (!mismatches.isEmpty()) {
      throw new ReferenceConflictException(String.join("\n", mismatches));
    }

    int parentsPerCommit = config.getParentsPerCommit();
    List<Hash> newParents = new ArrayList<>(parentsPerCommit);
    newParents.add(branchHead);
    long commitSeq;
    if (currentBranchEntry != null) {
      List<Hash> p = currentBranchEntry.getParents();
      newParents.addAll(p.subList(0, Math.min(p.size(), parentsPerCommit - 1)));
      commitSeq = currentBranchEntry.getCommitSeq() + 1;
    } else {
      commitSeq = 1;
    }

    // Helps when building a new key-list to not fetch the current commit again.
    Function<Hash, CommitLogEntry> currentCommit =
        h -> h.equals(branchHead) ? currentBranchEntry : null;

    CommitLogEntry newBranchCommit =
        buildIndividualCommit(
            ctx,
            timeInMicros,
            newParents,
            commitSeq,
            commitParams.getCommitMetaSerialized(),
            commitParams.getPuts(),
            commitParams.getDeletes(),
            currentBranchEntry != null ? currentBranchEntry.getKeyListDistance() : 0,
            newKeyLists,
            currentCommit,
            emptyList());
    writeIndividualCommit(ctx, newBranchCommit);

    return newBranchCommit;
  }

  private static void checkContentKeysUnique(CommitParams commitParams) {
    Set<ContentKey> keys = new HashSet<>();
    Set<ContentKey> duplicates = new HashSet<>();
    Stream.concat(
            Stream.concat(
                commitParams.getDeletes().stream(),
                commitParams.getPuts().stream().map(KeyWithBytes::getKey)),
            commitParams.getUnchanged().stream())
        .forEach(
            key -> {
              if (!keys.add(key)) {
                duplicates.add(key);
              }
            });
    if (!duplicates.isEmpty()) {
      throw new IllegalArgumentException(
          format(
              "Duplicate keys are not allowed in a commit: %s",
              duplicates.stream().map(ContentKey::toString).collect(Collectors.joining(", "))));
    }
  }

  /**
   * Logic implementation of a merge-attempt.
   *
   * @param ctx technical operation context
   * @param toHead current HEAD of {@code toBranch}
   * @param branchCommits consumer for the individual commits to merge
   * @param newKeyLists consumer for optimistically written {@link KeyListEntity}s
   * @return hash of the last commit-log-entry written to {@code toBranch}
   */
  protected Hash mergeAttempt(
      OP_CONTEXT ctx,
      long timeInMicros,
      Hash toHead,
      Consumer<Hash> branchCommits,
      Consumer<Hash> newKeyLists,
      Consumer<CommitLogEntry> writtenCommits,
      Consumer<CommitLogEntry> addedCommits,
      MergeParams mergeParams,
      ImmutableMergeResult.Builder<CommitLogEntry> mergeResult)
      throws ReferenceNotFoundException, ReferenceConflictException {

    validateHashExists(ctx, mergeParams.getMergeFromHash());

    // 1. ensure 'expectedHash' is a parent of HEAD-of-'toBranch'
    hashOnRef(ctx, mergeParams.getToBranch(), mergeParams.getExpectedHead(), toHead);

    mergeParams.getExpectedHead().ifPresent(mergeResult::expectedHash);
    mergeResult.targetBranch(mergeParams.getToBranch()).effectiveTargetHash(toHead);

    // 2. find nearest common-ancestor between 'from' + 'fromHash'
    Hash commonAncestor =
        findCommonAncestor(ctx, mergeParams.getMergeFromHash(), mergeParams.getToBranch(), toHead);

    mergeResult.commonAncestor(commonAncestor);

    // 3. Collect commit-log-entries
    List<CommitLogEntry> toEntriesReverseChronological;
    try (Stream<CommitLogEntry> commits = readCommitLogStream(ctx, toHead)) {
      toEntriesReverseChronological =
          takeUntilExcludeLast(commits, e -> e.getHash().equals(commonAncestor))
              .collect(Collectors.toList());
    }

    toEntriesReverseChronological.forEach(mergeResult::addTargetCommits);

    List<CommitLogEntry> commitsToMergeChronological;
    try (Stream<CommitLogEntry> commits =
        readCommitLogStream(ctx, mergeParams.getMergeFromHash())) {
      commitsToMergeChronological =
          takeUntilExcludeLast(commits, e -> e.getHash().equals(commonAncestor))
              .collect(Collectors.toList());
    }

    if (commitsToMergeChronological.isEmpty()) {
      // Nothing to merge, shortcut
      throw new IllegalArgumentException(
          format(
              "No hashes to merge from '%s' onto '%s' @ '%s' using common ancestor '%s',"
                  + " expected commit ID from request was '%s'.",
              mergeParams.getMergeFromHash().asString(),
              mergeParams.getToBranch().getName(),
              toHead,
              commonAncestor.asString(),
              mergeParams.getExpectedHead().map(Hash::asString).orElse("(not specified)")));
    }

    commitsToMergeChronological.forEach(mergeResult::addSourceCommits);

    return mergeTransplantCommon(
        ctx,
        timeInMicros,
        toHead,
        branchCommits,
        newKeyLists,
        commitsToMergeChronological,
        toEntriesReverseChronological,
        mergeParams,
        mergeResult,
        writtenCommits,
        addedCommits,
        singletonList(commitsToMergeChronological.get(0).getHash()));
  }

  /**
   * Logic implementation of a transplant-attempt.
   *
   * @param ctx technical operation context
   * @param targetHead current HEAD of {@code targetBranch}
   * @param branchCommits consumer for the individual commits to merge
   * @param newKeyLists consumer for optimistically written {@link KeyListEntity}s
   * @return hash of the last commit-log-entry written to {@code targetBranch}
   */
  protected Hash transplantAttempt(
      OP_CONTEXT ctx,
      long timeInMicros,
      Hash targetHead,
      Consumer<Hash> branchCommits,
      Consumer<Hash> newKeyLists,
      Consumer<CommitLogEntry> writtenCommits,
      Consumer<CommitLogEntry> addedCommits,
      TransplantParams transplantParams,
      ImmutableMergeResult.Builder<CommitLogEntry> mergeResult)
      throws ReferenceNotFoundException, ReferenceConflictException {
    if (transplantParams.getSequenceToTransplant().isEmpty()) {
      throw new IllegalArgumentException("No hashes to transplant given.");
    }

    transplantParams.getExpectedHead().ifPresent(mergeResult::expectedHash);
    mergeResult.targetBranch(transplantParams.getToBranch()).effectiveTargetHash(targetHead);

    // 1. ensure 'expectedHash' is a parent of HEAD-of-'targetBranch' & collect keys
    List<CommitLogEntry> targetEntriesReverseChronological = new ArrayList<>();
    hashOnRef(
        ctx,
        targetHead,
        transplantParams.getToBranch(),
        transplantParams.getExpectedHead(),
        targetEntriesReverseChronological::add);

    targetEntriesReverseChronological.forEach(mergeResult::addTargetCommits);

    // Exclude the expected-hash on the target-branch from key-collisions check
    if (!targetEntriesReverseChronological.isEmpty()
        && transplantParams.getExpectedHead().isPresent()
        && targetEntriesReverseChronological
            .get(0)
            .getHash()
            .equals(transplantParams.getExpectedHead().get())) {
      targetEntriesReverseChronological.remove(0);
    }

    // 4. ensure 'sequenceToTransplant' is sequential
    int[] index = {transplantParams.getSequenceToTransplant().size() - 1};
    Hash lastHash =
        transplantParams
            .getSequenceToTransplant()
            .get(transplantParams.getSequenceToTransplant().size() - 1);
    List<CommitLogEntry> commitsToTransplantChronological;
    try (Stream<CommitLogEntry> commits = readCommitLogStream(ctx, lastHash)) {
      commitsToTransplantChronological =
          takeUntilExcludeLast(
                  commits,
                  e -> {
                    int i = index[0]--;
                    if (i == -1) {
                      return true;
                    }
                    if (!e.getHash().equals(transplantParams.getSequenceToTransplant().get(i))) {
                      throw new IllegalArgumentException("Sequence of hashes is not contiguous.");
                    }
                    return false;
                  })
              .collect(Collectors.toList());
    }

    commitsToTransplantChronological.forEach(mergeResult::addSourceCommits);

    // 5. check for key-collisions
    return mergeTransplantCommon(
        ctx,
        timeInMicros,
        targetHead,
        branchCommits,
        newKeyLists,
        commitsToTransplantChronological,
        targetEntriesReverseChronological,
        transplantParams,
        mergeResult,
        writtenCommits,
        addedCommits,
        emptyList());
  }

  protected Hash mergeTransplantCommon(
      OP_CONTEXT ctx,
      long timeInMicros,
      Hash toHead,
      Consumer<Hash> branchCommits,
      Consumer<Hash> newKeyLists,
      List<CommitLogEntry> commitsToMergeChronological,
      List<CommitLogEntry> toEntriesReverseChronological,
      MetadataRewriteParams params,
      ImmutableMergeResult.Builder<CommitLogEntry> mergeResult,
      Consumer<CommitLogEntry> writtenCommits,
      Consumer<CommitLogEntry> addedCommits,
      List<Hash> additionalParents)
      throws ReferenceConflictException, ReferenceNotFoundException {

    // Collect modified keys.
    Collections.reverse(toEntriesReverseChronological);

    Map<ContentKey, ImmutableKeyDetails.Builder> keyDetailsMap = new HashMap<>();

    Function<ContentKey, MergeType> mergeType =
        key -> params.getMergeTypes().getOrDefault(key, params.getDefaultMergeType());

    Function<ContentKey, ImmutableKeyDetails.Builder> keyDetails =
        key ->
            keyDetailsMap.computeIfAbsent(
                key,
                x ->
                    KeyDetails.builder()
                        .mergeBehavior(MergeBehavior.valueOf(mergeType.apply(key).name())));

    BiConsumer<Stream<CommitLogEntry>, BiConsumer<ImmutableKeyDetails.Builder, Iterable<Hash>>>
        keysFromCommitsToKeyDetails =
            (commits, receiver) -> {
              Map<ContentKey, Set<Hash>> keyHashesMap = new HashMap<>();
              Function<ContentKey, Set<Hash>> keyHashes =
                  key -> keyHashesMap.computeIfAbsent(key, x -> new LinkedHashSet<>());
              commits.forEach(
                  commit -> {
                    commit
                        .getDeletes()
                        .forEach(delete -> keyHashes.apply(delete).add(commit.getHash()));
                    commit
                        .getPuts()
                        .forEach(put -> keyHashes.apply(put.getKey()).add(commit.getHash()));
                  });
              keyHashesMap.forEach((key, hashes) -> receiver.accept(keyDetails.apply(key), hashes));
            };

    Set<ContentKey> keysTouchedOnTarget = new HashSet<>();
    keysFromCommitsToKeyDetails.accept(
        commitsToMergeChronological.stream(), ImmutableKeyDetails.Builder::addAllSourceCommits);
    keysFromCommitsToKeyDetails.accept(
        toEntriesReverseChronological.stream()
            .peek(
                e -> {
                  e.getPuts().stream().map(KeyWithBytes::getKey).forEach(keysTouchedOnTarget::add);
                  e.getDeletes().forEach(keysTouchedOnTarget::remove);
                }),
        ImmutableKeyDetails.Builder::addAllTargetCommits);

    Predicate<ContentKey> skipCheckPredicate = k -> mergeType.apply(k).isSkipCheck();
    Predicate<ContentKey> mergePredicate = k -> mergeType.apply(k).isMerge();

    // Ignore keys in collision-check that will not be merged or collision-checked
    keysTouchedOnTarget.removeIf(skipCheckPredicate);

    boolean hasCollisions =
        hasKeyCollisions(ctx, toHead, keysTouchedOnTarget, commitsToMergeChronological, keyDetails);
    keyDetailsMap.forEach((key, details) -> mergeResult.putDetails(key, details.build()));

    mergeResult.wasSuccessful(!hasCollisions);

    if (hasCollisions) {
      MergeResult<CommitLogEntry> result = mergeResult.resultantTargetHash(toHead).build();
      throw new MergeConflictException(
          format(
              "The following keys have been changed in conflict: %s",
              result.getDetails().entrySet().stream()
                  .filter(e -> e.getValue().getConflictType() != ConflictType.NONE)
                  .map(Map.Entry::getKey)
                  .sorted()
                  .map(key -> format("'%s'", key))
                  .collect(Collectors.joining(", "))),
          result);
    }

    if (params.isDryRun()) {
      return toHead;
    }

    if (params.keepIndividualCommits()) {
      // re-apply commits in 'sequenceToTransplant' onto 'targetBranch'
      toHead =
          copyCommits(
              ctx,
              timeInMicros,
              toHead,
              commitsToMergeChronological,
              newKeyLists,
              params.getUpdateCommitMetadata(),
              mergePredicate,
              addedCommits);

      // Write commits

      writeMultipleCommits(ctx, commitsToMergeChronological);
      commitsToMergeChronological.stream()
          .peek(writtenCommits)
          .map(CommitLogEntry::getHash)
          .forEach(branchCommits);
    } else {
      CommitLogEntry squashed =
          squashCommits(
              ctx,
              timeInMicros,
              toHead,
              commitsToMergeChronological,
              newKeyLists,
              params.getUpdateCommitMetadata(),
              mergePredicate,
              additionalParents,
              addedCommits);

      if (squashed != null) {
        writtenCommits.accept(squashed);
        toHead = squashed.getHash();
      }
    }
    return toHead;
  }

  /**
   * Compute the diff between two references.
   *
   * @param ctx technical operation context
   * @param from "from" reference to compute the difference from, appears on the "from" side in
   *     {@link Diff} with hash in {@code from} to compute the diff for, must exist in {@code from}
   * @param to "to" reference to compute the difference from, appears on the "to" side in {@link
   *     Diff} with hash in {@code to} to compute the diff for, must exist in {@code to}
   * @param keyFilter optional filter on key + content-id + content-type
   * @return computed difference
   */
  @MustBeClosed
  protected Stream<Difference> buildDiff(
      OP_CONTEXT ctx, Hash from, Hash to, KeyFilterPredicate keyFilter)
      throws ReferenceNotFoundException {
    // TODO this implementation works, but is definitely not the most efficient one.

    Set<ContentKey> allKeys = new HashSet<>();
    try (Stream<ContentKey> s =
        keysForCommitEntry(ctx, from, keyFilter).map(KeyListEntry::getKey)) {
      s.forEach(allKeys::add);
    }
    try (Stream<ContentKey> s = keysForCommitEntry(ctx, to, keyFilter).map(KeyListEntry::getKey)) {
      s.forEach(allKeys::add);
    }

    if (allKeys.isEmpty()) {
      // no keys, shortcut
      return Stream.empty();
    }

    List<ContentKey> allKeysList = new ArrayList<>(allKeys);
    Map<ContentKey, ContentAndState> fromValues = fetchValues(ctx, from, allKeysList, keyFilter);
    Map<ContentKey, ContentAndState> toValues = fetchValues(ctx, to, allKeysList, keyFilter);

    Function<ContentAndState, Optional<ByteString>> valToContent =
        cs -> cs != null ? Optional.of(cs.getRefState()) : Optional.empty();

    return IntStream.range(0, allKeys.size())
        .mapToObj(allKeysList::get)
        .map(
            k -> {
              ContentAndState fromVal = fromValues.get(k);
              ContentAndState toVal = toValues.get(k);
              Optional<ByteString> f = valToContent.apply(fromVal);
              Optional<ByteString> t = valToContent.apply(toVal);
              if (f.equals(t)) {
                return null;
              }
              byte payload =
                  fromVal != null
                      ? fromVal.getPayload()
                      : (toVal != null ? toVal.getPayload() : (byte) 0);
              Optional<ByteString> g =
                  Optional.ofNullable(
                      fromVal != null
                          ? fromVal.getGlobalState()
                          : (toVal != null ? toVal.getGlobalState() : null));
              return Difference.of(payload, k, g, f, t);
            })
        .filter(Objects::nonNull);
  }

  /**
   * Common functionality to filter and enhance based on the given {@link GetNamedRefsParams}.
   *
   * @param ctx database-adapter context
   * @param params defines which kind of references and which additional information shall be
   *     retrieved
   * @param defaultBranchHead prerequisite, the hash of the default branch's HEAD commit (depending
   *     on the database-adapter implementation). If {@code null}, {@link
   *     #namedRefsWithDefaultBranchRelatedInfo(AutoCloseable, GetNamedRefsParams, Stream, Hash)}
   *     will not add additional default-branch related information (common ancestor and commits
   *     behind/ahead).
   * @param refs current {@link Stream} of {@link ReferenceInfo} to be enhanced.
   * @return filtered/enhanced stream based on {@code refs}.
   */
  @MustBeClosed
  @SuppressWarnings("MustBeClosedChecker")
  protected Stream<ReferenceInfo<ByteString>> namedRefsFilterAndEnhance(
      OP_CONTEXT ctx,
      GetNamedRefsParams params,
      Hash defaultBranchHead,
      Stream<ReferenceInfo<ByteString>> refs) {
    refs = namedRefsMaybeFilter(params, refs);

    refs = namedRefsWithDefaultBranchRelatedInfo(ctx, params, refs, defaultBranchHead);

    refs = namedReferenceWithCommitMeta(ctx, params, refs);

    return refs;
  }

  /** Applies the reference type filter (tags or branches) to the Java stream. */
  protected static Stream<ReferenceInfo<ByteString>> namedRefsMaybeFilter(
      GetNamedRefsParams params, Stream<ReferenceInfo<ByteString>> refs) {
    if (params.getBranchRetrieveOptions().isRetrieve()
        && params.getTagRetrieveOptions().isRetrieve()) {
      // No filtering necessary, if all named-reference types (tags and branches) are being fetched.
      return refs;
    }
    return refs.filter(ref -> namedRefsRetrieveOptionsForReference(params, ref).isRetrieve());
  }

  protected static boolean namedRefsRequiresBaseReference(GetNamedRefsParams params) {
    return namedRefsRequiresBaseReference(params.getBranchRetrieveOptions())
        || namedRefsRequiresBaseReference(params.getTagRetrieveOptions());
  }

  protected static boolean namedRefsRequiresBaseReference(RetrieveOptions retrieveOptions) {
    return retrieveOptions.isComputeAheadBehind() || retrieveOptions.isComputeCommonAncestor();
  }

  protected static boolean namedRefsAnyRetrieves(GetNamedRefsParams params) {
    return params.getBranchRetrieveOptions().isRetrieve()
        || params.getTagRetrieveOptions().isRetrieve();
  }

  protected static RetrieveOptions namedRefsRetrieveOptionsForReference(
      GetNamedRefsParams params, ReferenceInfo<ByteString> ref) {
    return namedRefsRetrieveOptionsForReference(params, ref.getNamedRef());
  }

  protected static RetrieveOptions namedRefsRetrieveOptionsForReference(
      GetNamedRefsParams params, NamedRef ref) {
    if (ref instanceof BranchName) {
      return params.getBranchRetrieveOptions();
    }
    if (ref instanceof TagName) {
      return params.getTagRetrieveOptions();
    }
    throw new IllegalArgumentException("ref must be either BranchName or TabName, but is " + ref);
  }

  /**
   * Returns an updated {@link ReferenceInfo} with the commit-meta of the reference's HEAD commit.
   */
  @MustBeClosed
  protected Stream<ReferenceInfo<ByteString>> namedReferenceWithCommitMeta(
      OP_CONTEXT ctx, GetNamedRefsParams params, Stream<ReferenceInfo<ByteString>> refs) {
    return refs.map(
        ref -> {
          if (!namedRefsRetrieveOptionsForReference(params, ref).isRetrieveCommitMetaForHead()) {
            return ref;
          }
          CommitLogEntry logEntry = fetchFromCommitLog(ctx, ref.getHash());
          if (logEntry == null) {
            return ref;
          }
          return ImmutableReferenceInfo.<ByteString>builder()
              .from(ref)
              .headCommitMeta(logEntry.getMetadata())
              .commitSeq(logEntry.getCommitSeq())
              .addParentHashes(logEntry.getParents().get(0))
              .addAllParentHashes(logEntry.getAdditionalParents())
              .build();
        });
  }

  /**
   * If necessary based on the given {@link GetNamedRefsParams}, updates the returned {@link
   * ReferenceInfo}s with the common-ancestor of the named reference and the default branch and the
   * number of commits behind/ahead compared to the default branch.
   *
   * <p>The common ancestor and/or information of commits behind/ahead is meaningless ({@code null})
   * for the default branch. Both fields are also {@code null} if the named reference points to the
   * {@link #noAncestorHash()} (beginning of time).
   */
  @MustBeClosed
  protected Stream<ReferenceInfo<ByteString>> namedRefsWithDefaultBranchRelatedInfo(
      OP_CONTEXT ctx,
      GetNamedRefsParams params,
      Stream<ReferenceInfo<ByteString>> refs,
      Hash defaultBranchHead) {
    if (defaultBranchHead == null) {
      // No enhancement of common ancestor and/or commits behind/ahead.
      return refs;
    }

    CommonAncestorState commonAncestorState =
        new CommonAncestorState(
            ctx,
            defaultBranchHead,
            params.getBranchRetrieveOptions().isComputeAheadBehind()
                || params.getTagRetrieveOptions().isComputeAheadBehind());

    return refs.map(
        ref -> {
          if (ref.getNamedRef().equals(params.getBaseReference())) {
            return ref;
          }

          RetrieveOptions retrieveOptions = namedRefsRetrieveOptionsForReference(params, ref);

          ReferenceInfo<ByteString> updated =
              namedRefsRequiresBaseReference(retrieveOptions)
                  ? findCommonAncestor(
                      ctx,
                      ref.getHash(),
                      commonAncestorState,
                      (diffOnFrom, hash) -> {
                        ReferenceInfo<ByteString> newRef = ref;
                        if (retrieveOptions.isComputeCommonAncestor()) {
                          newRef = newRef.withCommonAncestor(hash);
                        }
                        if (retrieveOptions.isComputeAheadBehind()) {
                          int behind = commonAncestorState.indexOf(hash);
                          CommitsAheadBehind aheadBehind =
                              CommitsAheadBehind.of(diffOnFrom, behind);
                          newRef = newRef.withAheadBehind(aheadBehind);
                        }
                        return newRef;
                      })
                  : null;

          return updated != null ? updated : ref;
        });
  }

  /**
   * Convenience for {@link #hashOnRef(AutoCloseable, Hash, NamedRef, Optional, Consumer)
   * hashOnRef(ctx, knownHead, ref.getReference(), ref.getHashOnReference(), null)}.
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
          try (Stream<CommitLogEntry> commits = readCommitLogStream(ctx, knownHead)) {
            commits.forEach(commitLogVisitor);
          }
        }
        return suspect;
      }

      try (Stream<Hash> hashes = commitsForHashOnRef(ctx, knownHead, commitLogVisitor)) {
        if (hashes.noneMatch(suspect::equals)) {
          throw hashNotFound(ref, suspect);
        }
        return suspect;
      }
    } else {
      return knownHead;
    }
  }

  @MustBeClosed
  private Stream<Hash> commitsForHashOnRef(
      OP_CONTEXT ctx, Hash knownHead, Consumer<CommitLogEntry> commitLogVisitor)
      throws ReferenceNotFoundException {
    if (commitLogVisitor != null) {
      return readCommitLogStream(ctx, knownHead)
          .peek(commitLogVisitor)
          .map(CommitLogEntry::getHash);
    } else {
      return readCommitLogHashesStream(ctx, knownHead);
    }
  }

  protected void validateHashExists(OP_CONTEXT ctx, Hash hash) throws ReferenceNotFoundException {
    if (!NO_ANCESTOR.equals(hash) && fetchFromCommitLog(ctx, hash) == null) {
      throw referenceNotFound(hash);
    }
  }

  /** Load the commit-log entry for the given hash, return {@code null}, if not found. */
  @VisibleForTesting
  public final CommitLogEntry fetchFromCommitLog(OP_CONTEXT ctx, Hash hash) {
    if (hash.equals(NO_ANCESTOR)) {
      // Do not try to fetch NO_ANCESTOR - it won't exist.
      return null;
    }
    try (Traced ignore = trace("fetchFromCommitLog").tag(TAG_HASH, hash.asString())) {
      return doFetchFromCommitLog(ctx, hash);
    }
  }

  protected abstract CommitLogEntry doFetchFromCommitLog(OP_CONTEXT ctx, Hash hash);

  @Override
  @SuppressWarnings("MustBeClosedChecker")
  public Stream<CommitLogEntry> scanAllCommitLogEntries() {
    OP_CONTEXT ctx = borrowConnection();
    return doScanAllCommitLogEntries(ctx)
        .onClose(
            () -> {
              try {
                ctx.close();
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
  }

  @MustBeClosed
  protected abstract Stream<CommitLogEntry> doScanAllCommitLogEntries(OP_CONTEXT c);

  /**
   * Fetch multiple {@link CommitLogEntry commit-log-entries} from the commit-log. The returned list
   * must have exactly as many elements as in the parameter {@code hashes}. Non-existing hashes are
   * returned as {@code null}.
   */
  private List<CommitLogEntry> fetchMultipleFromCommitLog(
      OP_CONTEXT ctx,
      List<Hash> hashes,
      @Nonnull @jakarta.annotation.Nonnull Function<Hash, CommitLogEntry> inMemoryCommits) {
    List<CommitLogEntry> result = new ArrayList<>(hashes.size());
    BitSet remainingHashes = null;

    // Prefetch commits already available in memory. Record indexes for the missing commits to
    // enable placing them in the correct positions later, when they are fetched from storage.
    for (int i = 0; i < hashes.size(); i++) {
      Hash hash = hashes.get(i);
      if (NO_ANCESTOR.equals(hash)) {
        result.add(null);
        continue;
      }

      CommitLogEntry found = inMemoryCommits.apply(hash);
      if (found != null) {
        result.add(found);
      } else {
        if (remainingHashes == null) {
          remainingHashes = new BitSet();
        }
        result.add(null); // to be replaced with storage result below
        remainingHashes.set(i);
      }
    }

    if (remainingHashes != null) {
      List<CommitLogEntry> fromStorage;

      try (Traced ignore =
          trace("fetchPageFromCommitLog")
              .tag(TAG_HASH, hashes.get(0).asString())
              .tag(TAG_COUNT, hashes.size())) {
        fromStorage =
            doFetchMultipleFromCommitLog(
                ctx, remainingHashes.stream().mapToObj(hashes::get).collect(Collectors.toList()));
      }

      // Fill the gaps in the final result list. Note that fetchPageFromCommitLog must return the
      // list of the same size as its `remainingHashes` parameter.
      Iterator<CommitLogEntry> iter = fromStorage.iterator();
      remainingHashes.stream().forEach(i -> result.set(i, iter.next()));
    }

    return result;
  }

  protected abstract List<CommitLogEntry> doFetchMultipleFromCommitLog(
      OP_CONTEXT ctx, List<Hash> hashes);

  /** Reads from the commit-log starting at the given commit-log-hash. */
  @MustBeClosed
  protected Stream<CommitLogEntry> readCommitLogStream(OP_CONTEXT ctx, Hash initialHash)
      throws ReferenceNotFoundException {
    Spliterator<CommitLogEntry> split = readCommitLog(ctx, initialHash, h -> null);
    return StreamSupport.stream(split, false);
  }

  @MustBeClosed
  protected Stream<CommitLogEntry> readCommitLogStream(
      OP_CONTEXT ctx,
      Hash initialHash,
      @Nonnull @jakarta.annotation.Nonnull Function<Hash, CommitLogEntry> inMemoryCommits)
      throws ReferenceNotFoundException {
    Spliterator<CommitLogEntry> split = readCommitLog(ctx, initialHash, inMemoryCommits);
    return StreamSupport.stream(split, false);
  }

  protected Spliterator<CommitLogEntry> readCommitLog(
      OP_CONTEXT ctx,
      Hash initialHash,
      @Nonnull @jakarta.annotation.Nonnull Function<Hash, CommitLogEntry> inMemoryCommits)
      throws ReferenceNotFoundException {
    Preconditions.checkNotNull(inMemoryCommits, "in-memory commits cannot be null");

    if (NO_ANCESTOR.equals(initialHash)) {
      return Spliterators.emptySpliterator();
    }

    CommitLogEntry initial = inMemoryCommits.apply(initialHash);
    if (initial == null) {
      initial = fetchFromCommitLog(ctx, initialHash);
    }

    if (initial == null) {
      throw referenceNotFound(initialHash);
    }

    BiFunction<OP_CONTEXT, List<Hash>, List<CommitLogEntry>> fetcher =
        (c, hashes) -> fetchMultipleFromCommitLog(c, hashes, inMemoryCommits);

    return logFetcher(ctx, initial, fetcher, CommitLogEntry::getParents);
  }

  /**
   * Like {@link #readCommitLogStream(AutoCloseable, Hash)}, but only returns the {@link Hash
   * commit-log-entry hashes}, which can be taken from {@link CommitLogEntry#getParents()}, thus no
   * need to perform a read-operation against every hash.
   */
  @MustBeClosed
  protected Stream<Hash> readCommitLogHashesStream(OP_CONTEXT ctx, Hash initialHash) {
    Spliterator<Hash> split = readCommitLogHashes(ctx, initialHash);
    return StreamSupport.stream(split, false);
  }

  protected Spliterator<Hash> readCommitLogHashes(OP_CONTEXT ctx, Hash initialHash) {
    return logFetcher(
        ctx,
        initialHash,
        (c, hashes) -> hashes,
        hash -> {
          CommitLogEntry entry = fetchFromCommitLog(ctx, hash);
          if (entry == null) {
            return emptyList();
          }
          return entry.getParents();
        });
  }

  /**
   * Constructs a {@link Stream} of entries for either the global-state-log or a commit-log or a
   * reflog entry. Use {@link #readCommitLogStream(AutoCloseable, Hash)} or the similar
   * implementation for the global-log or reflog entry for non-transactional adapters.
   */
  protected <T> Spliterator<T> logFetcher(
      OP_CONTEXT ctx,
      T initial,
      BiFunction<OP_CONTEXT, List<Hash>, List<T>> fetcher,
      Function<T, List<Hash>> nextPage) {
    return logFetcherCommon(ctx, Collections.singletonList(initial), fetcher, nextPage);
  }

  protected <T> Spliterator<T> logFetcherWithPage(
      OP_CONTEXT ctx,
      List<Hash> initialPage,
      BiFunction<OP_CONTEXT, List<Hash>, List<T>> fetcher,
      Function<T, List<Hash>> nextPage) {
    return logFetcherCommon(ctx, fetcher.apply(ctx, initialPage), fetcher, nextPage);
  }

  private <T> Spliterator<T> logFetcherCommon(
      OP_CONTEXT ctx,
      List<T> initial,
      BiFunction<OP_CONTEXT, List<Hash>, List<T>> fetcher,
      Function<T, List<Hash>> nextPage) {
    return new AbstractSpliterator<T>(Long.MAX_VALUE, 0) {
      private Iterator<T> currentBatch;
      private boolean eof;
      private T previous;

      @Override
      public boolean tryAdvance(Consumer<? super T> consumer) {
        if (eof) {
          return false;
        } else if (currentBatch == null) {
          currentBatch = initial.iterator();
        } else if (!currentBatch.hasNext()) {
          if (previous == null) {
            eof = true;
            return false;
          }
          List<Hash> page = nextPage.apply(previous);
          previous = null;
          if (!page.isEmpty()) {
            currentBatch = fetcher.apply(ctx, page).iterator();
            if (!currentBatch.hasNext()) {
              eof = true;
              return false;
            }
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
      long commitSeq,
      ByteString commitMeta,
      Iterable<KeyWithBytes> puts,
      Set<ContentKey> deletes,
      int currentKeyListDistance,
      Consumer<Hash> newKeyLists,
      @Nonnull @jakarta.annotation.Nonnull Function<Hash, CommitLogEntry> inMemoryCommits,
      Iterable<Hash> additionalParents)
      throws ReferenceNotFoundException, ReferenceConflictException {
    Hash commitHash = individualCommitHash(parentHashes, commitMeta, puts, deletes);

    namespacesValidationForCommit(ctx, parentHashes, puts, deletes, inMemoryCommits);

    int keyListDistance = currentKeyListDistance + 1;

    CommitLogEntry entry =
        CommitLogEntry.of(
            timeInMicros,
            commitHash,
            commitSeq,
            parentHashes,
            commitMeta,
            puts,
            deletes,
            keyListDistance,
            null,
            emptyList(),
            emptyList(),
            additionalParents);

    if (keyListDistance >= config.getKeyListDistance()) {
      entry = buildKeyList(ctx, entry, newKeyLists, inMemoryCommits);
    }
    return entry;
  }

  private void namespacesValidationForCommit(
      OP_CONTEXT ctx,
      List<Hash> parentHashes,
      Iterable<KeyWithBytes> puts,
      Set<ContentKey> deletes,
      Function<Hash, CommitLogEntry> inMemoryCommits)
      throws ReferenceNotFoundException, ReferenceConflictException {
    if (!config.validateNamespaces()) {
      return;
    }

    // Collect the keys that need to be checked for being a namespace, and the payload per
    // content-key
    Set<ContentKey> keysToCheck = new HashSet<>();
    Set<ContentKey> expectedNamespaces = new HashSet<>();
    Map<ContentKey, Byte> putPayloads = new HashMap<>();
    for (KeyWithBytes put : puts) {
      ContentKey key = put.getKey();
      keysToCheck.add(key);
      if (key.getElementCount() > 1) {
        ContentKey parent = key.getParent();
        keysToCheck.add(parent);
        expectedNamespaces.add(parent);
      }
      putPayloads.put(key, put.getPayload());
    }

    // Fetch the contents for the keys to be checked, included the keys to be deleted
    Set<ContentKey> keysToFetch = new HashSet<>(keysToCheck);
    keysToFetch.addAll(deletes);

    Map<ContentKey, Byte> fetchedPayloads;
    try (Stream<KeyListEntry> keysStream =
        keysForCommitEntry(
            ctx,
            parentHashes.get(0),
            (key, contentId, type) -> {
              // Key is required either by a put-operation's parent key or a delete-operation
              if (keysToFetch.contains(key)) {
                return true;
              }

              // Also include the keys that are children of the deleted keys
              int keyLen = key.getElementCount();
              for (ContentKey deleted : deletes) {
                int deletedLen = deleted.getElementCount();
                if (keyLen > deletedLen && key.startsWith(deleted)) {
                  return true;
                }
              }
              return false;
            },
            inMemoryCommits)) {
      fetchedPayloads =
          keysStream.collect(Collectors.toMap(KeyListEntry::getKey, KeyListEntry::getPayload));
    }

    int namespacePayload = payloadForContent(Content.Type.NAMESPACE);

    Set<Conflict> conflicts = new LinkedHashSet<>();

    for (ContentKey key : keysToCheck) {
      Byte payloadInPut = putPayloads.get(key);

      // Figure out the payload of the keys to be checked, and whether those exist
      byte payload;
      boolean exists = payloadInPut != null;
      Byte existingContentPayload = fetchedPayloads.get(key);
      if (!exists) {
        exists = existingContentPayload != null;
        payload = exists ? existingContentPayload : (byte) 0;
      } else {
        payload = payloadInPut;
      }

      // Validate that the payload did not change
      if (existingContentPayload != null && payload != existingContentPayload) {
        throw new IllegalArgumentException(
            format(
                "Cannot overwrite existing key '%s' of type %s with a different payload %s",
                key,
                contentTypeForPayload(existingContentPayload),
                contentTypeForPayload(payload)));
      }

      // Validate that each content-key's parent namespace exists
      if (expectedNamespaces.contains(key)) {
        if (!exists) {
          conflicts.add(conflict(NAMESPACE_ABSENT, key, format("namespace '%s' must exist", key)));
        } else if (payload != namespacePayload) {
          conflicts.add(
              conflict(
                  NOT_A_NAMESPACE,
                  key,
                  format(
                      "expecting the key '%s' to be a namespace, but is not a namespace "
                          + "(using a content object that is not a namespace as a namespace is forbidden)",
                      key)));
        }
      }
    }

    // Validate that deleted namespaces do not have children
    for (ContentKey deleted : deletes) {
      Byte deletedPayload = fetchedPayloads.get(deleted);
      if (deletedPayload == null) {
        // oh, deleted content does not exist - bummer
        continue;
      }

      if (deletedPayload != namespacePayload) {
        // not a namespace, go ahead
        continue;
      }

      int nsLen = deleted.getElementCount();

      for (ContentKey ck : fetchedPayloads.keySet()) {
        if (deletes.contains(ck)) {
          // this key is being deleted as well - ignore
          continue;
        }

        int ckLen = ck.getElementCount();

        // check if element is in the current namespace, fail it is true - this means,
        // there is a live content-key in the current namespace - must not delete the namespace
        if (ckLen > nsLen && ck.startsWith(deleted)) {
          conflicts.add(
              conflict(
                  NAMESPACE_NOT_EMPTY, deleted, format("namespace '%s' is not empty", deleted)));
        }
      }
    }

    if (!conflicts.isEmpty()) {
      throw new ReferenceConflictException(conflicts);
    }
  }

  /** Calculate the hash for the content of a {@link CommitLogEntry}. */
  @SuppressWarnings("UnstableApiUsage")
  protected Hash individualCommitHash(
      List<Hash> parentHashes,
      ByteString commitMeta,
      Iterable<KeyWithBytes> puts,
      Iterable<ContentKey> deletes) {
    Hasher hasher = newHasher();
    hasher.putLong(COMMIT_LOG_HASH_SEED);
    parentHashes.forEach(h -> hasher.putBytes(h.asBytes().asReadOnlyByteBuffer()));
    hasher.putBytes(commitMeta.asReadOnlyByteBuffer());
    puts.forEach(
        e -> {
          hashKey(hasher, e.getKey());
          hasher.putString(e.getContentId().getId(), StandardCharsets.UTF_8);
          hasher.putBytes(e.getValue().asReadOnlyByteBuffer());
        });
    deletes.forEach(e -> hashKey(hasher, e));
    return Hash.of(UnsafeByteOperations.unsafeWrap(hasher.hash().asBytes()));
  }

  /**
   * Adds a complete key-list to the given {@link CommitLogEntry}, will read from the database.
   *
   * <p>The implementation fills {@link CommitLogEntry#getKeyList()} with the most recently updated
   * {@link ContentKey}s.
   *
   * <p>If the calculated size of the database-object/row gets larger than {@link
   * DatabaseAdapterConfig#getMaxKeyListSize()}, the next {@link ContentKey}s will be added to new
   * {@link KeyListEntity}s, each with a maximum size of {@link
   * DatabaseAdapterConfig#getMaxKeyListSize()}.
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
      OP_CONTEXT ctx,
      CommitLogEntry unwrittenEntry,
      Consumer<Hash> newKeyLists,
      @Nonnull @jakarta.annotation.Nonnull Function<Hash, CommitLogEntry> inMemoryCommits)
      throws ReferenceNotFoundException {
    // Read commit-log until the previous persisted key-list

    Hash startHash = unwrittenEntry.getParents().get(0);

    // Return the new commit-log-entry with the complete-key-list
    ImmutableCommitLogEntry.Builder newCommitEntry =
        ImmutableCommitLogEntry.builder()
            .from(unwrittenEntry)
            .keyListDistance(0)
            .keyListVariant(KeyListVariant.OPEN_ADDRESSING);

    KeyListBuildState buildState =
        new KeyListBuildState(
            newCommitEntry,
            maxEntitySize(config.getMaxKeyListSize()) - entitySize(unwrittenEntry),
            maxEntitySize(config.getMaxKeyListEntitySize()),
            config.getKeyListHashLoadFactor(),
            this::entitySize);

    Set<ContentKey> keysToEnhanceWithCommitId = new HashSet<>();

    try (Stream<KeyListEntry> keys = keysForCommitEntry(ctx, startHash, null, inMemoryCommits)) {
      keys.forEach(
          keyListEntry -> {
            if (keyListEntry.getCommitId() == null) {
              keysToEnhanceWithCommitId.add(keyListEntry.getKey());
            } else {
              buildState.add(keyListEntry);
            }
          });
    }

    if (!keysToEnhanceWithCommitId.isEmpty()) {
      // Found KeyListEntry w/o commitId, need to add that information.
      Spliterator<CommitLogEntry> clSplit = readCommitLog(ctx, startHash, inMemoryCommits);
      while (true) {
        boolean more =
            clSplit.tryAdvance(
                e -> {
                  e.getDeletes().forEach(keysToEnhanceWithCommitId::remove);
                  for (KeyWithBytes put : e.getPuts()) {
                    if (keysToEnhanceWithCommitId.remove(put.getKey())) {
                      KeyListEntry entry =
                          KeyListEntry.of(
                              put.getKey(), put.getContentId(), put.getPayload(), e.getHash());
                      buildState.add(entry);
                    }
                  }
                });
        if (!more || keysToEnhanceWithCommitId.isEmpty()) {
          break;
        }
      }
    }

    List<KeyListEntity> newKeyListEntities = buildState.finish();

    // Inform the (CAS)-op-loop about the IDs of the KeyListEntities being optimistically written.
    newKeyListEntities.stream().map(KeyListEntity::getId).forEach(newKeyLists);

    // Write the new KeyListEntities
    if (!newKeyListEntities.isEmpty()) {
      writeKeyListEntities(ctx, newKeyListEntities);
    }

    // Return the new commit-log-entry with the complete-key-list
    return newCommitEntry.build();
  }

  protected int maxEntitySize(int value) {
    return value;
  }

  /** Calculate the expected size of the given {@link CommitLogEntry} in the database (in bytes). */
  protected abstract int entitySize(CommitLogEntry entry);

  /** Calculate the expected size of the given {@link CommitLogEntry} in the database (in bytes). */
  protected abstract int entitySize(KeyListEntry entry);

  /**
   * If the current HEAD of the target branch for a commit/transplant/merge is not equal to the
   * expected/reference HEAD, verify that there is no conflict, like keys in the operations of the
   * commit(s) contained in keys of the commits 'expectedHead (excluding) .. currentHead
   * (including)'.
   *
   * @return commit log entry at {@code branchHead}
   */
  protected CommitLogEntry checkForModifiedKeysBetweenExpectedAndCurrentCommit(
      OP_CONTEXT ctx, CommitParams commitParams, Hash branchHead, List<String> mismatches)
      throws ReferenceNotFoundException {

    CommitLogEntry commitAtHead = null;
    if (commitParams.getExpectedHead().isPresent()) {
      Hash expectedHead = commitParams.getExpectedHead().get();
      if (!expectedHead.equals(branchHead)) {
        Set<ContentKey> operationKeys =
            Sets.newHashSetWithExpectedSize(
                commitParams.getDeletes().size()
                    + commitParams.getUnchanged().size()
                    + commitParams.getPuts().size());
        operationKeys.addAll(commitParams.getDeletes());
        operationKeys.addAll(commitParams.getUnchanged());
        commitParams.getPuts().stream().map(KeyWithBytes::getKey).forEach(operationKeys::add);

        ConflictingKeyCheckResult conflictingKeyCheckResult =
            checkConflictingKeysForCommit(
                ctx, branchHead, expectedHead, operationKeys, mismatches::add);

        // If the expectedHead is the special value NO_ANCESTOR, which is not persisted,
        // ignore the fact that it has not been seen. Otherwise, raise a
        // ReferenceNotFoundException that the expected-hash does not exist on the target
        // branch.
        if (!conflictingKeyCheckResult.sinceSeen && !expectedHead.equals(NO_ANCESTOR)) {
          throw hashNotFound(commitParams.getToBranch(), expectedHead);
        }

        commitAtHead = conflictingKeyCheckResult.headCommit;
      }
    }

    if (commitAtHead == null) {
      commitAtHead = fetchFromCommitLog(ctx, branchHead);
    }
    return commitAtHead;
  }

  /** Retrieve the content-keys and their types for the commit-log-entry with the given hash. */
  @MustBeClosed
  protected Stream<KeyListEntry> keysForCommitEntry(
      OP_CONTEXT ctx, Hash hash, KeyFilterPredicate keyFilter) throws ReferenceNotFoundException {
    return keysForCommitEntry(ctx, hash, keyFilter, h -> null);
  }

  /** Retrieve the content-keys and their types for the commit-log-entry with the given hash. */
  @MustBeClosed
  protected Stream<KeyListEntry> keysForCommitEntry(
      OP_CONTEXT ctx,
      Hash hash,
      KeyFilterPredicate keyFilter,
      @Nonnull @jakarta.annotation.Nonnull Function<Hash, CommitLogEntry> inMemoryCommits)
      throws ReferenceNotFoundException {
    // walk the commit-logs in reverse order - starting with the last persisted key-list

    Set<ContentKey> seen = new HashSet<>();

    Predicate<KeyListEntry> predicate =
        keyListEntry -> keyListEntry != null && seen.add(keyListEntry.getKey());
    if (keyFilter != null) {
      predicate =
          predicate.and(kt -> keyFilter.check(kt.getKey(), kt.getContentId(), kt.getPayload()));
    }
    Predicate<KeyListEntry> keyPredicate = predicate;

    @SuppressWarnings("MustBeClosedChecker")
    Stream<CommitLogEntry> log =
        takeUntilIncludeLast(
            readCommitLogStream(ctx, hash, inMemoryCommits), CommitLogEntry::hasKeySummary);
    return log.flatMap(
        e -> {

          // Add CommitLogEntry.deletes to "seen" so these keys won't be returned
          seen.addAll(e.getDeletes());

          // Return from CommitLogEntry.puts first
          Stream<KeyListEntry> stream =
              e.getPuts().stream()
                  .map(
                      put ->
                          KeyListEntry.of(
                              put.getKey(), put.getContentId(), put.getPayload(), e.getHash()))
                  .filter(keyPredicate);

          if (e.hasKeySummary()) {
            // Return from CommitLogEntry.keyList after the keys in CommitLogEntry.puts
            KeyList embeddedKeyList = e.getKeyList();
            if (embeddedKeyList != null) {
              Stream<KeyListEntry> embedded =
                  embeddedKeyList.getKeys().stream().filter(keyPredicate);
              stream = Stream.concat(stream, embedded);
            }

            List<Hash> keyListIds = e.getKeyListsIds();
            if (keyListIds != null && !keyListIds.isEmpty()) {
              // If there are nested key-lists, retrieve those lazily and add the keys from these

              Stream<KeyListEntry> entities =
                  Stream.of(keyListIds)
                      .flatMap(
                          ids -> {
                            @SuppressWarnings("MustBeClosedChecker")
                            Stream<KeyListEntity> r = fetchKeyLists(ctx, ids);
                            return r;
                          })
                      .map(KeyListEntity::getKeys)
                      .map(KeyList::getKeys)
                      .flatMap(Collection::stream)
                      .filter(keyPredicate);
              stream = Stream.concat(stream, entities);
            }
          }

          return stream;
        });
  }

  /**
   * Fetch the global-state and per-ref content for the given {@link ContentKey}s and {@link Hash
   * commitSha}. Non-existing keys must not be present in the returned map.
   */
  protected Map<ContentKey, ContentAndState> fetchValues(
      OP_CONTEXT ctx, Hash refHead, Collection<ContentKey> keys, KeyFilterPredicate keyFilter)
      throws ReferenceNotFoundException {
    Set<ContentKey> remainingKeys = new HashSet<>(keys);

    Map<ContentKey, ContentAndState> nonGlobal = new HashMap<>();
    Map<ContentKey, ContentId> keyToContentIds = new HashMap<>();
    Set<ContentId> contentIdsForGlobal = new HashSet<>();

    Consumer<CommitLogEntry> commitLogEntryHandler =
        entry -> {
          // remove deleted keys from keys to look for
          entry.getDeletes().forEach(remainingKeys::remove);

          // handle put operations
          for (KeyWithBytes put : entry.getPuts()) {
            if (!remainingKeys.remove(put.getKey())) {
              continue;
            }

            if (!keyFilter.check(put.getKey(), put.getContentId(), put.getPayload())) {
              continue;
            }
            nonGlobal.put(put.getKey(), ContentAndState.of(put.getPayload(), put.getValue()));
            keyToContentIds.put(put.getKey(), put.getContentId());
            if (STORE_WORKER.requiresGlobalState(put.getPayload(), put.getValue())) {
              contentIdsForGlobal.add(put.getContentId());
            }
          }
        };

    // The algorithm is a bit complex, but not horribly complex.
    //
    // The commit-log `Stream` in the following try-with-resources fetches the commit-log until
    // all requested keys have been seen.
    //
    // When a commit-log-entry with key-lists is encountered, use the key-lists to determine if
    // and how the remaining keys need to be retrieved.
    // - If any of the requested keys is not in the key-lists, ignore it - it doesn't exist.
    // - If the `KeyListEntry` for a requested key contains the commit-ID, use the commit-ID to
    //   directly access the commit that contains the `Put` operation for that key.
    //
    // Both the "outer" `Stream` and the result of the latter case (list of commit-log-entries)
    // share common functionality implemented in the function `commitLogEntryHandler`, which
    // handles the 'Put` operations.

    AtomicBoolean keyListProcessed = new AtomicBoolean();
    try (Stream<CommitLogEntry> baseLog = readCommitLogStream(ctx, refHead);
        Stream<CommitLogEntry> log = takeUntilExcludeLast(baseLog, e -> remainingKeys.isEmpty())) {
      log.forEach(
          entry -> {
            commitLogEntryHandler.accept(entry);

            if (entry.hasKeySummary() && keyListProcessed.compareAndSet(false, true)) {
              // CommitLogEntry has a KeyList.
              // All keys in 'remainingKeys', that are _not_ in the KeyList(s), can be removed,
              // because at this point we know that these do not exist.
              //
              // But do only process the "newest" key-list - older key lists are irrelevant.
              // KeyListEntry written before Nessie 0.22.0 do not have the commit-ID field set,
              // which means the remaining commit-log needs to be searched for the commit that
              // added the key - but processing older key-lists "on the way" makes no sense,
              // because those will not have the key either.

              if (!remainingKeys.isEmpty()) {
                List<KeyListEntry> keyListEntries;
                switch (entry.getKeyListVariant()) {
                  case OPEN_ADDRESSING:
                    keyListEntries =
                        fetchValuesHandleOpenAddressingKeyList(ctx, remainingKeys, entry);
                    break;
                  case EMBEDDED_AND_EXTERNAL_MRU:
                    keyListEntries = fetchValuesHandleKeyList(ctx, remainingKeys, entry);
                    break;
                  default:
                    throw new IllegalStateException(
                        "Unknown key list variant " + entry.getKeyListVariant());
                }

                if (!keyListEntries.isEmpty()) {
                  List<CommitLogEntry> commitLogEntries =
                      fetchMultipleFromCommitLog(
                          ctx,
                          keyListEntries.stream()
                              .map(KeyListEntry::getCommitId)
                              .filter(Objects::nonNull)
                              .distinct()
                              .collect(Collectors.toList()),
                          h -> null);
                  commitLogEntries.forEach(commitLogEntryHandler);
                }

                // Older Nessie versions before 0.22.0 did not record the commit-ID for a key
                // so that we have to continue scanning the commit log for the remaining keys.
                // Newer Nessie versions since 0.22.0 do record the commit-ID, so we can safely
                // assume that remainingKeys do not exist.
                remainingKeys.retainAll(
                    keyListEntries.stream()
                        .filter(e -> e.getCommitId() == null)
                        .map(KeyListEntry::getKey)
                        .collect(Collectors.toSet()));
              }
            }
          });
    }

    Map<ContentId, ByteString> globals =
        contentIdsForGlobal.isEmpty()
            ? Collections.emptyMap()
            : fetchGlobalStates(ctx, contentIdsForGlobal);

    return nonGlobal.entrySet().stream()
        .collect(
            Collectors.toMap(
                Entry::getKey,
                e -> {
                  ContentAndState cs = e.getValue();
                  ByteString global = globals.get(keyToContentIds.get(e.getKey()));
                  return global != null
                      ? ContentAndState.of(cs.getPayload(), cs.getRefState(), global)
                      : cs;
                }));
  }

  /**
   * Handles key lists written with {@link CommitLogEntry.KeyListVariant#OPEN_ADDRESSING} (since
   * Nessie 0.31.0).
   */
  private List<KeyListEntry> fetchValuesHandleOpenAddressingKeyList(
      OP_CONTEXT ctx, Collection<ContentKey> remainingKeys, CommitLogEntry entry) {
    FetchValuesUsingOpenAddressing helper = new FetchValuesUsingOpenAddressing(entry);

    List<KeyListEntry> keyListEntries = new ArrayList<>();

    // If one or more `Key`s could not be immediately found in their "natural" segment in round 0,
    // because the end of the segment was hit, the next segment(s) will be consulted for the
    // remaining keys. Each "wrap around" increments the 'round'.
    for (int round = 0; !remainingKeys.isEmpty(); round++) {
      // Figure out the key-list-entities to load
      List<Hash> entitiesToFetch =
          helper.entityIdsToFetch(round, config.getKeyListEntityPrefetch(), remainingKeys);

      // Fetch the key-list-entities for the identified segments, store those
      if (!entitiesToFetch.isEmpty()) {
        try (Stream<KeyListEntity> keyLists = fetchKeyLists(ctx, entitiesToFetch)) {
          keyLists.forEach(helper::entityLoaded);
        }
      }

      // Try to extract the key-list-entries for the remainingKeys and add to the result list
      remainingKeys = helper.checkForKeys(round, remainingKeys, keyListEntries::add);
    }

    return keyListEntries;
  }

  /**
   * Handles key lists written with {@link CommitLogEntry.KeyListVariant#EMBEDDED_AND_EXTERNAL_MRU}
   * (before Nessie 0.31.0).
   */
  private List<KeyListEntry> fetchValuesHandleKeyList(
      OP_CONTEXT ctx, Set<ContentKey> remainingKeys, CommitLogEntry entry) {
    List<KeyListEntry> keyListEntries = new ArrayList<>();

    try (Stream<KeyList> keyLists = keyListsFromCommitLogEntry(ctx, entry)) {
      keyLists
          .flatMap(keyList -> keyList.getKeys().stream())
          .filter(Objects::nonNull)
          .filter(keyListEntry -> remainingKeys.contains(keyListEntry.getKey()))
          .forEach(keyListEntries::add);
    }

    return keyListEntries;
  }

  @MustBeClosed
  private Stream<KeyList> keyListsFromCommitLogEntry(OP_CONTEXT ctx, CommitLogEntry entry) {
    KeyList embeddedKeyList = entry.getKeyList();

    Stream<KeyList> keyList = embeddedKeyList != null ? Stream.of(embeddedKeyList) : Stream.empty();

    List<Hash> keyListIds = entry.getKeyListsIds();
    if (keyListIds != null && !keyListIds.isEmpty()) {
      keyList =
          Stream.concat(
              Stream.of(entry.getKeyList()),
              Stream.of(keyListIds)
                  // lazy fetch
                  .flatMap(
                      ids -> {
                        @SuppressWarnings("MustBeClosedChecker")
                        Stream<KeyList> r = fetchKeyLists(ctx, ids).map(KeyListEntity::getKeys);
                        return r;
                      }));
    }

    return keyList;
  }

  /**
   * Fetches the global-state information for the given content-ids.
   *
   * @param ctx technical context
   * @param contentIds the content-ids to fetch
   * @return map of content-id to state
   */
  protected final Map<ContentId, ByteString> fetchGlobalStates(
      OP_CONTEXT ctx, Set<ContentId> contentIds) throws ReferenceNotFoundException {
    try (Traced ignore = trace("fetchGlobalStates").tag(TAG_COUNT, contentIds.size())) {
      return doFetchGlobalStates(ctx, contentIds);
    }
  }

  protected abstract Map<ContentId, ByteString> doFetchGlobalStates(
      OP_CONTEXT ctx, Set<ContentId> contentIds) throws ReferenceNotFoundException;

  @VisibleForTesting
  @MustBeClosed
  public final Stream<KeyListEntity> fetchKeyLists(OP_CONTEXT ctx, List<Hash> keyListsIds) {
    if (keyListsIds.isEmpty()) {
      return Stream.empty();
    }
    try (Traced ignore = trace("fetchKeyLists").tag(TAG_COUNT, keyListsIds.size())) {
      return doFetchKeyLists(ctx, keyListsIds);
    }
  }

  @MustBeClosed
  protected abstract Stream<KeyListEntity> doFetchKeyLists(OP_CONTEXT ctx, List<Hash> keyListsIds);

  /**
   * Write a new commit-entry, the given commit entry is to be persisted as is. All values of the
   * given {@link CommitLogEntry} can be considered valid and consistent.
   *
   * <p>Implementations however can enforce strict consistency checks/guarantees, like a best-effort
   * approach to prevent hash-collisions but without any other consistency checks/guarantees.
   */
  protected final void writeIndividualCommit(OP_CONTEXT ctx, CommitLogEntry entry)
      throws ReferenceConflictException {
    try (Traced ignore = trace("writeIndividualCommit")) {
      doWriteIndividualCommit(ctx, entry);
    }
  }

  protected abstract void doWriteIndividualCommit(OP_CONTEXT ctx, CommitLogEntry entry)
      throws ReferenceConflictException;

  protected final void writeMultipleCommits(OP_CONTEXT ctx, List<CommitLogEntry> entries)
      throws ReferenceConflictException {
    try (Traced ignore = trace("writeMultipleCommits").tag(TAG_COUNT, entries.size())) {
      doWriteMultipleCommits(ctx, entries);
    }
  }

  protected abstract void doWriteMultipleCommits(OP_CONTEXT ctx, List<CommitLogEntry> entries)
      throws ReferenceConflictException;

  protected abstract void doUpdateMultipleCommits(OP_CONTEXT ctx, List<CommitLogEntry> entries)
      throws ReferenceNotFoundException;

  @VisibleForTesting
  public final void writeKeyListEntities(OP_CONTEXT ctx, List<KeyListEntity> newKeyListEntities) {
    try (Traced ignore = trace("writeKeyListEntities").tag(TAG_COUNT, newKeyListEntities.size())) {
      doWriteKeyListEntities(ctx, newKeyListEntities);
    }
  }

  protected abstract void doWriteKeyListEntities(
      OP_CONTEXT ctx, List<KeyListEntity> newKeyListEntities);

  protected static final class ConflictingKeyCheckResult {
    private boolean sinceSeen;
    private CommitLogEntry headCommit;

    public boolean isSinceSeen() {
      return sinceSeen;
    }

    public CommitLogEntry getHeadCommit() {
      return headCommit;
    }
  }

  /**
   * Check whether the commits in the range {@code sinceCommitExcluding] .. [upToCommitIncluding}
   * contain any of the given {@link ContentKey}s.
   *
   * <p>Conflicts are reported via {@code mismatches}.
   */
  protected ConflictingKeyCheckResult checkConflictingKeysForCommit(
      OP_CONTEXT ctx,
      Hash upToCommitIncluding,
      Hash sinceCommitExcluding,
      Set<ContentKey> keys,
      Consumer<String> mismatches)
      throws ReferenceNotFoundException {
    ConflictingKeyCheckResult result = new ConflictingKeyCheckResult();

    try (Stream<CommitLogEntry> commits = readCommitLogStream(ctx, upToCommitIncluding)) {
      Stream<CommitLogEntry> log =
          takeUntilExcludeLast(
              commits,
              e -> {
                if (e.getHash().equals(upToCommitIncluding)) {
                  result.headCommit = e;
                }
                if (e.getHash().equals(sinceCommitExcluding)) {
                  result.sinceSeen = true;
                  return true;
                }
                return false;
              });

      Set<ContentKey> handled = new HashSet<>();
      log.forEach(
          e -> {
            e.getPuts()
                .forEach(
                    a -> {
                      if (keys.contains(a.getKey()) && handled.add(a.getKey())) {
                        mismatches.accept(
                            format(
                                "Key '%s' has conflicting put-operation from commit '%s'.",
                                a.getKey(), e.getHash().asString()));
                      }
                    });
            e.getDeletes()
                .forEach(
                    a -> {
                      if (keys.contains(a) && handled.add(a)) {
                        mismatches.accept(
                            format(
                                "Key '%s' has conflicting delete-operation from commit '%s'.",
                                a, e.getHash().asString()));
                      }
                    });
          });
    }

    return result;
  }

  protected final class CommonAncestorState {
    final Iterator<Hash> toLog;
    final List<Hash> toCommitHashesList;
    final Set<Hash> toCommitHashes = new HashSet<>();

    public CommonAncestorState(OP_CONTEXT ctx, Hash toHead, boolean trackCount) {
      this.toLog = Spliterators.iterator(readCommitLogHashes(ctx, toHead));
      this.toCommitHashesList = trackCount ? new ArrayList<>() : null;
    }

    boolean fetchNext() {
      if (toLog.hasNext()) {
        Hash hash = toLog.next();
        toCommitHashes.add(hash);
        if (toCommitHashesList != null) {
          toCommitHashesList.add(hash);
        }
        return true;
      }
      return false;
    }

    public boolean contains(Hash candidate) {
      return toCommitHashes.contains(candidate);
    }

    public int indexOf(Hash hash) {
      return toCommitHashesList.indexOf(hash);
    }
  }

  /**
   * Finds the common-ancestor of two commit-log-entries. If no common-ancestor is found, throws a
   * {@link ReferenceConflictException} or. Otherwise, this method returns the hash of the
   * common-ancestor.
   */
  protected Hash findCommonAncestor(OP_CONTEXT ctx, Hash from, NamedRef toBranch, Hash toHead)
      throws ReferenceConflictException {

    // TODO this implementation requires guardrails:
    //  max number of "to"-commits to fetch, max number of "from"-commits to fetch,
    //  both impact the cost (CPU, memory, I/O) of a merge operation.

    CommonAncestorState commonAncestorState = new CommonAncestorState(ctx, toHead, false);

    Hash commonAncestorHash =
        findCommonAncestor(ctx, from, commonAncestorState, (dist, hash) -> hash);
    if (commonAncestorHash == null) {
      throw new ReferenceConflictException(
          format(
              "No common ancestor found for merge of '%s' into branch '%s' @ '%s'",
              from, toBranch.getName(), toHead.asString()));
    }
    return commonAncestorHash;
  }

  protected <R> R findCommonAncestor(
      OP_CONTEXT ctx, Hash from, CommonAncestorState state, BiFunction<Integer, Hash, R> result) {
    Iterator<Hash> fromLog = Spliterators.iterator(readCommitLogHashes(ctx, from));
    List<Hash> fromCommitHashes = new ArrayList<>();
    while (true) {
      boolean anyFetched = false;
      for (int i = 0; i < config.getParentsPerCommit(); i++) {
        if (state.fetchNext()) {
          anyFetched = true;
        }
        if (fromLog.hasNext()) {
          fromCommitHashes.add(fromLog.next());
          anyFetched = true;
        }
      }
      if (!anyFetched) {
        return null;
      }

      for (int diffOnFrom = 0; diffOnFrom < fromCommitHashes.size(); diffOnFrom++) {
        Hash f = fromCommitHashes.get(diffOnFrom);
        if (state.contains(f)) {
          return result.apply(diffOnFrom, f);
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
  protected boolean hasKeyCollisions(
      OP_CONTEXT ctx,
      Hash refHead,
      Set<ContentKey> keysTouchedOnTarget,
      List<CommitLogEntry> commitsChronological,
      Function<ContentKey, ImmutableKeyDetails.Builder> keyDetails)
      throws ReferenceNotFoundException {
    Set<ContentKey> keyCollisions = new HashSet<>();
    for (int i = commitsChronological.size() - 1; i >= 0; i--) {
      CommitLogEntry sourceCommit = commitsChronological.get(i);
      Stream.concat(
              sourceCommit.getPuts().stream().map(KeyWithBytes::getKey),
              sourceCommit.getDeletes().stream())
          .filter(keysTouchedOnTarget::contains)
          .forEach(keyCollisions::add);
    }

    if (!keyCollisions.isEmpty()) {
      removeKeyCollisionsForNamespaces(
          ctx, refHead, commitsChronological.get(0).getHash(), keyCollisions);
      if (!keyCollisions.isEmpty()) {
        keyCollisions.forEach(
            key ->
                keyDetails
                    .apply(key)
                    .conflict(Conflict.conflict(Conflict.ConflictType.UNKNOWN, key, "UNRESOLVABLE"))
                    .conflictType(ConflictType.UNRESOLVABLE));
        return true;
      }
    }
    return false;
  }

  /**
   * If any key collision was found, we need to check whether the key collision was happening on a
   * Namespace and if so, remove that key collision, since Namespaces can be merged/transplanted
   * without problems.
   *
   * @param ctx The context
   * @param hashFromTarget The hash from the target branch
   * @param hashFromSource The hash from the source branch
   * @param keyCollisions The found key collisions
   * @throws ReferenceNotFoundException If the given reference could not be found
   */
  private void removeKeyCollisionsForNamespaces(
      OP_CONTEXT ctx, Hash hashFromTarget, Hash hashFromSource, Set<ContentKey> keyCollisions)
      throws ReferenceNotFoundException {
    Predicate<Entry<ContentKey, ContentAndState>> isNamespace =
        e ->
            STORE_WORKER
                .getType(e.getValue().getPayload(), e.getValue().getRefState())
                .equals(Content.Type.NAMESPACE);
    Set<ContentKey> namespacesOnTarget =
        fetchValues(ctx, hashFromTarget, keyCollisions, ALLOW_ALL).entrySet().stream()
            .filter(isNamespace)
            .map(Entry::getKey)
            .collect(Collectors.toSet());

    // this will be an implicit set intersection between the namespaces on source & target
    Set<ContentKey> intersection =
        fetchValues(ctx, hashFromSource, namespacesOnTarget, ALLOW_ALL).entrySet().stream()
            .filter(isNamespace)
            .map(Entry::getKey)
            .collect(Collectors.toSet());

    // remove all keys related to namespaces from the existing collisions
    intersection.forEach(keyCollisions::remove);
  }

  /**
   * For merge/transplant, applies one squashed commit derived from the given commits onto the
   * target-hash.
   */
  protected CommitLogEntry squashCommits(
      OP_CONTEXT ctx,
      long timeInMicros,
      Hash toHead,
      List<CommitLogEntry> commitsToMergeChronological,
      Consumer<Hash> newKeyLists,
      MetadataRewriter<ByteString> rewriteMetadata,
      Predicate<ContentKey> includeKeyPredicate,
      Iterable<Hash> additionalParents,
      Consumer<CommitLogEntry> addedCommits)
      throws ReferenceConflictException, ReferenceNotFoundException {

    List<ByteString> commitMeta = new ArrayList<>();
    Map<ContentKey, KeyWithBytes> puts = new LinkedHashMap<>();
    Set<ContentKey> deletes = new LinkedHashSet<>();
    for (int i = commitsToMergeChronological.size() - 1; i >= 0; i--) {
      CommitLogEntry source = commitsToMergeChronological.get(i);
      for (ContentKey delete : source.getDeletes()) {
        if (includeKeyPredicate.test(delete)) {
          deletes.add(delete);
          puts.remove(delete);
        }
      }
      for (KeyWithBytes put : source.getPuts()) {
        if (includeKeyPredicate.test(put.getKey())) {
          deletes.remove(put.getKey());
          puts.put(put.getKey(), put);
        }
      }

      commitMeta.add(source.getMetadata());
    }

    if (puts.isEmpty() && deletes.isEmpty()) {
      // Copied commit will not contain any operation, skip.
      return null;
    }

    ByteString newCommitMeta = rewriteMetadata.squash(commitMeta, commitMeta.size());

    CommitLogEntry targetHeadCommit = fetchFromCommitLog(ctx, toHead);

    int parentsPerCommit = config.getParentsPerCommit();
    List<Hash> parents = new ArrayList<>(parentsPerCommit);
    parents.add(toHead);
    long commitSeq;
    int keyListDistance;
    if (targetHeadCommit != null) {
      List<Hash> p = targetHeadCommit.getParents();
      parents.addAll(p.subList(0, Math.min(p.size(), parentsPerCommit - 1)));
      commitSeq = targetHeadCommit.getCommitSeq() + 1;
      keyListDistance = targetHeadCommit.getKeyListDistance();
    } else {
      commitSeq = 1;
      keyListDistance = 0;
    }

    CommitLogEntry squashedCommit =
        buildIndividualCommit(
            ctx,
            timeInMicros,
            parents,
            commitSeq,
            newCommitMeta,
            puts.values(),
            deletes,
            keyListDistance,
            newKeyLists,
            h -> null,
            additionalParents);

    if (commitsToMergeChronological.size() == 1) {
      CommitLogEntry single = commitsToMergeChronological.get(0);
      if (squashedCommit.getHash().equals(single.getHash())
          && squashedCommit.getPuts().equals(single.getPuts())
          && squashedCommit.getDeletes().equals(single.getDeletes())
          && squashedCommit.getMetadata().equals(single.getMetadata())) {
        // Fast-forward merge
        return single;
      }
    }

    writeIndividualCommit(ctx, squashedCommit);
    addedCommits.accept(squashedCommit);

    return squashedCommit;
  }

  /** For merge/transplant, applies the given commits onto the target-hash. */
  protected Hash copyCommits(
      OP_CONTEXT ctx,
      long timeInMicros,
      Hash targetHead,
      List<CommitLogEntry> commitsChronological,
      Consumer<Hash> newKeyLists,
      MetadataRewriter<ByteString> rewriteMetadata,
      Predicate<ContentKey> includeKeyPredicate,
      Consumer<CommitLogEntry> addedCommits)
      throws ReferenceNotFoundException, ReferenceConflictException {
    int parentsPerCommit = config.getParentsPerCommit();

    List<Hash> parents = new ArrayList<>(parentsPerCommit);
    CommitLogEntry targetHeadCommit = fetchFromCommitLog(ctx, targetHead);
    long commitSeq;
    if (targetHeadCommit != null) {
      parents.addAll(targetHeadCommit.getParents());
      commitSeq = targetHeadCommit.getCommitSeq() + 1;
    } else {
      commitSeq = 1L;
    }

    int keyListDistance = targetHeadCommit != null ? targetHeadCommit.getKeyListDistance() : 0;

    Map<Hash, CommitLogEntry> unwrittenCommits = new HashMap<>();

    // Rewrite commits to transplant and store those in 'commitsToTransplantReverse'
    for (int i = commitsChronological.size() - 1; i >= 0; i--, commitSeq++) {
      CommitLogEntry sourceCommit = commitsChronological.get(i);

      List<KeyWithBytes> puts =
          sourceCommit.getPuts().stream()
              .filter(p -> includeKeyPredicate.test(p.getKey()))
              .collect(Collectors.toList());
      Set<ContentKey> deletes =
          sourceCommit.getDeletes().stream()
              .filter(includeKeyPredicate)
              .collect(Collectors.toSet());

      if (puts.isEmpty() && deletes.isEmpty()) {
        // Copied commit will not contain any operation, skip.
        commitsChronological.remove(i);
        continue;
      }

      while (parents.size() > parentsPerCommit - 1) {
        parents.remove(parentsPerCommit - 1);
      }
      if (parents.isEmpty()) {
        parents.add(targetHead);
      } else {
        parents.add(0, targetHead);
      }

      ByteString updatedMetadata = rewriteMetadata.rewriteSingle(sourceCommit.getMetadata());

      CommitLogEntry newEntry =
          buildIndividualCommit(
              ctx,
              timeInMicros,
              parents,
              commitSeq,
              updatedMetadata,
              puts,
              deletes,
              keyListDistance,
              newKeyLists,
              unwrittenCommits::get,
              emptyList());
      keyListDistance = newEntry.getKeyListDistance();

      unwrittenCommits.put(newEntry.getHash(), newEntry);

      if (!newEntry.getHash().equals(sourceCommit.getHash())) {
        commitsChronological.set(i, newEntry);
        addedCommits.accept(newEntry);
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
      OP_CONTEXT ctx, CommitParams commitParams, Consumer<String> mismatches)
      throws ReferenceNotFoundException {
    Map<ContentId, Optional<ByteString>> expectedStates = commitParams.getExpectedStates();
    if (expectedStates.isEmpty()) {
      return;
    }

    Map<ContentId, ByteString> globalStates = fetchGlobalStates(ctx, expectedStates.keySet());
    for (Entry<ContentId, Optional<ByteString>> expectedState : expectedStates.entrySet()) {
      ByteString currentState = globalStates.get(expectedState.getKey());
      if (currentState == null) {
        if (expectedState.getValue().isPresent()) {
          mismatches.accept(
              format("No current global-state for content-id '%s'.", expectedState.getKey()));
        }
      } else {
        if (!expectedState.getValue().isPresent()) {
          // This happens, when a table's being created on a branch, but that table already exists.
          mismatches.accept(
              format("Global-state for content-id '%s' already exists.", expectedState.getKey()));
        } else if (!currentState.equals(expectedState.getValue().get())) {
          mismatches.accept(
              format("Mismatch in global-state for content-id '%s'.", expectedState.getKey()));
        }
      }
    }
  }

  protected void tryLoopStateCompletion(
      @Nonnull @jakarta.annotation.Nonnull Boolean success, TryLoopState state) {
    tryLoopFinished(
        success ? "success" : "fail", state.getRetries(), state.getDuration(NANOSECONDS));
  }

  protected void repositoryEvent(Supplier<? extends AdapterEvent.Builder<?, ?>> eventBuilder) {
    if (eventConsumer != null && eventBuilder != null) {
      AdapterEvent event = eventBuilder.get().eventTimeMicros(config.currentTimeInMicros()).build();
      try {
        eventConsumer.accept(event);
      } catch (RuntimeException e) {
        repositoryEventDeliveryFailed(event, e);
      }
    }
  }

  private static void repositoryEventDeliveryFailed(AdapterEvent event, RuntimeException e) {
    LOGGER.warn(
        "Repository event delivery failed for operation type {}", event.getOperationType(), e);
    Tracer t = GlobalTracer.get();
    Span span = t.activeSpan();
    Span log =
        span.log(
            ImmutableMap.of(
                Fields.EVENT,
                Tags.ERROR.getKey(),
                Fields.MESSAGE,
                "Repository event delivery failed",
                Fields.ERROR_OBJECT,
                e));
    Tags.ERROR.set(log, true);
  }
}
