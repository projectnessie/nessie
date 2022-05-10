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
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.mergeConflictMessage;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.randomHash;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.referenceAlreadyExists;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.referenceNotFound;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.repoDescUpdateConflictMessage;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.takeUntilExcludeLast;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.transplantConflictMessage;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.verifyExpectedHash;
import static org.projectnessie.versioned.persist.adapter.spi.Traced.trace;
import static org.projectnessie.versioned.persist.adapter.spi.TryLoopState.newTryLoopState;
import static org.projectnessie.versioned.persist.nontx.NonTransactionalDatabaseAdapter.CasOpResult.casOpResult;
import static org.projectnessie.versioned.persist.nontx.NonTransactionalOperationContext.NON_TRANSACTIONAL_OPERATION_CONTEXT;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.Spliterators.AbstractSpliterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ImmutableMergeResult;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.MergeResult;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.RefLogNotFoundException;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.StoreWorker;
import org.projectnessie.versioned.TagName;
import org.projectnessie.versioned.VersionStoreException;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.CommitParams;
import org.projectnessie.versioned.persist.adapter.ContentAndState;
import org.projectnessie.versioned.persist.adapter.ContentId;
import org.projectnessie.versioned.persist.adapter.ContentIdAndBytes;
import org.projectnessie.versioned.persist.adapter.Difference;
import org.projectnessie.versioned.persist.adapter.KeyFilterPredicate;
import org.projectnessie.versioned.persist.adapter.KeyListEntity;
import org.projectnessie.versioned.persist.adapter.KeyListEntry;
import org.projectnessie.versioned.persist.adapter.MergeParams;
import org.projectnessie.versioned.persist.adapter.RefLog;
import org.projectnessie.versioned.persist.adapter.RepoDescription;
import org.projectnessie.versioned.persist.adapter.RepoMaintenanceParams;
import org.projectnessie.versioned.persist.adapter.TransplantParams;
import org.projectnessie.versioned.persist.adapter.serialize.ProtoSerialization;
import org.projectnessie.versioned.persist.adapter.spi.AbstractDatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.spi.Traced;
import org.projectnessie.versioned.persist.adapter.spi.TryLoopState;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.ContentIdWithBytes;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.GlobalStateLogEntry;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.GlobalStatePointer;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.NamedReference;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.RefLogEntry;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.RefLogEntry.Operation;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.RefLogParents;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.RefPointer;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.RefType;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.ReferenceNames;

/**
 * Non-transactional database-adapter implementation suitable for no-sql databases.
 *
 * <p>Relies on three main entities:
 *
 * <ul>
 *   <li><em>Global state pointer</em> points to the current HEAD in the <em>global state log</em>
 *       and also contains all named-references and their current HEADs.
 *   <li><em>Global state log entry</em> is organized as a linked list and contains the new global
 *       states for all content-keys and a (list of) its parents..
 *   <li><em>Commit log entry</em> is organized as a linked list and contains the changes to
 *       content-keys, the commit-metadata and a (list of) its parents.
 * </ul>
 */
public abstract class NonTransactionalDatabaseAdapter<
        CONFIG extends NonTransactionalDatabaseAdapterConfig>
    extends AbstractDatabaseAdapter<NonTransactionalOperationContext, CONFIG> {

  public static final String TAG_COMMIT_COUNT = "commit-count";
  public static final String TAG_KEY_LIST_COUNT = "key-list-count";
  public static final String TAG_REF = "ref";

  protected NonTransactionalDatabaseAdapter(CONFIG config, StoreWorker<?, ?, ?> storeWorker) {
    super(config, storeWorker);
  }

  @Override
  public NonTransactionalOperationContext borrowConnection() {
    return NON_TRANSACTIONAL_OPERATION_CONTEXT;
  }

  @Override
  public Hash hashOnReference(NamedRef namedReference, Optional<Hash> hashOnReference)
      throws ReferenceNotFoundException {
    return hashOnRef(NON_TRANSACTIONAL_OPERATION_CONTEXT, namedReference, hashOnReference);
  }

  @Override
  public Map<Key, ContentAndState<ByteString>> values(
      Hash commit, Collection<Key> keys, KeyFilterPredicate keyFilter)
      throws ReferenceNotFoundException {
    return fetchValues(NON_TRANSACTIONAL_OPERATION_CONTEXT, commit, keys, keyFilter);
  }

  @Override
  public Stream<CommitLogEntry> commitLog(Hash offset) throws ReferenceNotFoundException {
    return readCommitLogStream(NON_TRANSACTIONAL_OPERATION_CONTEXT, offset);
  }

  @Override
  public ReferenceInfo<ByteString> namedRef(String ref, GetNamedRefsParams params)
      throws ReferenceNotFoundException {
    Preconditions.checkNotNull(params, "Parameter for GetNamedRefsParams must not be null");

    NonTransactionalOperationContext ctx = NON_TRANSACTIONAL_OPERATION_CONTEXT;

    ReferenceInfo<ByteString> refHead = referenceHead(ctx, ref);
    Hash defaultBranchHead = namedRefsDefaultBranchHead(ctx, params);

    Stream<ReferenceInfo<ByteString>> refs = Stream.of(refHead);

    return namedRefsFilterAndEnhance(ctx, params, defaultBranchHead, refs)
        .findFirst()
        .orElseThrow(() -> referenceNotFound(ref));
  }

  @Override
  public Stream<ReferenceInfo<ByteString>> namedRefs(GetNamedRefsParams params)
      throws ReferenceNotFoundException {
    Preconditions.checkNotNull(params, "Parameter for GetNamedRefsParams must not be null.");
    Preconditions.checkArgument(
        namedRefsAnyRetrieves(params), "Must retrieve branches or tags or both.");

    NonTransactionalOperationContext ctx = NON_TRANSACTIONAL_OPERATION_CONTEXT;

    Stream<ReferenceInfo<ByteString>> refs =
        fetchNamedReferences(ctx)
            .map(NonTransactionalDatabaseAdapter::namedReferenceToReferenceInfo);

    Hash defaultBranchHead = namedRefsDefaultBranchHead(ctx, params);

    return namedRefsFilterAndEnhance(ctx, params, defaultBranchHead, refs);
  }

  @Override
  public Stream<KeyListEntry> keys(Hash commit, KeyFilterPredicate keyFilter)
      throws ReferenceNotFoundException {
    return keysForCommitEntry(NON_TRANSACTIONAL_OPERATION_CONTEXT, commit, keyFilter);
  }

  @Override
  public MergeResult<CommitLogEntry> merge(MergeParams mergeParams)
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
      AtomicReference<ImmutableMergeResult.Builder<CommitLogEntry>> mergeResultHolder =
          new AtomicReference<>();

      Hash result =
          casOpLoop(
              "merge",
              mergeParams.getToBranch(),
              CasOpVariant.COMMIT,
              (ctx, refHead, branchCommits, newKeyLists) -> {
                ImmutableMergeResult.Builder<CommitLogEntry> mergeResult = MergeResult.builder();
                mergeResultHolder.set(mergeResult);

                Hash currentHead = Hash.of(refHead.getHash());

                long timeInMicros = commitTimeInMicros();

                Hash newHead =
                    mergeAttempt(
                        ctx,
                        timeInMicros,
                        currentHead,
                        branchCommits,
                        newKeyLists,
                        mergeParams,
                        mergeResult);

                return casOpResult(
                    refHead,
                    newHead,
                    refLog ->
                        refLog
                            .setRefName(
                                ByteString.copyFromUtf8(mergeParams.getToBranch().getName()))
                            .setRefType(refHead.getType())
                            .setCommitHash(newHead.asBytes())
                            .setOperationTime(timeInMicros)
                            .setOperation(RefLogEntry.Operation.MERGE)
                            .addSourceHashes(mergeParams.getMergeFromHash().asBytes()));
              },
              () -> mergeConflictMessage("Retry-failure", mergeParams));

      ImmutableMergeResult.Builder<CommitLogEntry> mergeResult =
          Objects.requireNonNull(
              mergeResultHolder.get(), "Internal error, merge-result builder not set.");
      if (!mergeParams.isDryRun()) {
        mergeResult.wasApplied(true);
      }
      return mergeResult.resultantTargetHash(result).build();
    } catch (ReferenceNotFoundException | ReferenceConflictException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("RedundantThrows")
  @Override
  public MergeResult<CommitLogEntry> transplant(TransplantParams transplantParams)
      throws ReferenceNotFoundException, ReferenceConflictException {
    try {
      AtomicReference<ImmutableMergeResult.Builder<CommitLogEntry>> mergeResultHolder =
          new AtomicReference<>();

      Hash result =
          casOpLoop(
              "transplant",
              transplantParams.getToBranch(),
              CasOpVariant.COMMIT,
              (ctx, refHead, branchCommits, newKeyLists) -> {
                ImmutableMergeResult.Builder<CommitLogEntry> mergeResult = MergeResult.builder();
                mergeResultHolder.set(mergeResult);

                Hash currentHead = Hash.of(refHead.getHash());

                long timeInMicros = commitTimeInMicros();

                Hash newHead =
                    transplantAttempt(
                        ctx,
                        timeInMicros,
                        currentHead,
                        branchCommits,
                        newKeyLists,
                        transplantParams,
                        mergeResult);

                return casOpResult(
                    refHead,
                    newHead,
                    refLog -> {
                      refLog
                          .setRefName(
                              ByteString.copyFromUtf8(transplantParams.getToBranch().getName()))
                          .setRefType(refHead.getType())
                          .setCommitHash(newHead.asBytes())
                          .setOperationTime(timeInMicros)
                          .setOperation(RefLogEntry.Operation.TRANSPLANT);
                      transplantParams
                          .getSequenceToTransplant()
                          .forEach(hash -> refLog.addSourceHashes(hash.asBytes()));
                    });
              },
              () -> transplantConflictMessage("Retry-failure", transplantParams));

      ImmutableMergeResult.Builder<CommitLogEntry> mergeResult =
          Objects.requireNonNull(
              mergeResultHolder.get(), "Internal error, merge-result builder not set.");
      if (!transplantParams.isDryRun()) {
        mergeResult.wasApplied(true);
      }
      return mergeResult.resultantTargetHash(result).build();
    } catch (ReferenceNotFoundException | ReferenceConflictException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Hash commit(CommitParams commitParams)
      throws ReferenceConflictException, ReferenceNotFoundException {
    try {
      return casOpLoop(
          "commit",
          commitParams.getToBranch(),
          CasOpVariant.COMMIT,
          (ctx, refHead, x, newKeyLists) -> {
            Hash currentHead = Hash.of(refHead.getHash());

            long timeInMicros = commitTimeInMicros();

            CommitLogEntry newBranchCommit =
                commitAttempt(ctx, timeInMicros, currentHead, commitParams, newKeyLists);
            Hash newHead = newBranchCommit.getHash();

            return casOpResult(
                refHead,
                newHead,
                refLog ->
                    refLog
                        .setRefName(ByteString.copyFromUtf8(commitParams.getToBranch().getName()))
                        .setRefType(refHead.getType())
                        .setCommitHash(newHead.asBytes())
                        .setOperationTime(timeInMicros)
                        .setOperation(Operation.COMMIT));
          },
          () ->
              commitConflictMessage(
                  "Retry-Failure", commitParams.getToBranch(), commitParams.getExpectedHead()));
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
          "createRef",
          ref,
          CasOpVariant.CREATE_REF,
          (ctx, refHead, branchCommits, newKeyLists) -> {
            if (refHead != null) {
              throw referenceAlreadyExists(ref);
            }

            Hash hash = target;
            if (hash == null) {
              // Special case: Don't validate, if the 'target' parameter is null.
              // This is mostly used for tests that re-create the default-branch.
              hash = NO_ANCESTOR;
            }

            validateHashExists(ctx, hash);

            Hash newHead = hash;

            return casOpResult(
                refHead,
                hash,
                refLog ->
                    refLog
                        .setRefName(ByteString.copyFromUtf8(ref.getName()))
                        .setRefType(protoTypeForRef(ref))
                        .setCommitHash(newHead.asBytes())
                        .setOperationTime(commitTimeInMicros())
                        .setOperation(RefLogEntry.Operation.CREATE_REFERENCE));
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
          "deleteRef",
          reference,
          CasOpVariant.DELETE_REF,
          (ctx, refHead, branchCommits, newKeyLists) -> {
            Hash currentHead = Hash.of(refHead.getHash());
            verifyExpectedHash(currentHead, reference, expectedHead);

            return casOpResult(
                refHead,
                null,
                refLog ->
                    refLog
                        .setRefName(ByteString.copyFromUtf8(reference.getName()))
                        .setRefType(protoTypeForRef(reference))
                        .setCommitHash(currentHead.asBytes())
                        .setOperationTime(commitTimeInMicros())
                        .setOperation(RefLogEntry.Operation.DELETE_REFERENCE));
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
          "assignRef",
          assignee,
          CasOpVariant.REF_UPDATE,
          (ctx, refHead, branchCommits, newKeyLists) -> {
            Hash beforeAssign = Hash.of(refHead.getHash());
            verifyExpectedHash(beforeAssign, assignee, expectedHead);

            validateHashExists(ctx, assignTo);

            return casOpResult(
                refHead,
                assignTo,
                refLog ->
                    refLog
                        .setRefName(ByteString.copyFromUtf8(assignee.getName()))
                        .setRefType(protoTypeForRef(assignee))
                        .setCommitHash(assignTo.asBytes())
                        .setOperationTime(commitTimeInMicros())
                        .setOperation(RefLogEntry.Operation.ASSIGN_REFERENCE)
                        .addSourceHashes(beforeAssign.asBytes()));
          },
          () -> assignConflictMessage("Retry-Failure", assignee, expectedHead, assignTo));
    } catch (ReferenceNotFoundException | ReferenceConflictException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Stream<Difference> diff(Hash from, Hash to, KeyFilterPredicate keyFilter)
      throws ReferenceNotFoundException {
    return buildDiff(NON_TRANSACTIONAL_OPERATION_CONTEXT, from, to, keyFilter);
  }

  @Override
  public void initializeRepo(String defaultBranchName) {
    NonTransactionalOperationContext ctx = NON_TRANSACTIONAL_OPERATION_CONTEXT;
    if (fetchGlobalPointer(ctx) == null) {
      RefLogEntry newRefLog;
      try {
        for (int stripe = 0; stripe < config.getRefLogStripes(); stripe++) {
          unsafeWriteRefLogStripe(
              ctx,
              stripe,
              RefLogParents.newBuilder()
                  .addRefLogParentsInclHead(NO_ANCESTOR.asBytes())
                  .setLockId(randomHash().asBytes())
                  .build());
        }

        RefLogParents refLogParents =
            RefLogParents.newBuilder()
                .addRefLogParentsInclHead(NO_ANCESTOR.asBytes())
                .setLockId(randomHash().asBytes())
                .build();

        newRefLog =
            writeRefLogEntry(
                ctx,
                refLogParents,
                refLog ->
                    refLog
                        .setRefName(ByteString.copyFromUtf8(defaultBranchName))
                        .setRefType(RefType.Branch)
                        .setCommitHash(NO_ANCESTOR.asBytes())
                        .setOperationTime(commitTimeInMicros())
                        .setOperation(RefLogEntry.Operation.CREATE_REFERENCE));

        refLogParents =
            refLogParents.toBuilder()
                .clearRefLogParentsInclHead()
                .addRefLogParentsInclHead(newRefLog.getRefLogId())
                .addRefLogParentsInclHead(NO_ANCESTOR.asBytes())
                .build();

        unsafeWriteRefLogStripe(ctx, refLogStripeForName(defaultBranchName), refLogParents);
      } catch (ReferenceConflictException e) {
        throw new RuntimeException(e);
      }

      unsafeWriteGlobalPointer(
          ctx,
          GlobalStatePointer.newBuilder()
              .setGlobalId(randomHash().asBytes())
              .setGlobalLogHead(NO_ANCESTOR.asBytes())
              .setRefLogId(NO_ANCESTOR.asBytes())
              .addRefLogParentsInclHead(NO_ANCESTOR.asBytes())
              .addGlobalParentsInclHead(NO_ANCESTOR.asBytes())
              .build());

      Preconditions.checkState(
          createNamedReference(ctx, BranchName.of(defaultBranchName), NO_ANCESTOR),
          "Could not create default branch");
    }
  }

  @Override
  public Optional<ContentIdAndBytes> globalContent(ContentId contentId) {
    return globalLogFetcher(NON_TRANSACTIONAL_OPERATION_CONTEXT)
        .flatMap(e -> e.getPutsList().stream())
        .map(ProtoSerialization::protoToContentIdAndBytes)
        .filter(entry -> contentId.equals(entry.getContentId()))
        .map(cb -> ContentIdAndBytes.of(cb.getContentId(), cb.getValue()))
        .findFirst();
  }

  @Override
  public RepoDescription fetchRepositoryDescription() {
    NonTransactionalOperationContext ctx = NON_TRANSACTIONAL_OPERATION_CONTEXT;
    RepoDescription current = fetchRepositoryDescription(ctx);
    return current == null ? RepoDescription.DEFAULT : current;
  }

  @Override
  public void updateRepositoryDescription(Function<RepoDescription, RepoDescription> updater)
      throws ReferenceConflictException {
    NonTransactionalOperationContext ctx = NON_TRANSACTIONAL_OPERATION_CONTEXT;

    try (TryLoopState tryState =
        newTryLoopState(
            "updateRepositoryDescription",
            ts ->
                repoDescUpdateConflictMessage(
                    String.format(
                        "Retry-failure after %d retries, %d ms",
                        ts.getRetries(), ts.getDuration(TimeUnit.MILLISECONDS))),
            this::tryLoopStateCompletion,
            config)) {
      while (true) {
        RepoDescription current = fetchRepositoryDescription(ctx);

        RepoDescription updated =
            updater.apply(current == null ? RepoDescription.DEFAULT : current);

        if (updated == null) {
          return;
        }

        if (tryUpdateRepositoryDescription(ctx, current, updated)) {
          tryState.success(NO_ANCESTOR);
          return;
        }

        tryState.retry();
      }
    }
  }

  @Override
  public Map<String, Map<String, String>> repoMaintenance(RepoMaintenanceParams params) {
    return Collections.emptyMap();
  }

  @Override
  public void assertCleanStateForTests() {
    // nothing to do
  }

  // /////////////////////////////////////////////////////////////////////////////////////////////
  // Non-Transactional DatabaseAdapter subclass API (protected)
  // /////////////////////////////////////////////////////////////////////////////////////////////

  protected final RepoDescription fetchRepositoryDescription(NonTransactionalOperationContext ctx) {
    try (Traced ignore = trace("fetchRepositoryDescription")) {
      return doFetchRepositoryDescription(ctx);
    }
  }

  protected abstract RepoDescription doFetchRepositoryDescription(
      NonTransactionalOperationContext ctx);

  protected final boolean tryUpdateRepositoryDescription(
      NonTransactionalOperationContext ctx, RepoDescription expected, RepoDescription updateTo) {
    try (Traced ignore = trace("tryUpdateRepositoryDescription")) {
      return doTryUpdateRepositoryDescription(ctx, expected, updateTo);
    }
  }

  protected abstract boolean doTryUpdateRepositoryDescription(
      NonTransactionalOperationContext ctx, RepoDescription expected, RepoDescription updateTo);

  /** Get the protobuf-enum-value for a named-reference. */
  protected static RefType protoTypeForRef(NamedRef target) {
    RefType type;
    if (target instanceof BranchName) {
      type = RefType.Branch;
    } else if (target instanceof TagName) {
      type = RefType.Tag;
    } else {
      throw new IllegalArgumentException(target.getClass().getSimpleName());
    }
    return type;
  }

  /**
   * Transform the protobuf-enum-value for the named-reference-type plus the reference name into a
   * {@link NamedRef}.
   */
  protected static NamedRef toNamedRef(RefType type, String name) {
    switch (type) {
      case Branch:
        return BranchName.of(name);
      case Tag:
        return TagName.of(name);
      default:
        throw new IllegalArgumentException(type.name());
    }
  }

  /**
   * Convenience method for {@link AbstractDatabaseAdapter#hashOnRef(AutoCloseable, NamedRef,
   * Optional, Hash) hashOnRef(ctx, reference.getReference(), branchHead(fetchGlobalPointer(ctx),
   * reference), reference.getHashOnReference())}.
   */
  protected Hash hashOnRef(
      NonTransactionalOperationContext ctx, NamedRef reference, Optional<Hash> hashOnRef)
      throws ReferenceNotFoundException {
    return hashOnRef(ctx, reference, hashOnRef, branchHead(ctx, reference));
  }

  /**
   * "Body" of a Compare-And-Swap loop that returns the value to apply. {@link #casOpLoop(String,
   * NamedRef, CasOpVariant, CasOp, Supplier)} then tries to perform the Compare-And-Swap using the
   * known "current value", as passed via the {@code pointer} parameter to {@link
   * #apply(NonTransactionalOperationContext, RefPointer, Consumer, Consumer)}, and the "new value"
   * from the return value.
   */
  @FunctionalInterface
  public interface CasOp {
    /**
     * Applies an operation within a CAS-loop. The implementation gets the current ref-pointer and
     * must return a {@link CasOpResult} with the reference's new HEAD and function to configure the
     * ref-log entry.
     *
     * @param ctx operation context
     * @param refPointer information about the target named reference
     * @param branchCommits if more commits than the one returned via the return value were
     *     optimistically written, those must be passed to this consumer.
     * @param newKeyLists IDs of optimistically written {@link KeyListEntity} entities must be
     *     passed to this consumer.
     * @return "new value" that {@link #casOpLoop(String, NamedRef, CasOpVariant, CasOp, Supplier)}
     *     tries to apply
     */
    CasOpResult apply(
        NonTransactionalOperationContext ctx,
        RefPointer refPointer,
        Consumer<Hash> branchCommits,
        Consumer<Hash> newKeyLists)
        throws VersionStoreException;
  }

  protected static final class CasOpResult {
    final RefPointer currentHead;
    final Hash newHead;
    final Consumer<RefLogEntry.Builder> refLogEntry;

    private CasOpResult(
        RefPointer currentHead, Hash newHead, Consumer<RefLogEntry.Builder> refLogEntry) {
      this.currentHead = currentHead;
      this.newHead = newHead;
      this.refLogEntry = refLogEntry;
    }

    public static CasOpResult casOpResult(
        RefPointer currentHead, Hash newHead, Consumer<RefLogEntry.Builder> refLogEntry) {
      return new CasOpResult(currentHead, newHead, refLogEntry);
    }
  }

  enum CasOpVariant {
    /** For commit/merge/transplant, which add one or more commits to that named reference. */
    COMMIT(true),
    /**
     * For {@link #assign(NamedRef, Optional, Hash)}, which only update the updates the HEAD of a
     * named reference, but does not add a commit.
     */
    REF_UPDATE(false),
    /** For {@link #create(NamedRef, Hash)}. */
    CREATE_REF(false),
    /** For {@link #delete(NamedRef, Optional)}. */
    DELETE_REF(false);

    /**
     * Whether the hash returned by {@code casOp} will be a new commit and/or {@code * casOp}
     * produced more commits (think: merge+transplant) via the {@code individualCommits} * argument
     * to {@link CasOp#apply(NonTransactionalOperationContext, RefPointer, Consumer, Consumer)}.
     * Those commits will be unconditionally deleted, if this {@code commitOp} flag is * {@code
     * true}.
     */
    final boolean commitOp;

    CasOpVariant(boolean commitOp) {
      this.commitOp = commitOp;
    }
  }

  /**
   * This is the actual CAS-loop, which applies an operation onto a named-ref.
   *
   * @param ref named-reference on which the operation happens
   * @param opVariant influences the behavior, whether the operation adds one or more commits and
   *     whether the operation deletes the named reference.
   * @param casOp the implementation of the CAS-operation
   * @param retryErrorMessage provides an error-message for a {@link ReferenceConflictException}
   *     when the CAS operation failed to complete within the configured time and number of retries.
   */
  protected Hash casOpLoop(
      String opName,
      NamedRef ref,
      CasOpVariant opVariant,
      CasOp casOp,
      Supplier<String> retryErrorMessage)
      throws VersionStoreException {
    NonTransactionalOperationContext ctx = NON_TRANSACTIONAL_OPERATION_CONTEXT;

    try (TryLoopState tryState =
        newTryLoopState(
            opName,
            ts ->
                repoDescUpdateConflictMessage(
                    String.format(
                        "%s after %d retries, %d ms",
                        retryErrorMessage.get(),
                        ts.getRetries(),
                        ts.getDuration(TimeUnit.MILLISECONDS))),
            this::tryLoopStateCompletion,
            config)) {
      while (true) {
        Set<Hash> individualCommits = new HashSet<>();
        Set<Hash> individualKeyLists = new HashSet<>();

        NamedReference refHead = fetchNamedReference(ctx, ref.getName());
        if ((refHead == null || refHead.getRef().getType() != protoTypeForRef(ref))
            && opVariant != CasOpVariant.CREATE_REF) {
          // For all operations, except createReference, verify that the reference exists.
          throw referenceNotFound(ref);
        }
        CasOpResult result =
            casOp.apply(
                ctx,
                refHead != null ? refHead.getRef() : null,
                individualCommits::add,
                individualKeyLists::add);

        boolean casSuccess;
        switch (opVariant) {
          case CREATE_REF:
            casSuccess = createNamedReference(ctx, ref, result.newHead);
            break;
          case DELETE_REF:
            casSuccess = deleteNamedReference(ctx, ref, refHead.getRef());
            break;
          case REF_UPDATE:
          case COMMIT:
            if (refHead.getRef().getHash().equals(result.newHead.asBytes())) {
              // Target branch HEAD did not change
              return tryState.success(result.newHead);
            }
            casSuccess = updateNamedReference(ctx, ref, refHead.getRef(), result.newHead);
            break;
          default:
            throw new UnsupportedOperationException("Unknown opVariant " + opVariant);
        }

        if (!casSuccess) {
          if (opVariant.commitOp) {
            if (result.newHead != null) {
              individualCommits.add(result.newHead);
            }
            cleanUpCommitCas(ctx, individualCommits, individualKeyLists);
          }

          tryState.retry();
          continue;
        }

        // CAS against branch-heads succeeded, now try to write the ref-log-entry

        int stripe = refLogStripeForName(ref.getName());
        while (true) {
          RefLogParents refLogParents = fetchRefLogParents(ctx, stripe);

          RefLogEntry newRefLog = writeRefLogEntry(ctx, refLogParents, result.refLogEntry);

          RefLogParents.Builder newRefLogParents =
              RefLogParents.newBuilder()
                  .addRefLogParentsInclHead(newRefLog.getRefLogId())
                  .setLockId(randomHash().asBytes());
          newRefLog.getParentsList().stream()
              .limit(config.getParentsPerRefLogEntry() - 1)
              .forEach(newRefLogParents::addRefLogParentsInclHead);

          if (refLogParentsCas(ctx, stripe, refLogParents, newRefLogParents.build())) {
            return tryState.success(result.newHead);
          }

          cleanUpRefLogWrite(ctx, Hash.of(newRefLog.getRefLogId()));

          // Need to retry "forever" until the ref-log entry could be added.
          tryState.retryEndless();
        }
      }
    }
  }

  protected static ReferenceInfo<ByteString> namedReferenceToReferenceInfo(NamedReference r) {
    return ReferenceInfo.of(
        Hash.of(r.getRef().getHash()), toNamedRef(r.getRef().getType(), r.getName()));
  }

  private final class RefLogSplit {
    private final Spliterator<RefLog> source;

    private boolean exhausted;

    private RefLog current;

    private RefLogSplit(NonTransactionalOperationContext ctx, int stripe) {
      this.source = refLogStripeFetcher(ctx, stripe);
    }

    private void advance() {
      if (exhausted) {
        return;
      }

      current = null;
      if (!source.tryAdvance(refLog -> current = refLog)) {
        exhausted = true;
      }
    }

    private void maybeAdvance() {
      if (current == null) {
        advance();
      }
    }

    RefLog pull() {
      if (exhausted) {
        return null;
      }

      maybeAdvance();

      RefLog r = current;
      if (r != null) {
        current = null;
      }
      return r;
    }

    long operationTime() {
      maybeAdvance();

      return current != null ? current.getOperationTime() : 0L;
    }

    boolean hasMore() {
      maybeAdvance();

      return current != null;
    }

    @Override
    public String toString() {
      return "RefLogSplit{" + "exhausted=" + exhausted + ", current=" + current + '}';
    }
  }

  private final class RegLogSpliterator extends Spliterators.AbstractSpliterator<RefLog> {

    private final List<RefLogSplit> splits;

    private final Hash initialHash;
    private boolean initialHashSeen;
    private RefLog initialRefLog;

    RegLogSpliterator(NonTransactionalOperationContext ctx, int stripes, Hash initialHash)
        throws RefLogNotFoundException {
      super(Long.MAX_VALUE, Spliterator.ORDERED);

      this.initialHash = initialHash;
      this.initialHashSeen = initialHash == null;

      splits =
          IntStream.range(-1, config.getRefLogStripes())
              .mapToObj(stripe -> new RefLogSplit(ctx, stripe))
              .collect(Collectors.toList());

      // The API for DatabaseAdapter.refLog(Hash) requires to throw a RefLogNotFoundException,
      // if no RefLog for the initial hash exists. This loop tries to find the initial RefLog entry
      // and throw
      AtomicReference<RefLog> initial = new AtomicReference<>();
      while (!initialHashSeen) {
        if (!tryAdvance(initial::set)) {
          break;
        }
      }

      // Throw RefLogNotFoundException, if RefLog with initial hash could not be found.
      if (!initialHashSeen) {
        throw RefLogNotFoundException.forRefLogId(initialHash.asString());
      }
      this.initialRefLog = initial.get();
    }

    @Override
    public boolean tryAdvance(Consumer<? super RefLog> action) {
      if (initialRefLog != null) {
        // This returns the RefLog entry for the initial hash.
        action.accept(initialRefLog);
        initialRefLog = null;
        return true;
      }

      Optional<RefLogSplit> oldest =
          splits.stream()
              .filter(RefLogSplit::hasMore)
              .max(Comparator.comparing(RefLogSplit::operationTime));

      if (!oldest.isPresent()) {
        return false;
      }

      RefLog refLog = oldest.get().pull();
      if (refLog == null) {
        return false;
      }

      if (!initialHashSeen) {
        initialHashSeen = refLog.getRefLogId().equals(initialHash);
      }

      if (initialHashSeen) {
        action.accept(refLog);
      }

      return true;
    }
  }

  private Spliterator<RefLog> refLogStripeFetcher(
      NonTransactionalOperationContext ctx, int stripe) {
    List<ByteString> initial;
    if (stripe == -1) {
      // read "old" ref-log, where the ref-log HEAD was maintained in the global-pointer
      GlobalStatePointer globalPointer = fetchGlobalPointer(ctx);
      initial = globalPointer.getRefLogParentsInclHeadList();
    } else {
      // read "new" ref-log
      RefLogParents refLogParents = fetchRefLogParents(ctx, stripe);
      if (refLogParents == null) {
        refLogParents =
            RefLogParents.newBuilder().addRefLogParentsInclHead(NO_ANCESTOR.asBytes()).build();
      }
      initial = refLogParents.getRefLogParentsInclHeadList();
    }
    List<Hash> initialPage = initial.stream().map(Hash::of).collect(Collectors.toList());
    return logFetcherWithPage(ctx, initialPage, this::fetchPageFromRefLog, RefLog::getParents);
  }

  @Override
  protected Spliterator<RefLog> readRefLog(NonTransactionalOperationContext ctx, Hash initialHash)
      throws RefLogNotFoundException {
    if (NO_ANCESTOR.equals(initialHash)) {
      return Spliterators.emptySpliterator();
    }

    return new RegLogSpliterator(ctx, config.getRefLogStripes(), initialHash);
  }

  protected abstract void unsafeWriteRefLogStripe(
      NonTransactionalOperationContext ctx, int stripe, RefLogParents refLogParents);

  protected final boolean refLogParentsCas(
      NonTransactionalOperationContext ctx,
      int stripe,
      RefLogParents refLogParents,
      RefLogParents newRefLogParents) {
    try (Traced ignore = trace("refLogParentsCas")) {
      return doRefLogParentsCas(ctx, stripe, refLogParents, newRefLogParents);
    }
  }

  protected abstract boolean doRefLogParentsCas(
      NonTransactionalOperationContext ctx,
      int stripe,
      RefLogParents refLogParents,
      RefLogParents refLogEntry);

  protected final RefLogParents fetchRefLogParents(
      NonTransactionalOperationContext ctx, int stripe) {
    try (Traced ignore = trace("fetchRefLogParentsForReference")) {
      return doFetchRefLogParents(ctx, stripe);
    }
  }

  protected final int refLogStripeForName(String refName) {
    return Math.abs(refName.hashCode() % config.getRefLogStripes());
  }

  protected abstract RefLogParents doFetchRefLogParents(
      NonTransactionalOperationContext ctx, int stripe);

  protected final NamedReference fetchNamedReference(
      NonTransactionalOperationContext ctx, String refName) {
    try (Traced ignore = trace("fetchNamedReference").tag(TAG_REF, refName)) {
      NamedReference namedRef = doFetchNamedReference(ctx, refName);
      if (namedRef == null) {
        // Backwards compatibility, fetch named-reference's HEAD from global-pointer and create
        // the reference in the "new" format. We buy backwards-compatibility with an extra read
        // for non-existing branches.
        namedRef = maybeMigrateLegacyNamedReferences(ctx, refName);
      }
      return namedRef;
    }
  }

  /**
   * Migrates named references away from global-pointer. It's somewhat insecure, but probably good
   * enough.
   */
  private NamedReference maybeMigrateLegacyNamedReferences(
      NonTransactionalOperationContext ctx, String refName) {
    NamedReference namedRef = null;
    GlobalStatePointer pointer = fetchGlobalPointer(ctx);
    if (pointer != null) {
      if (pointer.getNamedReferencesCount() > 0) {
        GlobalStatePointer.Builder newPointer = pointer.toBuilder().clearNamedReferences();
        for (NamedReference legacyNamedRef : pointer.getNamedReferencesList()) {
          if (legacyNamedRef.getName().equals(refName)) {
            namedRef = legacyNamedRef;
          }
          if (!createNamedReference(
              ctx,
              toNamedRef(legacyNamedRef.getRef().getType(), legacyNamedRef.getName()),
              Hash.of(legacyNamedRef.getRef().getHash()))) {
            if (doFetchNamedReference(ctx, refName) == null) {
              newPointer.addNamedReferences(legacyNamedRef);
            }
          }
        }
        // TODO better use CAS?
        unsafeWriteGlobalPointer(ctx, newPointer.build());
      }
    }
    return namedRef;
  }

  protected abstract NamedReference doFetchNamedReference(
      NonTransactionalOperationContext ctx, String refName);

  protected final Stream<NamedReference> fetchNamedReferences(
      NonTransactionalOperationContext ctx) {

    maybeMigrateLegacyNamedReferences(ctx, null);

    AbstractSpliterator<ReferenceNames> split =
        new AbstractSpliterator<ReferenceNames>(Long.MAX_VALUE, Spliterator.ORDERED) {
          private int segment;

          @Override
          public boolean tryAdvance(Consumer<? super ReferenceNames> action) {
            ReferenceNames referenceNames = fetchReferenceNames(ctx, segment++);

            if (referenceNames == null) {
              return false;
            }

            action.accept(referenceNames);
            return true;
          }
        };

    return StreamSupport.stream(split, false)
        .map(ReferenceNames::getRefNamesList)
        .flatMap(List::stream)
        .map(ref -> fetchNamedReference(ctx, ref))
        .filter(Objects::nonNull);
  }

  protected final boolean createNamedReference(
      NonTransactionalOperationContext ctx, NamedRef ref, Hash newHead) {
    NamedReference namedReference =
        NamedReference.newBuilder()
            .setName(ref.getName())
            .setRef(
                RefPointer.newBuilder().setType(protoTypeForRef(ref)).setHash(newHead.asBytes()))
            .build();

    try (Traced ignore = trace("createNamedReference").tag(TAG_REF, ref.getName())) {

      // Add the reference to the list of reference-names first. This (eventually) guarantees,
      // that the name of the new reference will be present in the list. If another operation
      // fails, that's fine, because doFetchNamedReferences() does not return references that do
      // not exist in 'refNames'.

      doAddToNamedReferences(ctx, ref);

      return doCreateNamedReference(ctx, ref, namedReference);
    }
  }

  protected void doAddToNamedReferences(NonTransactionalOperationContext ctx, NamedRef ref) {
    doUpdateNamedReferencesList(
        referenceNames -> referenceNames.getRefNamesList().contains(ref.getName()),
        referenceNames -> {
          ReferenceNames.Builder referenceNamesBuilder = ReferenceNames.newBuilder();
          referenceNamesBuilder.addAllRefNames(referenceNames.getRefNamesList());
          referenceNamesBuilder.addRefNames(ref.getName());
          return referenceNamesBuilder;
        });
  }

  protected abstract boolean doCreateNamedReference(
      NonTransactionalOperationContext ctx, NamedRef ref, NamedReference namedReference);

  protected final boolean deleteNamedReference(
      NonTransactionalOperationContext ctx, NamedRef ref, RefPointer refHead) {
    try (Traced ignore = trace("deleteNamedReference").tag(TAG_REF, ref.getName())) {
      if (!doDeleteNamedReference(ctx, ref, refHead)) {
        return false;
      }

      // Remove the reference name from the list of reference names after actually removing
      // the named reference. That's fine, because doFetchNamedReferences() does not return
      // references that do not exist in 'refNames'.
      // TODO Caveat! A concurrent create-named-reference using the same name as the ref
      //  currently being dropped may end up having its name *not* present in the list of
      //  references. The reference would still be accessible and deletable though.

      doRemoveFromNamedReferences(ctx, ref);

      return true;
    }
  }

  protected void doRemoveFromNamedReferences(NonTransactionalOperationContext ctx, NamedRef ref) {
    doUpdateNamedReferencesList(
        referenceNames -> !referenceNames.getRefNamesList().contains(ref.getName()),
        referenceNames -> {
          ReferenceNames.Builder referenceNamesBuilder = ReferenceNames.newBuilder();
          referenceNames.getRefNamesList().stream()
              .filter(name -> !name.equals(ref.getName()))
              .forEach(referenceNamesBuilder::addRefNames);
          return referenceNamesBuilder;
        });
  }

  protected void doUpdateNamedReferencesList(
      Predicate<ReferenceNames> nextSegment,
      Function<ReferenceNames, ReferenceNames.Builder> updateReferenceNames) {
    throw new UnsupportedOperationException(
        "Implementation is missing doUpdateNamedReferencesList or doAddToNamedReferences+doRemoveFromNamedReferences");
  }

  protected abstract boolean doDeleteNamedReference(
      NonTransactionalOperationContext ctx, NamedRef ref, RefPointer refHead);

  protected final boolean updateNamedReference(
      NonTransactionalOperationContext ctx, NamedRef ref, RefPointer refHead, Hash newHead) {
    try (Traced ignore = trace("updateNamedReference").tag(TAG_REF, ref.getName())) {
      return doUpdateNamedReference(ctx, ref, refHead, newHead);
    }
  }

  protected abstract boolean doUpdateNamedReference(
      NonTransactionalOperationContext ctx, NamedRef ref, RefPointer refHead, Hash newHead);

  /**
   * Write a new refLog-entry with a best-effort approach to prevent hash-collisions but without any
   * other consistency checks/guarantees. Some implementations however can enforce strict
   * consistency checks/guarantees.
   */
  protected final void writeRefLog(NonTransactionalOperationContext ctx, RefLogEntry entry)
      throws ReferenceConflictException {
    try (Traced ignore = trace("writeRefLog")) {
      doWriteRefLog(ctx, entry);
    }
  }

  protected abstract void doWriteRefLog(NonTransactionalOperationContext ctx, RefLogEntry entry)
      throws ReferenceConflictException;

  /**
   * Unsafe operation to initialize a repository: unconditionally writes the global-state-pointer.
   */
  protected abstract void unsafeWriteGlobalPointer(
      NonTransactionalOperationContext ctx, GlobalStatePointer pointer);

  /**
   * If a {@link #refLogParentsCas(NonTransactionalOperationContext, int, RefLogParents,
   * RefLogParents)} failed, {@link
   * org.projectnessie.versioned.persist.adapter.DatabaseAdapter#commit(CommitParams)} calls this
   * function to remove the optimistically written data.
   *
   * <p>Implementation notes: non-transactional implementations <em>must</em> delete entries for the
   * given keys, no-op for transactional implementations.
   */
  protected final void cleanUpCommitCas(
      NonTransactionalOperationContext ctx, Set<Hash> branchCommits, Set<Hash> newKeyLists) {
    try (Traced ignore =
        trace("cleanUpCommitCas")
            .tag(TAG_COMMIT_COUNT, branchCommits.size())
            .tag(TAG_KEY_LIST_COUNT, newKeyLists.size())) {
      doCleanUpCommitCas(ctx, branchCommits, newKeyLists);
    }
  }

  protected abstract void doCleanUpCommitCas(
      NonTransactionalOperationContext ctx, Set<Hash> branchCommits, Set<Hash> newKeyLists);

  protected final void cleanUpRefLogWrite(NonTransactionalOperationContext ctx, Hash refLogId) {
    try (Traced ignore = trace("cleanUpRefLogWrite")) {
      doCleanUpRefLogWrite(ctx, refLogId);
    }
  }

  protected abstract void doCleanUpRefLogWrite(NonTransactionalOperationContext ctx, Hash refLogId);

  protected final ReferenceNames fetchReferenceNames(
      NonTransactionalOperationContext ctx, int segment) {
    try (Traced ignore = trace("fetchReferenceNames")) {
      return doFetchReferenceNames(ctx, segment);
    }
  }

  protected abstract ReferenceNames doFetchReferenceNames(
      NonTransactionalOperationContext ctx, int segment);

  /**
   * Retrieves the current HEAD of the given named reference.
   *
   * @param ref reference to retrieve the current HEAD for
   * @return current HEAD, not {@code null}
   * @throws ReferenceNotFoundException if {@code ref} does not exist.
   */
  protected Hash branchHead(NonTransactionalOperationContext ctx, NamedRef ref)
      throws ReferenceNotFoundException {
    if (ref == null) {
      return null;
    }
    NamedReference namedReference =
        fetchNamedReference(NON_TRANSACTIONAL_OPERATION_CONTEXT, ref.getName());
    if (namedReference == null || namedReference.getRef().getType() != protoTypeForRef(ref)) {
      throw referenceNotFound(ref.getName());
    }
    return Hash.of(namedReference.getRef().getHash());
  }

  protected ReferenceInfo<ByteString> referenceHead(
      NonTransactionalOperationContext ctx, String ref) throws ReferenceNotFoundException {

    NamedReference namedReference = fetchNamedReference(NON_TRANSACTIONAL_OPERATION_CONTEXT, ref);
    if (namedReference == null) {
      throw referenceNotFound(ref);
    }
    return namedReferenceToReferenceInfo(namedReference);
  }

  /**
   * Retrieves the hash of the default branch specified in {@link
   * GetNamedRefsParams#getBaseReference()}, if the retrieve options in {@link GetNamedRefsParams}
   * require it.
   */
  private Hash namedRefsDefaultBranchHead(
      NonTransactionalOperationContext ctx, GetNamedRefsParams params)
      throws ReferenceNotFoundException {
    if (namedRefsRequiresBaseReference(params)) {
      Preconditions.checkNotNull(params.getBaseReference(), "Base reference name missing.");
      return branchHead(ctx, params.getBaseReference());
    }
    return null;
  }

  /**
   * Load the current global-state-pointer.
   *
   * @return the current global points if set, or {@code null} if not set.
   */
  protected final GlobalStatePointer fetchGlobalPointer(NonTransactionalOperationContext ctx) {
    try (Traced ignore = trace("fetchGlobalPointer")) {
      return doFetchGlobalPointer(ctx);
    }
  }

  protected abstract GlobalStatePointer doFetchGlobalPointer(NonTransactionalOperationContext ctx);

  protected static ByteString globalLogHead(GlobalStatePointer pointer) {
    // backwards-compatibility code
    return pointer.hasGlobalLogHead() ? pointer.getGlobalLogHead() : pointer.getGlobalId();
  }

  @Override
  protected Map<ContentId, ByteString> doFetchGlobalStates(
      NonTransactionalOperationContext ctx, Set<ContentId> contentIds) {
    if (contentIds.isEmpty()) {
      return Collections.emptyMap();
    }

    Stream<GlobalStateLogEntry> log = globalLogFetcher(ctx);

    Set<ContentId> remainingIds = new HashSet<>(contentIds);

    return takeUntilExcludeLast(log, x -> remainingIds.isEmpty())
        .flatMap(e -> e.getPutsList().stream())
        .filter(put -> remainingIds.remove(ContentId.of(put.getContentId().getId())))
        .collect(
            Collectors.toMap(
                e -> ContentId.of(e.getContentId().getId()), ContentIdWithBytes::getValue));
  }

  /** Reads from the global-state-log starting at the given global-state-log-ID. */
  private Stream<GlobalStateLogEntry> globalLogFetcher(NonTransactionalOperationContext ctx) {
    GlobalStatePointer pointer = fetchGlobalPointer(ctx);
    if (pointer == null) {
      return Stream.empty();
    }

    // Before Nessie 0.21.0: the global-state-pointer contains the "heads" for the ref-log and
    // global-log, so it has to read the head ref-log & global-log to get the IDs of all the
    // previous parents to fill the parents in the new global-log-entry & ref-log-entry.
    //
    // Since Nessie 0.21.0: the global-state-pointer contains the "heads" for the ref-log and
    // global-log PLUS the parents of those, so Nessie no longer need to read the head entries
    // from the ref-log + global-log.
    //
    // The check of the first entry is there to ensure backwards compatibility and also
    // rolling-upgrades work.
    Spliterator<GlobalStateLogEntry> split;
    if (pointer.getGlobalParentsInclHeadCount() == 0
        || !globalLogHead(pointer).equals(pointer.getGlobalParentsInclHead(0))) {
      // Before Nessie 0.21.0

      Hash initialId = Hash.of(globalLogHead(pointer));

      GlobalStateLogEntry initial = fetchFromGlobalLog(ctx, initialId);
      if (initial == null) {
        throw new RuntimeException(
            new ReferenceNotFoundException(
                String.format("Global log entry '%s' not does not exist.", initialId.asString())));
      }

      split =
          logFetcher(
              ctx,
              initial,
              this::fetchPageFromGlobalLog,
              e -> e.getParentsList().stream().map(Hash::of).collect(Collectors.toList()));
    } else {
      // Since Nessie 0.21.0

      List<Hash> hashes =
          pointer.getGlobalParentsInclHeadList().stream()
              .map(Hash::of)
              .collect(Collectors.toList());
      split =
          logFetcherWithPage(
              ctx,
              hashes,
              this::fetchPageFromGlobalLog,
              e -> e.getParentsList().stream().map(Hash::of).collect(Collectors.toList()));
    }
    return StreamSupport.stream(split, false);
  }

  /**
   * Load the global-log entry with the given id.
   *
   * @return the loaded entry if it is available, {@code null} if it does not exist.
   */
  protected final GlobalStateLogEntry fetchFromGlobalLog(
      NonTransactionalOperationContext ctx, Hash id) {
    try (Traced ignore = trace("fetchFromGlobalLog").tag(TAG_HASH, id.asString())) {
      return doFetchFromGlobalLog(ctx, id);
    }
  }

  protected abstract GlobalStateLogEntry doFetchFromGlobalLog(
      NonTransactionalOperationContext ctx, Hash id);

  protected final List<GlobalStateLogEntry> fetchPageFromGlobalLog(
      NonTransactionalOperationContext ctx, List<Hash> hashes) {
    try (Traced ignore =
        trace("fetchPageFromGlobalLog")
            .tag(TAG_HASH, hashes.get(0).asString())
            .tag(TAG_COUNT, hashes.size())) {
      return doFetchPageFromGlobalLog(ctx, hashes);
    }
  }

  protected abstract List<GlobalStateLogEntry> doFetchPageFromGlobalLog(
      NonTransactionalOperationContext ctx, List<Hash> hashes);

  protected RefLogEntry writeRefLogEntry(
      NonTransactionalOperationContext ctx,
      RefLogParents refLogParents,
      Consumer<RefLogEntry.Builder> refLogEntryBuilder)
      throws ReferenceConflictException {

    Hash currentRefLogId = randomHash();

    Stream<ByteString> newParents =
        refLogParents != null
            ? refLogParents.getRefLogParentsInclHeadList().stream()
                .limit(config.getParentsPerRefLogEntry())
            : Stream.of(NO_ANCESTOR.asBytes());

    RefLogEntry.Builder entry = RefLogEntry.newBuilder().setRefLogId(currentRefLogId.asBytes());
    refLogEntryBuilder.accept(entry);
    newParents.forEach(entry::addParents);
    RefLogEntry refLogEntry = entry.build();

    writeRefLog(ctx, refLogEntry);

    return refLogEntry;
  }

  @Override
  public Stream<RefLog> refLog(Hash offset) throws RefLogNotFoundException {
    return readRefLogStream(NON_TRANSACTIONAL_OPERATION_CONTEXT, offset);
  }
}
