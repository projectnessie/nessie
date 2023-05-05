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
import com.google.common.collect.Maps;
import com.google.errorprone.annotations.MustBeClosed;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.projectnessie.model.ContentKey;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.CommitResult;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ImmutableCommitResult;
import org.projectnessie.versioned.ImmutableMergeResult;
import org.projectnessie.versioned.ImmutableReferenceAssignedResult;
import org.projectnessie.versioned.ImmutableReferenceCreatedResult;
import org.projectnessie.versioned.ImmutableReferenceDeletedResult;
import org.projectnessie.versioned.MergeResult;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.RefLogNotFoundException;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.ReferenceAssignedResult;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceCreatedResult;
import org.projectnessie.versioned.ReferenceDeletedResult;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.ResultType;
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
import org.projectnessie.versioned.persist.adapter.events.AdapterEvent;
import org.projectnessie.versioned.persist.adapter.events.AdapterEventConsumer;
import org.projectnessie.versioned.persist.adapter.events.CommitEvent;
import org.projectnessie.versioned.persist.adapter.events.MergeEvent;
import org.projectnessie.versioned.persist.adapter.events.ReferenceAssignedEvent;
import org.projectnessie.versioned.persist.adapter.events.ReferenceCreatedEvent;
import org.projectnessie.versioned.persist.adapter.events.ReferenceDeletedEvent;
import org.projectnessie.versioned.persist.adapter.events.RepositoryErasedEvent;
import org.projectnessie.versioned.persist.adapter.events.RepositoryInitializedEvent;
import org.projectnessie.versioned.persist.adapter.events.TransplantEvent;
import org.projectnessie.versioned.persist.adapter.serialize.ProtoSerialization;
import org.projectnessie.versioned.persist.adapter.spi.AbstractDatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.spi.BatchSpliterator;
import org.projectnessie.versioned.persist.adapter.spi.Traced;
import org.projectnessie.versioned.persist.adapter.spi.TryLoopState;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.ContentIdWithBytes;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.GlobalStateLogEntry;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.GlobalStatePointer;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.NamedReference;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.RefLogEntry;
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

  protected NonTransactionalDatabaseAdapter(CONFIG config, AdapterEventConsumer eventConsumer) {
    super(config, eventConsumer);
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
  public Map<ContentKey, ContentAndState> values(
      Hash commit, Collection<ContentKey> keys, KeyFilterPredicate keyFilter)
      throws ReferenceNotFoundException {
    return fetchValues(NON_TRANSACTIONAL_OPERATION_CONTEXT, commit, keys, keyFilter);
  }

  @Override
  @MustBeClosed
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

    try (Stream<ReferenceInfo<ByteString>> refStream =
        namedRefsFilterAndEnhance(ctx, params, defaultBranchHead, refs)) {
      return refStream.findFirst().orElseThrow(() -> referenceNotFound(ref));
    }
  }

  @Override
  @MustBeClosed
  public Stream<ReferenceInfo<ByteString>> namedRefs(GetNamedRefsParams params)
      throws ReferenceNotFoundException {
    Preconditions.checkNotNull(params, "Parameter for GetNamedRefsParams must not be null.");
    Preconditions.checkArgument(
        namedRefsAnyRetrieves(params), "Must retrieve branches or tags or both.");

    NonTransactionalOperationContext ctx = NON_TRANSACTIONAL_OPERATION_CONTEXT;

    @SuppressWarnings("MustBeClosedChecker")
    Stream<ReferenceInfo<ByteString>> refs =
        fetchNamedReferences(ctx)
            .map(NonTransactionalDatabaseAdapter::namedReferenceToReferenceInfo);

    Hash defaultBranchHead = namedRefsDefaultBranchHead(ctx, params);

    return namedRefsFilterAndEnhance(ctx, params, defaultBranchHead, refs);
  }

  @Override
  @MustBeClosed
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
      return casOpLoop(
          "merge",
          mergeParams.getToBranch(),
          CasOpVariant.MERGE,
          (ctx, refHead, branchCommits, newKeyLists) -> {
            ImmutableMergeResult.Builder<CommitLogEntry> mergeResult =
                MergeResult.<CommitLogEntry>builder()
                    .resultType(ResultType.MERGE)
                    .sourceRef(mergeParams.getFromRef());

            Hash currentHead = Hash.of(refHead.getHash());

            long timeInMicros = config.currentTimeInMicros();

            List<CommitLogEntry> writtenCommits = new ArrayList<>();
            Hash newHead =
                mergeAttempt(
                    ctx,
                    timeInMicros,
                    currentHead,
                    branchCommits,
                    newKeyLists,
                    writtenCommits::add,
                    mergeResult::addCreatedCommits,
                    mergeParams,
                    mergeResult);

            if (!mergeParams.isDryRun()) {
              mergeResult.wasApplied(true);
            }
            mergeResult.resultantTargetHash(newHead);

            return casOpResult(
                refHead,
                mergeResult.build(),
                null,
                () ->
                    MergeEvent.builder()
                        .previousHash(currentHead)
                        .hash(newHead)
                        .branch(mergeParams.getToBranch())
                        .commits(writtenCommits));
          },
          () -> mergeConflictMessage("Retry-failure", mergeParams));

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
      return casOpLoop(
          "transplant",
          transplantParams.getToBranch(),
          CasOpVariant.MERGE,
          (ctx, refHead, branchCommits, newKeyLists) -> {
            ImmutableMergeResult.Builder<CommitLogEntry> mergeResult =
                MergeResult.<CommitLogEntry>builder()
                    .resultType(ResultType.TRANSPLANT)
                    .sourceRef(transplantParams.getFromRef());

            Hash currentHead = Hash.of(refHead.getHash());

            long timeInMicros = config.currentTimeInMicros();

            List<CommitLogEntry> writtenCommits = new ArrayList<>();
            Hash newHead =
                transplantAttempt(
                    ctx,
                    timeInMicros,
                    currentHead,
                    branchCommits,
                    newKeyLists,
                    writtenCommits::add,
                    mergeResult::addCreatedCommits,
                    transplantParams,
                    mergeResult);

            if (!transplantParams.isDryRun()) {
              mergeResult.wasApplied(true);
            }
            mergeResult.resultantTargetHash(newHead);

            return casOpResult(
                refHead,
                mergeResult.build(),
                null,
                () ->
                    TransplantEvent.builder()
                        .previousHash(currentHead)
                        .hash(newHead)
                        .branch(transplantParams.getToBranch())
                        .commits(writtenCommits));
          },
          () -> transplantConflictMessage("Retry-failure", transplantParams));

    } catch (ReferenceNotFoundException | ReferenceConflictException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public CommitResult<CommitLogEntry> commit(CommitParams commitParams)
      throws ReferenceConflictException, ReferenceNotFoundException {
    try {
      return casOpLoop(
          "commit",
          commitParams.getToBranch(),
          CasOpVariant.COMMIT,
          (ctx, refHead, branchCommits, newKeyLists) -> {
            Hash currentHead = Hash.of(refHead.getHash());

            ImmutableCommitResult.Builder<CommitLogEntry> commitResult = CommitResult.builder();

            long timeInMicros = config.currentTimeInMicros();

            CommitLogEntry newBranchCommit =
                commitAttempt(ctx, timeInMicros, currentHead, commitParams, newKeyLists);
            Hash newHead = newBranchCommit.getHash();

            branchCommits.accept(newHead);

            commitResult.targetBranch(commitParams.getToBranch()).commit(newBranchCommit);

            return casOpResult(
                refHead,
                commitResult.build(),
                null,
                () ->
                    CommitEvent.builder()
                        .previousHash(currentHead)
                        .hash(newHead)
                        .branch(commitParams.getToBranch())
                        .addCommits(newBranchCommit));
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
  public ReferenceCreatedResult create(NamedRef ref, Hash target)
      throws ReferenceAlreadyExistsException, ReferenceNotFoundException {
    try {
      return casOpLoop(
          "createRef",
          ref,
          CasOpVariant.CREATE_REF,
          (ctx, refHead, branchCommits, newKeyLists) -> {
            ImmutableReferenceCreatedResult.Builder result =
                ImmutableReferenceCreatedResult.builder().namedRef(ref);

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

            result.hash(newHead);

            return casOpResult(
                null,
                result.build(),
                refLog ->
                    refLog
                        .setRefName(ByteString.copyFromUtf8(ref.getName()))
                        .setRefType(protoTypeForRef(ref))
                        .setCommitHash(newHead.asBytes())
                        .setOperationTime(config.currentTimeInMicros())
                        .setOperation(RefLogEntry.Operation.CREATE_REFERENCE),
                () -> ReferenceCreatedEvent.builder().currentHash(newHead).ref(ref));
          },
          () -> createConflictMessage("Retry-Failure", ref, target));
    } catch (ReferenceAlreadyExistsException | ReferenceNotFoundException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ReferenceDeletedResult delete(NamedRef reference, Optional<Hash> expectedHead)
      throws ReferenceNotFoundException, ReferenceConflictException {
    try {
      return casOpLoop(
          "deleteRef",
          reference,
          CasOpVariant.DELETE_REF,
          (ctx, refHead, branchCommits, newKeyLists) -> {
            ImmutableReferenceDeletedResult.Builder result =
                ImmutableReferenceDeletedResult.builder().namedRef(reference);

            Hash currentHead = Hash.of(refHead.getHash());
            verifyExpectedHash(currentHead, reference, expectedHead);

            result.hash(currentHead);

            return casOpResult(
                refHead,
                result.build(),
                refLog ->
                    refLog
                        .setRefName(ByteString.copyFromUtf8(reference.getName()))
                        .setRefType(protoTypeForRef(reference))
                        .setCommitHash(currentHead.asBytes())
                        .setOperationTime(config.currentTimeInMicros())
                        .setOperation(RefLogEntry.Operation.DELETE_REFERENCE),
                () -> ReferenceDeletedEvent.builder().currentHash(currentHead).ref(reference));
          },
          () -> deleteConflictMessage("Retry-Failure", reference, expectedHead));
    } catch (ReferenceNotFoundException | ReferenceConflictException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ReferenceAssignedResult assign(
      NamedRef assignee, Optional<Hash> expectedHead, Hash assignTo)
      throws ReferenceNotFoundException, ReferenceConflictException {
    try {
      return casOpLoop(
          "assignRef",
          assignee,
          CasOpVariant.REF_UPDATE,
          (ctx, refHead, branchCommits, newKeyLists) -> {
            ImmutableReferenceAssignedResult.Builder result =
                ImmutableReferenceAssignedResult.builder().namedRef(assignee);

            Hash beforeAssign = Hash.of(refHead.getHash());
            verifyExpectedHash(beforeAssign, assignee, expectedHead);

            validateHashExists(ctx, assignTo);

            result.previousHash(beforeAssign).currentHash(assignTo);

            return casOpResult(
                refHead,
                result.build(),
                refLog ->
                    refLog
                        .setRefName(ByteString.copyFromUtf8(assignee.getName()))
                        .setRefType(protoTypeForRef(assignee))
                        .setCommitHash(assignTo.asBytes())
                        .setOperationTime(config.currentTimeInMicros())
                        .setOperation(RefLogEntry.Operation.ASSIGN_REFERENCE)
                        .addSourceHashes(beforeAssign.asBytes()),
                () ->
                    ReferenceAssignedEvent.builder()
                        .currentHash(assignTo)
                        .ref(assignee)
                        .previousHash(beforeAssign));
          },
          () -> assignConflictMessage("Retry-Failure", assignee, expectedHead, assignTo));
    } catch (ReferenceNotFoundException | ReferenceConflictException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  @MustBeClosed
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
        RefLogParents refLogParents =
            RefLogParents.newBuilder()
                .addRefLogParentsInclHead(NO_ANCESTOR.asBytes())
                .setVersion(randomHash().asBytes())
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
                        .setOperationTime(config.currentTimeInMicros())
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

      repositoryEvent(() -> RepositoryInitializedEvent.builder().defaultBranch(defaultBranchName));

      BranchName defaultBranch = BranchName.of(defaultBranchName);
      Preconditions.checkState(
          createNamedReference(ctx, defaultBranch, NO_ANCESTOR), "Could not create default branch");

      repositoryEvent(
          () -> ReferenceCreatedEvent.builder().ref(defaultBranch).currentHash(NO_ANCESTOR));
    }
  }

  @Override
  public void eraseRepo() {
    doEraseRepo();
    repositoryEvent(RepositoryErasedEvent::builder);
  }

  protected abstract void doEraseRepo();

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

    try (TryLoopState<Hash> tryState =
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

  @Override
  public void writeMultipleCommits(List<CommitLogEntry> commitLogEntries)
      throws ReferenceConflictException {
    try {
      doWriteMultipleCommits(NON_TRANSACTIONAL_OPERATION_CONTEXT, commitLogEntries);
    } catch (ReferenceConflictException e) {
      throw e;
    }
  }

  @Override
  public void updateMultipleCommits(List<CommitLogEntry> commitLogEntries)
      throws ReferenceNotFoundException {
    try {
      doUpdateMultipleCommits(NON_TRANSACTIONAL_OPERATION_CONTEXT, commitLogEntries);
    } catch (ReferenceNotFoundException e) {
      throw e;
    }
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
  public interface CasOp<R> {
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
    CasOpResult<R> apply(
        NonTransactionalOperationContext ctx,
        RefPointer refPointer,
        Consumer<Hash> branchCommits,
        Consumer<Hash> newKeyLists)
        throws VersionStoreException;
  }

  protected static final class CasOpResult<R> {
    final RefPointer currentHead;
    final R result;
    final Consumer<RefLogEntry.Builder> refLogEntry;
    final Supplier<? extends AdapterEvent.Builder<?, ?>> adapterEventBuilder;

    private CasOpResult(
        RefPointer currentHead,
        R result,
        Consumer<RefLogEntry.Builder> refLogEntry,
        Supplier<? extends AdapterEvent.Builder<?, ?>> adapterEventBuilder) {
      this.currentHead = currentHead;
      this.result = result;
      this.refLogEntry = refLogEntry;
      this.adapterEventBuilder = adapterEventBuilder;
    }

    public static <R> CasOpResult<R> casOpResult(
        RefPointer currentHead,
        R result,
        Consumer<RefLogEntry.Builder> refLogEntry,
        Supplier<? extends AdapterEvent.Builder<?, ?>> adapterEventBuilder) {
      return new CasOpResult<>(currentHead, result, refLogEntry, adapterEventBuilder);
    }
  }

  enum CasOpVariant {
    /** For commits, which add one commit to that named reference. */
    COMMIT(),
    /** For merge/transplant, which add one or more commits to that named reference. */
    MERGE(),
    /**
     * For {@link #assign(NamedRef, Optional, Hash)}, which only update the updates the HEAD of a
     * named reference, but does not add a commit.
     */
    REF_UPDATE(),
    /** For {@link #create(NamedRef, Hash)}. */
    CREATE_REF(),
    /** For {@link #delete(NamedRef, Optional)}. */
    DELETE_REF()
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
  protected <R> R casOpLoop(
      String opName,
      NamedRef ref,
      CasOpVariant opVariant,
      CasOp<R> casOp,
      Supplier<String> retryErrorMessage)
      throws VersionStoreException {
    NonTransactionalOperationContext ctx = NON_TRANSACTIONAL_OPERATION_CONTEXT;

    try (TryLoopState<R> tryState =
        newTryLoopState(
            opName,
            ts ->
                String.format(
                    "%s after %d retries, %d ms",
                    retryErrorMessage.get(),
                    ts.getRetries(),
                    ts.getDuration(TimeUnit.MILLISECONDS)),
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
        CasOpResult<R> result =
            casOp.apply(
                ctx,
                refHead != null ? refHead.getRef() : null,
                individualCommits::add,
                individualKeyLists::add);

        boolean casSuccess;
        switch (opVariant) {
          case CREATE_REF:
            ReferenceCreatedResult refCreatedResult = ((ReferenceCreatedResult) result.result);
            casSuccess = createNamedReference(ctx, ref, refCreatedResult.getHash());
            break;
          case DELETE_REF:
            casSuccess = deleteNamedReference(ctx, ref, refHead.getRef());
            break;
          case REF_UPDATE:
            ReferenceAssignedResult refAssignedResult = (ReferenceAssignedResult) result.result;
            if (refHead.getRef().getHash().equals(refAssignedResult.getCurrentHash().asBytes())) {
              // Target branch HEAD did not change
              return tryState.success(result.result);
            }
            casSuccess =
                updateNamedReference(
                    ctx, ref, refHead.getRef(), refAssignedResult.getCurrentHash());
            break;
          case COMMIT:
            CommitResult<?> commitResult = (CommitResult<?>) result.result;
            if (refHead.getRef().getHash().equals(commitResult.getCommitHash().asBytes())) {
              // Target branch HEAD did not change
              return tryState.success(result.result);
            }
            casSuccess =
                updateNamedReference(ctx, ref, refHead.getRef(), commitResult.getCommitHash());
            break;
          case MERGE:
            MergeResult<?> mergeResult = ((MergeResult<?>) result.result);
            Hash newHash = Objects.requireNonNull(mergeResult.getResultantTargetHash());
            if (refHead.getRef().getHash().equals(newHash.asBytes())) {
              // Target branch HEAD did not change
              return tryState.success(result.result);
            }
            casSuccess =
                updateNamedReference(
                    ctx, ref, refHead.getRef(), mergeResult.getResultantTargetHash());
            break;
          default:
            throw new UnsupportedOperationException("Unknown opVariant " + opVariant);
        }

        if (!casSuccess) {
          if (opVariant == CasOpVariant.COMMIT || opVariant == CasOpVariant.MERGE) {
            cleanUpCommitCas(ctx, individualCommits, individualKeyLists);
          }

          tryState.retry();
          continue;
        }

        repositoryEvent(result.adapterEventBuilder);

        if (result.refLogEntry == null) {
          // No ref-log entry to be written

          return tryState.success(result.result);
        } else {
          // CAS against branch-heads succeeded, now try to write the ref-log-entry

          // reset the retry-loop's sleep boundaries
          tryState.resetBounds();
          int stripe = refLogStripeForName(ref.getName());
          while (true) {
            RefLogParents refLogParents = fetchRefLogParents(ctx, stripe);

            RefLogEntry newRefLog = writeRefLogEntry(ctx, refLogParents, result.refLogEntry);

            RefLogParents.Builder newRefLogParents =
                RefLogParents.newBuilder()
                    .addRefLogParentsInclHead(newRefLog.getRefLogId())
                    .setVersion(randomHash().asBytes());
            newRefLog.getParentsList().stream()
                .limit(config.getParentsPerRefLogEntry() - 1)
                .forEach(newRefLogParents::addRefLogParentsInclHead);

            if (refLogParentsCas(ctx, stripe, refLogParents, newRefLogParents.build())) {
              return tryState.success(result.result);
            }

            cleanUpRefLogWrite(ctx, Hash.of(newRefLog.getRefLogId()));

            // Need to retry "forever" until the ref-log entry could be added.
            tryState.retryEndless();
          }
        }
      }
    }
  }

  protected static ReferenceInfo<ByteString> namedReferenceToReferenceInfo(NamedReference r) {
    return ReferenceInfo.of(
        Hash.of(r.getRef().getHash()), toNamedRef(r.getRef().getType(), r.getName()));
  }

  @Override
  protected Spliterator<RefLog> readRefLog(NonTransactionalOperationContext ctx, Hash initialHash)
      throws RefLogNotFoundException {
    if (NO_ANCESTOR.equals(initialHash)) {
      return Spliterators.emptySpliterator();
    }

    // Two possible optimizations here:
    // 1. Paging - implement an "implementation aware token"
    //    Instead of calling refLogStripeFetcher and then scanning the ref-log for the ref-log hash
    //    from the page-token, add another parameter to this method that the ref-log-stripe fetchers
    //    with their own initial ref-log-ids (so 1 hash per stripe in a paging token).
    //    This requires this method to either return a wrapper around `RefLog` that also holds the
    //    paging token for every returned `RefLog`, or to return a "PagingAwareSpliterator" that
    //    allows retrieving the paging token. The latter is probably more efficient, but requires
    //    a bit more "verbose" code.
    // 2. Faster initial stripe fetching
    //    Instead of retrieving all initial pages sequentially, perform a bulk-fetch for all initial
    //    pages. Since ref-log queries are not performance critical and also rare operations, it is
    //    not urgent to implement this optimization.

    Stream<List<ByteString>> initialHashes =
        Stream.concat(
            // Ref-log as in global-pointer (backwards compatibility)
            Stream.of(fetchGlobalPointer(ctx).getRefLogParentsInclHeadList()),
            // Ref-log stripes
            IntStream.range(0, config.getRefLogStripes())
                .mapToObj(
                    stripe -> {
                      // read "new" ref-log
                      RefLogParents refLogParents = fetchRefLogParents(ctx, stripe);
                      if (refLogParents == null) {
                        refLogParents =
                            RefLogParents.newBuilder()
                                .addRefLogParentsInclHead(NO_ANCESTOR.asBytes())
                                .build();
                      }
                      return refLogParents.getRefLogParentsInclHeadList();
                    }));

    Stream<Spliterator<RefLog>> stripeFetchers =
        initialHashes.map(
            initial -> {
              List<Hash> initialPage = initial.stream().map(Hash::of).collect(Collectors.toList());
              return logFetcherWithPage(
                  ctx, initialPage, this::fetchPageFromRefLog, RefLog::getParents);
            });

    return new RefLogSpliterator(initialHash, stripeFetchers);
  }

  protected abstract void unsafeWriteRefLogStripe(
      NonTransactionalOperationContext ctx, int stripe, RefLogParents refLogParents);

  protected final boolean refLogParentsCas(
      NonTransactionalOperationContext ctx,
      int stripe,
      RefLogParents previousEntry,
      RefLogParents newEntry) {
    try (Traced ignore = trace("refLogParentsCas")) {
      return doRefLogParentsCas(ctx, stripe, previousEntry, newEntry);
    }
  }

  protected abstract boolean doRefLogParentsCas(
      NonTransactionalOperationContext ctx,
      int stripe,
      RefLogParents previousEntry,
      RefLogParents newEntry);

  protected final RefLogParents fetchRefLogParents(
      NonTransactionalOperationContext ctx, int stripe) {
    try (Traced ignore = trace("fetchRefLogParentsForReference")) {
      return doFetchRefLogParents(ctx, stripe);
    }
  }

  protected final int refLogStripeForName(String refName) {
    int h = refName.hashCode();
    if (h == Integer.MIN_VALUE) {
      h++;
    }
    return Math.abs(h) % config.getRefLogStripes();
  }

  protected abstract RefLogParents doFetchRefLogParents(
      NonTransactionalOperationContext ctx, int stripe);

  protected final NamedReference fetchNamedReference(
      NonTransactionalOperationContext ctx, String refName) {
    List<NamedReference> namedRefs = fetchNamedReference(ctx, Collections.singletonList(refName));
    return namedRefs.isEmpty() ? null : namedRefs.get(0);
  }

  protected final List<NamedReference> fetchNamedReference(
      NonTransactionalOperationContext ctx, List<String> refNames) {
    try (Traced ignore = trace("fetchNamedReference").tag(TAG_REF, refNames.size())) {
      List<NamedReference> namedRefs = doFetchNamedReference(ctx, refNames);
      if (namedRefs.isEmpty()) {
        // Backwards compatibility, fetch named-reference's HEAD from global-pointer and create
        // the reference in the "new" format. We buy backwards-compatibility with an extra read
        // for non-existing branches.
        if (maybeMigrateLegacyNamedReferences(ctx)) {
          namedRefs = doFetchNamedReference(ctx, refNames);
        }
      }
      return namedRefs;
    }
  }

  /**
   * Migrates named references away from global-pointer.
   *
   * <p>Implemented as a two-step process:
   *
   * <ol>
   *   <li>Add all references from the global-pointer to the named-references-inventory, do not
   *       modify the global-pointer.
   *   <li>Create references from the global-pointer in the new, one-row-per-named-reference,
   *       structure + purge the added references from global-pointer.
   * </ol>
   */
  private boolean maybeMigrateLegacyNamedReferences(NonTransactionalOperationContext ctx) {

    GlobalStatePointer pointer = fetchGlobalPointer(ctx);
    if (pointer == null || pointer.getNamedReferencesCount() == 0) {
      return false;
    }

    // Figure out the NamedReference instances, that need to be added to the named-references
    // inventory.
    Map<String, NamedReference> namedRefsFromGlobal =
        Maps.newHashMapWithExpectedSize(pointer.getNamedReferencesCount());
    pointer.getNamedReferencesList().forEach(nr -> namedRefsFromGlobal.put(nr.getName(), nr));

    List<ReferenceNames> refNamesSegments =
        StreamSupport.stream(fetchReferenceNames(ctx), false).collect(Collectors.toList());

    // Add reference names from global pointer to named-references inventory.

    Map<String, NamedReference> refsToAddToInventoryMap = new HashMap<>(namedRefsFromGlobal);
    for (ReferenceNames refNames : refNamesSegments) {
      refNames.getRefNamesList().forEach(refsToAddToInventoryMap::remove);
    }

    if (!refsToAddToInventoryMap.isEmpty()) {
      Iterator<NamedReference> refsToAddToInventory = refsToAddToInventoryMap.values().iterator();

      int namedReferencesSegment = 0;
      List<NamedRef> refsToAddToSegment = new ArrayList<>();
      IntFunction<Integer> calculateHeadRoomForSegment =
          seg -> {
            ReferenceNames referenceNamesSegment =
                refNamesSegments.size() > seg ? refNamesSegments.get(seg) : null;
            return maxEntitySize(config.getReferencesSegmentSize())
                - (referenceNamesSegment != null ? referenceNamesSegment.getSerializedSize() : 0);
          };

      int headRoom = calculateHeadRoomForSegment.apply(namedReferencesSegment);

      while (refsToAddToInventory.hasNext()) {
        NamedReference namedRefToAdd = refsToAddToInventory.next();
        // Simplified size estimation for a protobuf string list, but considers "wide" UTF-8
        // characters.
        int namedRefToAddSize = ByteString.copyFromUtf8(namedRefToAdd.getName()).size() + 3;

        while (headRoom < namedRefToAddSize) {
          if (!refsToAddToSegment.isEmpty()) {
            doAddToNamedReferences(ctx, refsToAddToSegment.stream(), namedReferencesSegment);
            refsToAddToSegment.clear();
          }

          namedReferencesSegment++;
          headRoom = calculateHeadRoomForSegment.apply(namedReferencesSegment);
        }

        refsToAddToSegment.add(
            toNamedRef(namedRefToAdd.getRef().getType(), namedRefToAdd.getName()));
        headRoom -= namedRefToAddSize;
      }

      if (!refsToAddToSegment.isEmpty()) {
        doAddToNamedReferences(ctx, refsToAddToSegment.stream(), namedReferencesSegment);
        refsToAddToSegment.clear();
      }
    }

    // Create references in the "new structure"

    while (true) {
      // Pick a random reference from global-pointer

      NamedReference namedRefToMigrate =
          pointer
              .getNamedReferencesList()
              .get(ThreadLocalRandom.current().nextInt(pointer.getNamedReferencesCount()));

      // Migrate that named reference

      doCreateNamedReference(ctx, namedRefToMigrate);

      GlobalStatePointer.Builder newPointerBuilder =
          pointer.toBuilder().clearNamedReferences().setGlobalId(randomHash().asBytes());
      pointer.getNamedReferencesList().stream()
          .filter(nr -> !nr.getName().equals(namedRefToMigrate.getName()))
          .forEach(newPointerBuilder::addNamedReferences);
      GlobalStatePointer newPointer = newPointerBuilder.build();
      if (globalPointerCas(ctx, pointer, newPointer)) {
        pointer = newPointer;
      } else {
        pointer = fetchGlobalPointer(ctx);
      }
      if (pointer == null || pointer.getNamedReferencesCount() == 0) {
        break;
      }
    }

    return true;
  }

  protected abstract List<NamedReference> doFetchNamedReference(
      NonTransactionalOperationContext ctx, List<String> refNames);

  @MustBeClosed
  protected final Stream<NamedReference> fetchNamedReferences(
      NonTransactionalOperationContext ctx) {

    maybeMigrateLegacyNamedReferences(ctx);

    Spliterator<ReferenceNames> split = fetchReferenceNames(ctx);

    Spliterator<String> allNames =
        StreamSupport.stream(split, false)
            .map(ReferenceNames::getRefNamesList)
            .flatMap(List::stream)
            // The named references splits may contain duplicate names, which is probably very, very
            // rare, but still possible.
            .distinct()
            .spliterator();

    BatchSpliterator<String, NamedReference> batchSpliterator =
        new BatchSpliterator<>(
            config.getReferenceNamesBatchSize(),
            allNames,
            batch -> fetchNamedReference(ctx, batch).spliterator(),
            Spliterator.NONNULL | Spliterator.DISTINCT | Spliterator.IMMUTABLE);

    return StreamSupport.stream(batchSpliterator, false);
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

      int addToSegment = findAvailableNamedReferencesSegment(ctx);
      doAddToNamedReferences(ctx, Stream.of(ref), addToSegment);

      return doCreateNamedReference(ctx, namedReference);
    }
  }

  /** Find the segment that has enough room for another {@link NamedReference}. */
  protected int findAvailableNamedReferencesSegment(NonTransactionalOperationContext ctx) {
    int segment = 0;
    for (Iterator<ReferenceNames> iter = Spliterators.iterator(fetchReferenceNames(ctx));
        iter.hasNext();
        segment++) {
      ReferenceNames refNames = iter.next();
      int serSize = refNames.getSerializedSize();
      if (serSize < maxEntitySize(config.getReferencesSegmentSize())) {
        return segment;
      }
    }
    return segment;
  }

  protected abstract void doAddToNamedReferences(
      NonTransactionalOperationContext ctx, Stream<NamedRef> refStream, int addToSegment);

  protected abstract void doRemoveFromNamedReferences(
      NonTransactionalOperationContext ctx, NamedRef ref, int removeFromSegment);

  protected abstract boolean doCreateNamedReference(
      NonTransactionalOperationContext ctx, NamedReference namedReference);

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

      int segment = 0;
      for (Iterator<ReferenceNames> iter = Spliterators.iterator(fetchReferenceNames(ctx));
          iter.hasNext();
          segment++) {
        ReferenceNames refNames = iter.next();
        if (refNames.getRefNamesList().contains(ref.getName())) {
          doRemoveFromNamedReferences(ctx, ref, segment);
        }
      }

      return true;
    }
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
   * Atomically update the global-commit-pointer to the given new-global-head, if the value in the
   * database is the given expected-global-head.
   */
  protected final boolean globalPointerCas(
      NonTransactionalOperationContext ctx,
      GlobalStatePointer expected,
      GlobalStatePointer newPointer) {
    try (Traced ignore = trace("globalPointerCas")) {
      return doGlobalPointerCas(ctx, expected, newPointer);
    }
  }

  protected abstract boolean doGlobalPointerCas(
      NonTransactionalOperationContext ctx,
      GlobalStatePointer expected,
      GlobalStatePointer newPointer);

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

  protected final Spliterator<ReferenceNames> fetchReferenceNames(
      NonTransactionalOperationContext ctx) {
    return new ReferenceNamesSpliterator(
        seg -> fetchReferenceNames(ctx, seg, config.getReferencesSegmentPrefetch()));
  }

  protected final List<ReferenceNames> fetchReferenceNames(
      NonTransactionalOperationContext ctx, int segment, int prefetchSegments) {
    try (Traced ignore = trace("fetchReferenceNames")) {
      return doFetchReferenceNames(ctx, segment, prefetchSegments);
    }
  }

  protected abstract List<ReferenceNames> doFetchReferenceNames(
      NonTransactionalOperationContext ctx, int segment, int prefetchSegments);

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
    NamedReference namedReference = fetchNamedReference(ctx, ref.getName());
    if (namedReference == null || namedReference.getRef().getType() != protoTypeForRef(ref)) {
      throw referenceNotFound(ref.getName());
    }
    return Hash.of(namedReference.getRef().getHash());
  }

  protected ReferenceInfo<ByteString> referenceHead(
      NonTransactionalOperationContext ctx, String ref) throws ReferenceNotFoundException {

    NamedReference namedReference = fetchNamedReference(ctx, ref);
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
  @MustBeClosed
  public Stream<RefLog> refLog(Hash offset) throws RefLogNotFoundException {
    return readRefLogStream(NON_TRANSACTIONAL_OPERATION_CONTEXT, offset);
  }
}
