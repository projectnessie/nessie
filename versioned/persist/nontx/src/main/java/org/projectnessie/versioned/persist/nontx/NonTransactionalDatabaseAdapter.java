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
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.randomHash;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.referenceAlreadyExists;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.referenceNotFound;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.repoDescUpdateConflictMessage;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.takeUntilExcludeLast;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.takeUntilIncludeLast;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.transplantConflictMessage;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.verifyExpectedHash;
import static org.projectnessie.versioned.persist.adapter.spi.Traced.trace;
import static org.projectnessie.versioned.persist.adapter.spi.TryLoopState.newTryLoopState;
import static org.projectnessie.versioned.persist.nontx.NonTransactionalOperationContext.NON_TRANSACTIONAL_OPERATION_CONTEXT;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
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
import org.projectnessie.versioned.persist.adapter.GlobalLogCompactionParams;
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
import org.projectnessie.versioned.persist.serialize.AdapterTypes;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.ContentIdWithBytes;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.GlobalStateLogEntry;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.GlobalStatePointer;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.NamedReference;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.RefLogEntry;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.RefLogEntry.Operation;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.RefPointer;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.RefType;

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

  protected NonTransactionalDatabaseAdapter(CONFIG config, StoreWorker<?, ?, ?> storeWorker) {
    super(config, storeWorker);
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

    GlobalStatePointer pointer = fetchGlobalPointer(NON_TRANSACTIONAL_OPERATION_CONTEXT);
    ReferenceInfo<ByteString> refHead = referenceHead(pointer, ref);
    Hash defaultBranchHead = namedRefsDefaultBranchHead(params, pointer);

    Stream<ReferenceInfo<ByteString>> refs = Stream.of(refHead);

    return namedRefsFilterAndEnhance(
            NON_TRANSACTIONAL_OPERATION_CONTEXT, params, defaultBranchHead, refs)
        .findFirst()
        .orElseThrow(() -> referenceNotFound(ref));
  }

  @Override
  public Stream<ReferenceInfo<ByteString>> namedRefs(GetNamedRefsParams params)
      throws ReferenceNotFoundException {
    Preconditions.checkNotNull(params, "Parameter for GetNamedRefsParams must not be null.");
    Preconditions.checkArgument(
        namedRefsAnyRetrieves(params), "Must retrieve branches or tags or both.");

    GlobalStatePointer pointer = fetchGlobalPointer(NON_TRANSACTIONAL_OPERATION_CONTEXT);
    if (pointer == null) {
      return Stream.empty();
    }

    Hash defaultBranchHead = namedRefsDefaultBranchHead(params, pointer);

    Stream<ReferenceInfo<ByteString>> refs =
        pointer.getNamedReferencesList().stream()
            .map(
                r ->
                    ReferenceInfo.of(
                        Hash.of(r.getRef().getHash()),
                        toNamedRef(r.getRef().getType(), r.getName())));

    return namedRefsFilterAndEnhance(
        NON_TRANSACTIONAL_OPERATION_CONTEXT, params, defaultBranchHead, refs);
  }

  @Override
  public Stream<KeyListEntry> keys(Hash commit, KeyFilterPredicate keyFilter)
      throws ReferenceNotFoundException {
    return keysForCommitEntry(NON_TRANSACTIONAL_OPERATION_CONTEXT, commit, keyFilter);
  }

  @Override
  public Hash merge(MergeParams mergeParams)
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
          CasOpVariant.COMMIT,
          (ctx, pointer, branchCommits, newKeyLists) -> {
            Hash currentHead = branchHead(pointer, mergeParams.getToBranch());

            long timeInMicros = commitTimeInMicros();

            Hash newHead =
                mergeAttempt(
                    ctx, timeInMicros, currentHead, branchCommits, newKeyLists, mergeParams);

            if (newHead.equals(currentHead)) {
              // nothing done
              return pointer;
            }

            RefLogEntry newRefLog =
                writeRefLogEntry(
                    ctx,
                    pointer,
                    mergeParams.getToBranch().getName(),
                    RefType.Branch,
                    newHead,
                    RefLogEntry.Operation.MERGE,
                    timeInMicros,
                    Collections.singletonList(mergeParams.getMergeFromHash()));

            // Return hash of last commit (toHead) added to 'targetBranch' (via the casOpLoop)
            return updateGlobalStatePointer(
                mergeParams.getToBranch(), pointer, newHead, null, newRefLog);
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
  public Hash transplant(TransplantParams transplantParams)
      throws ReferenceNotFoundException, ReferenceConflictException {
    try {
      return casOpLoop(
          "transplant",
          transplantParams.getToBranch(),
          CasOpVariant.COMMIT,
          (ctx, pointer, branchCommits, newKeyLists) -> {
            Hash currentHead = branchHead(pointer, transplantParams.getToBranch());

            long timeInMicros = commitTimeInMicros();

            Hash newHead =
                transplantAttempt(
                    ctx, timeInMicros, currentHead, branchCommits, newKeyLists, transplantParams);

            if (newHead.equals(currentHead)) {
              // nothing done
              return pointer;
            }

            RefLogEntry newRefLog =
                writeRefLogEntry(
                    ctx,
                    pointer,
                    transplantParams.getToBranch().getName(),
                    RefType.Branch,
                    newHead,
                    RefLogEntry.Operation.TRANSPLANT,
                    timeInMicros,
                    transplantParams.getSequenceToTransplant());

            // Return hash of last commit (targetHead) added to 'targetBranch' (via the casOpLoop)
            return updateGlobalStatePointer(
                transplantParams.getToBranch(), pointer, newHead, null, newRefLog);
          },
          () -> transplantConflictMessage("Retry-failure", transplantParams));

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
          (ctx, pointer, x, newKeyLists) -> {
            Hash branchHead = branchHead(pointer, commitParams.getToBranch());

            long timeInMicros = commitTimeInMicros();

            CommitLogEntry newBranchCommit =
                commitAttempt(ctx, timeInMicros, branchHead, commitParams, newKeyLists);

            GlobalStateLogEntry newGlobalHead =
                commitParams.getGlobal().isEmpty()
                    ? null
                    : writeGlobalCommit(
                        ctx,
                        timeInMicros,
                        pointer,
                        commitParams.getGlobal().entrySet().stream()
                            .map(e -> ContentIdAndBytes.of(e.getKey(), e.getValue()))
                            .collect(Collectors.toList()));

            RefLogEntry newRefLog =
                writeRefLogEntry(
                    ctx,
                    pointer,
                    commitParams.getToBranch().getName(),
                    RefType.Branch,
                    newBranchCommit.getHash(),
                    RefLogEntry.Operation.COMMIT,
                    timeInMicros,
                    Collections.emptyList());

            return updateGlobalStatePointer(
                commitParams.getToBranch(),
                pointer,
                newBranchCommit.getHash(),
                newGlobalHead,
                newRefLog);
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
          CasOpVariant.REF_UPDATE,
          (ctx, pointer, branchCommits, newKeyLists) -> {
            if (refFromGlobalState(pointer, ref.getName()) != null) {
              throw referenceAlreadyExists(ref);
            }

            Hash hash = target;
            if (hash == null) {
              // Special case: Don't validate, if the 'target' parameter is null.
              // This is mostly used for tests that re-create the default-branch.
              hash = NO_ANCESTOR;
            }

            validateHashExists(ctx, hash);

            RefType refType = ref instanceof TagName ? RefType.Tag : RefType.Branch;
            RefLogEntry newRefLog =
                writeRefLogEntry(
                    ctx,
                    pointer,
                    ref.getName(),
                    refType,
                    hash,
                    RefLogEntry.Operation.CREATE_REFERENCE,
                    commitTimeInMicros(),
                    Collections.emptyList());

            return updateGlobalStatePointer(ref, pointer, hash, null, newRefLog);
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
          (ctx, pointer, branchCommits, newKeyLists) -> {
            Hash branchHead = branchHead(pointer, reference);
            verifyExpectedHash(branchHead, reference, expectedHead);

            RefType refType = reference instanceof TagName ? RefType.Tag : RefType.Branch;
            RefLogEntry newRefLog =
                writeRefLogEntry(
                    ctx,
                    pointer,
                    reference.getName(),
                    refType,
                    branchHead,
                    RefLogEntry.Operation.DELETE_REFERENCE,
                    commitTimeInMicros(),
                    Collections.emptyList());

            return updateGlobalStatePointer(reference, pointer, null, null, newRefLog);
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
          (ctx, pointer, branchCommits, newKeyLists) -> {
            Hash beforeAssign = branchHead(pointer, assignee);
            verifyExpectedHash(beforeAssign, assignee, expectedHead);

            validateHashExists(ctx, assignTo);

            RefType refType = assignee instanceof TagName ? RefType.Tag : RefType.Branch;
            RefLogEntry newRefLog =
                writeRefLogEntry(
                    ctx,
                    pointer,
                    assignee.getName(),
                    refType,
                    assignTo,
                    RefLogEntry.Operation.ASSIGN_REFERENCE,
                    commitTimeInMicros(),
                    Collections.singletonList(beforeAssign));

            return updateGlobalStatePointer(assignee, pointer, assignTo, null, newRefLog);
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
      GlobalStateLogEntry globalHead;
      RefLogEntry newRefLog;
      try {
        long timeInMicros = commitTimeInMicros();
        GlobalStatePointer dummyPointer =
            GlobalStatePointer.newBuilder()
                .setGlobalId(NO_ANCESTOR.asBytes())
                .setGlobalLogHead(NO_ANCESTOR.asBytes())
                .addGlobalParentsInclHead(NO_ANCESTOR.asBytes())
                .setRefLogId(NO_ANCESTOR.asBytes())
                .addRefLogParentsInclHead(NO_ANCESTOR.asBytes())
                .build();
        globalHead = writeGlobalCommit(ctx, timeInMicros, dummyPointer, Collections.emptyList());

        newRefLog =
            writeRefLogEntry(
                ctx,
                dummyPointer,
                defaultBranchName,
                RefType.Branch,
                NO_ANCESTOR,
                RefLogEntry.Operation.CREATE_REFERENCE,
                commitTimeInMicros(),
                Collections.emptyList());
      } catch (ReferenceConflictException e) {
        throw new RuntimeException(e);
      }

      unsafeWriteGlobalPointer(
          ctx,
          GlobalStatePointer.newBuilder()
              .setGlobalId(globalHead.getId())
              .setGlobalLogHead(globalHead.getId())
              .addNamedReferences(
                  NamedReference.newBuilder()
                      .setName(defaultBranchName)
                      .setRef(
                          RefPointer.newBuilder()
                              .setType(RefType.Branch)
                              .setHash(NO_ANCESTOR.asBytes())))
              .setRefLogId(newRefLog.getRefLogId())
              .addRefLogParentsInclHead(newRefLog.getRefLogId())
              .addRefLogParentsInclHead(NO_ANCESTOR.asBytes())
              .addGlobalParentsInclHead(globalHead.getId())
              .addGlobalParentsInclHead(NO_ANCESTOR.asBytes())
              .build());
    }
  }

  @Override
  public Stream<ContentId> globalKeys() {
    return globalLogFetcher(NON_TRANSACTIONAL_OPERATION_CONTEXT)
        .flatMap(e -> e.getPutsList().stream())
        .map(ProtoSerialization::protoToContentIdAndBytes)
        .map(ContentIdAndBytes::getContentId)
        .distinct();
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
  public Stream<ContentIdAndBytes> globalContent(Set<ContentId> keys) {
    HashSet<ContentId> remaining = new HashSet<>(keys);

    Stream<GlobalStateLogEntry> stream = globalLogFetcher(NON_TRANSACTIONAL_OPERATION_CONTEXT);

    return takeUntilIncludeLast(stream, x -> remaining.isEmpty())
        .flatMap(e -> e.getPutsList().stream())
        .map(ProtoSerialization::protoToContentIdAndBytes)
        .filter(kct -> remaining.remove(kct.getContentId()));
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
    Map<String, Map<String, String>> result = new HashMap<>();
    result.put("compactGlobalLog", compactGlobalLog(params.getGlobalLogCompactionParams()));
    return result;
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

  /**
   * Produces an updated copy of {@code pointer}.
   *
   * <p>Any previous appearance of {@code target.getName()} in the list of named references is
   * removed. If {@code toHead} is not null, {@code target} it will appear as the first element in
   * the list of named references. In other words, a reference to be deleted will be removed from
   * the list of named references, an updated (or created) reference will appear as the first
   * element of the list of named references.
   */
  protected static GlobalStatePointer updateGlobalStatePointer(
      NamedRef target,
      GlobalStatePointer pointer,
      @Nullable Hash toHead,
      GlobalStateLogEntry newGlobalHead,
      RefLogEntry newRefLog) {

    ByteString newGlobalId = randomHash().asBytes();

    GlobalStatePointer.Builder newPointer =
        GlobalStatePointer.newBuilder()
            .setGlobalId(newGlobalId)
            .setRefLogId(newRefLog.getRefLogId());

    if (newGlobalHead != null) {
      newPointer
          .setGlobalLogHead(newGlobalHead.getId())
          .addGlobalParentsInclHead(newGlobalHead.getId())
          .addAllGlobalParentsInclHead(newGlobalHead.getParentsList());
    } else {
      newPointer
          .setGlobalLogHead(globalLogHead(pointer))
          .addAllGlobalParentsInclHead(pointer.getGlobalParentsInclHeadList());
    }

    String refName = target.getName();
    if (toHead != null) {
      // Most recently updated references first
      newPointer.addNamedReferences(
          NamedReference.newBuilder()
              .setName(refName)
              .setRef(
                  RefPointer.newBuilder()
                      .setType(protoTypeForRef(target))
                      .setHash(toHead.asBytes())));
    }
    pointer.getNamedReferencesList().stream()
        .filter(namedRef -> !refName.equals(namedRef.getName()))
        .forEach(newPointer::addNamedReferences);
    newPointer
        .addRefLogParentsInclHead(newRefLog.getRefLogId())
        .addAllRefLogParentsInclHead(newRefLog.getParentsList());
    return newPointer.build();
  }

  /** Retrieves the {@link RefPointer} for a reference name, returns {@code null} if not found. */
  protected static RefPointer refFromGlobalState(GlobalStatePointer pointer, String refName) {
    for (NamedReference namedReference : pointer.getNamedReferencesList()) {
      if (namedReference.getName().equals(refName)) {
        return namedReference.getRef();
      }
    }
    return null;
  }

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
   * Convenience method for {@link AbstractDatabaseAdapter#hashOnRef(Object, NamedRef, Optional,
   * Hash) hashOnRef(ctx, reference.getReference(), branchHead(fetchGlobalPointer(ctx), reference),
   * reference.getHashOnReference())}.
   */
  protected Hash hashOnRef(
      NonTransactionalOperationContext ctx, NamedRef reference, Optional<Hash> hashOnRef)
      throws ReferenceNotFoundException {
    return hashOnRef(ctx, reference, hashOnRef, fetchGlobalPointer(ctx));
  }

  /**
   * Convenience method for {@link AbstractDatabaseAdapter#hashOnRef(Object, NamedRef, Optional,
   * Hash) hashOnRef(ctx, reference.getReference(), branchHead(pointer, reference),
   * reference.getHashOnReference())}.
   */
  protected Hash hashOnRef(
      NonTransactionalOperationContext ctx,
      NamedRef reference,
      Optional<Hash> hashOnRef,
      GlobalStatePointer pointer)
      throws ReferenceNotFoundException {
    return hashOnRef(ctx, reference, hashOnRef, branchHead(pointer, reference));
  }

  /**
   * "Body" of a Compare-And-Swap loop that returns the value to apply. {@link #casOpLoop(String,
   * NamedRef, CasOpVariant, CasOp, Supplier)} then tries to perform the Compare-And-Swap using the
   * known "current value", as passed via the {@code pointer} parameter to {@link
   * #apply(NonTransactionalOperationContext, GlobalStatePointer, Consumer, Consumer)}, and the "new
   * value" from the return value.
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
     * @return "new value" that {@link #casOpLoop(String, NamedRef, CasOpVariant, CasOp, Supplier)}
     *     tries to apply
     */
    GlobalStatePointer apply(
        NonTransactionalOperationContext ctx,
        GlobalStatePointer pointer,
        Consumer<Hash> branchCommits,
        Consumer<Hash> newKeyLists)
        throws VersionStoreException;
  }

  enum CasOpVariant {
    /**
     * For commit/merge/transplant, which add one or more commits to that named reference and then
     * update the global named-reference-to-HEAD map.
     */
    COMMIT(false, true),
    /**
     * For {@link #create(NamedRef, Hash)} and {@link #assign(NamedRef, Optional, Hash)}, which only
     * update the updates the HEAD of a named reference, but does not add a commit.
     */
    REF_UPDATE(false, false),
    /** For {@link #delete(NamedRef, Optional)}, which delete the reference and does not. */
    DELETE_REF(true, false);

    /**
     * Whether the operation is a "delete-reference" operation, which means that this function
     * cannot return any new HEAD, because the reference no longer exists.
     */
    final boolean deleteRef;
    /**
     * Whether the hash returned by {@code casOp} will be a new commit and/or {@code * casOp}
     * produced more commits (think: merge+transplant) via the {@code individualCommits} * argument
     * to {@link CasOp#apply(NonTransactionalOperationContext, GlobalStatePointer, Consumer,
     * Consumer)}. Those commits will be unconditionally deleted, if this {@code commitOp} flag is *
     * {@code true}.
     */
    final boolean commitOp;

    CasOpVariant(boolean deleteRef, boolean commitOp) {
      this.deleteRef = deleteRef;
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
        GlobalStatePointer pointer = fetchGlobalPointer(ctx);
        if (pointer == null) {
          throw referenceNotFound(ref);
        }
        Set<Hash> individualCommits = new HashSet<>();
        Set<Hash> individualKeyLists = new HashSet<>();

        GlobalStatePointer newPointer =
            casOp.apply(ctx, pointer, individualCommits::add, individualKeyLists::add);
        if (newPointer.getGlobalId().equals(pointer.getGlobalId())) {
          return tryState.success(branchHead(pointer, ref));
        }
        Hash branchHead = opVariant.deleteRef ? null : branchHead(newPointer, ref);

        if (pointer.getGlobalId().equals(newPointer.getGlobalId())) {
          throw hashCollisionDetected();
        }

        if (globalPointerCas(ctx, pointer, newPointer)) {
          return tryState.success(branchHead);
        } else if (opVariant.commitOp) {
          if (branchHead != null) {
            individualCommits.add(branchHead);
          }
          Optional<Hash> newGlobalLogHead =
              globalLogHead(pointer).equals(globalLogHead(newPointer))
                  ? Optional.empty()
                  : Optional.of(Hash.of(globalLogHead(newPointer)));
          cleanUpCommitCas(
              ctx,
              newGlobalLogHead,
              individualCommits,
              individualKeyLists,
              Hash.of(newPointer.getRefLogId()));
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
  protected final void writeGlobalCommit(
      NonTransactionalOperationContext ctx, GlobalStateLogEntry entry)
      throws ReferenceConflictException {
    try (Traced ignore = trace("writeGlobalCommit")) {
      doWriteGlobalCommit(ctx, entry);
    }
  }

  protected abstract void doWriteGlobalCommit(
      NonTransactionalOperationContext ctx, GlobalStateLogEntry entry)
      throws ReferenceConflictException;

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

  protected GlobalStateLogEntry writeGlobalCommit(
      NonTransactionalOperationContext ctx,
      long timeInMicros,
      GlobalStatePointer pointer,
      List<ContentIdAndBytes> globals)
      throws ReferenceConflictException {

    Hash parentHash = Hash.of(globalLogHead(pointer));

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
    Stream<ByteString> newParents;
    if (pointer.getGlobalParentsInclHeadCount() == 0
        || !globalLogHead(pointer).equals(pointer.getGlobalParentsInclHead(0))) {
      // Before Nessie 0.21.0

      newParents = Stream.of(parentHash.asBytes());
      GlobalStateLogEntry currentEntry = fetchFromGlobalLog(ctx, parentHash);
      if (currentEntry != null) {
        newParents =
            Stream.concat(
                newParents,
                currentEntry.getParentsList().stream()
                    .limit(config.getParentsPerGlobalCommit() - 1));
      }
    } else {
      // Since Nessie 0.21.0

      newParents =
          pointer.getGlobalParentsInclHeadList().stream().limit(config.getParentsPerGlobalCommit());
    }

    GlobalStateLogEntry.Builder entry = newGlobalLogEntryBuilder(timeInMicros);
    newParents.forEach(entry::addParents);
    globals.forEach(g -> entry.addPuts(ProtoSerialization.toProto(g)));
    GlobalStateLogEntry globalLogEntry = entry.build();
    writeGlobalCommit(ctx, globalLogEntry);

    return globalLogEntry;
  }

  protected GlobalStateLogEntry.Builder newGlobalLogEntryBuilder(long timeInMicros) {
    return GlobalStateLogEntry.newBuilder()
        .setCreatedTime(timeInMicros)
        .setId(randomHash().asBytes());
  }

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
   * If a {@link #globalPointerCas(NonTransactionalOperationContext, GlobalStatePointer,
   * GlobalStatePointer)} failed, {@link
   * org.projectnessie.versioned.persist.adapter.DatabaseAdapter#commit(CommitParams)} calls this
   * function to remove the optimistically written data.
   *
   * <p>Implementation notes: non-transactional implementations <em>must</em> delete entries for the
   * given keys, no-op for transactional implementations.
   */
  protected final void cleanUpCommitCas(
      NonTransactionalOperationContext ctx,
      Optional<Hash> globalHead,
      Set<Hash> branchCommits,
      Set<Hash> newKeyLists,
      Hash refLogId) {
    try (Traced ignore =
        trace("cleanUpCommitCas")
            .tag(TAG_COMMIT_COUNT, branchCommits.size())
            .tag(TAG_KEY_LIST_COUNT, newKeyLists.size())) {
      doCleanUpCommitCas(ctx, globalHead, branchCommits, newKeyLists, refLogId);
    }
  }

  protected abstract void doCleanUpCommitCas(
      NonTransactionalOperationContext ctx,
      Optional<Hash> globalHead,
      Set<Hash> branchCommits,
      Set<Hash> newKeyLists,
      Hash refLogId);

  protected final void cleanUpGlobalLog(
      NonTransactionalOperationContext ctx, Collection<Hash> globalIds) {
    try (Traced ignore = trace("cleanUpGlobalLog").tag(TAG_COMMIT_COUNT, globalIds.size())) {
      doCleanUpGlobalLog(ctx, globalIds);
    }
  }

  protected abstract void doCleanUpGlobalLog(
      NonTransactionalOperationContext ctx, Collection<Hash> globalIds);

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
    if (ref == null) {
      return null;
    }
    if (pointer == null) {
      throw referenceNotFound(ref);
    }
    RefPointer branchHead = refFromGlobalState(pointer, ref.getName());
    if (branchHead == null || !ref.equals(toNamedRef(branchHead.getType(), ref.getName()))) {
      throw referenceNotFound(ref);
    }
    return Hash.of(branchHead.getHash());
  }

  protected static ReferenceInfo<ByteString> referenceHead(GlobalStatePointer pointer, String ref)
      throws ReferenceNotFoundException {
    if (pointer == null) {
      throw referenceNotFound(ref);
    }
    RefPointer head = refFromGlobalState(pointer, ref);
    if (head == null) {
      throw referenceNotFound(ref);
    }
    return ReferenceInfo.of(Hash.of(head.getHash()), toNamedRef(head.getType(), ref));
  }

  /**
   * Retrieves the hash of the default branch specified in {@link
   * GetNamedRefsParams#getBaseReference()}, if the retrieve options in {@link GetNamedRefsParams}
   * require it.
   */
  private Hash namedRefsDefaultBranchHead(GetNamedRefsParams params, GlobalStatePointer pointer)
      throws ReferenceNotFoundException {
    if (namedRefsRequiresBaseReference(params)) {
      Preconditions.checkNotNull(params.getBaseReference(), "Base reference name missing.");
      return branchHead(pointer, params.getBaseReference());
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
    return globalLogFetcher(ctx, fetchGlobalPointer(ctx));
  }

  private Stream<GlobalStateLogEntry> globalLogFetcher(
      NonTransactionalOperationContext ctx, GlobalStatePointer pointer) {
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
      GlobalStatePointer pointer,
      String refName,
      RefType refType,
      Hash commitHash,
      Operation operation,
      long timeInMicros,
      List<Hash> sourceHashes)
      throws ReferenceConflictException {

    Hash parentHash = Hash.of(pointer.getRefLogId());
    Hash currentRefLogId = randomHash();

    Stream<ByteString> newParents;
    if (pointer.getRefLogParentsInclHeadCount() == 0
        || !pointer.getRefLogId().equals(pointer.getRefLogParentsInclHead(0))) {
      // Before Nessie 0.21.0

      newParents = Stream.of(parentHash.asBytes());
      RefLog currentEntry = fetchFromRefLog(ctx, parentHash);
      if (currentEntry != null) {
        newParents =
            Stream.concat(
                newParents,
                currentEntry.getParents().stream()
                    .limit(config.getParentsPerRefLogEntry() - 1)
                    .map(Hash::asBytes));
      }
    } else {
      // Since Nessie 0.21.0

      newParents =
          pointer.getRefLogParentsInclHeadList().stream().limit(config.getParentsPerRefLogEntry());
    }

    RefLogEntry.Builder entry =
        RefLogEntry.newBuilder()
            .setRefLogId(currentRefLogId.asBytes())
            .setRefName(ByteString.copyFromUtf8(refName))
            .setRefType(refType)
            .setCommitHash(commitHash.asBytes())
            .setOperationTime(timeInMicros)
            .setOperation(operation);
    sourceHashes.forEach(hash -> entry.addSourceHashes(hash.asBytes()));
    newParents.forEach(entry::addParents);
    RefLogEntry refLogEntry = entry.build();

    writeRefLog(ctx, refLogEntry);

    return refLogEntry;
  }

  @SuppressWarnings("StaticAssignmentOfThrowable")
  private static final RuntimeException COMPACTION_NOT_NECESSARY_LENGTH = new RuntimeException();

  @SuppressWarnings("StaticAssignmentOfThrowable")
  private static final RuntimeException COMPACTION_NOT_NECESSARY_WITHIN = new RuntimeException();

  protected Map<String, String> compactGlobalLog(
      GlobalLogCompactionParams globalLogCompactionParams) {
    if (!globalLogCompactionParams.isEnabled()) {
      return ImmutableMap.of("compacted", "false", "reason", "not enabled");
    }

    // Not using casOpLoop() here, as it is simpler than adopting casOpLoop().
    try (TryLoopState tryState =
        newTryLoopState(
            "compact-global-log",
            ts ->
                repoDescUpdateConflictMessage(
                    String.format(
                        "%s after %d retries, %d ms",
                        "Retry-Failure", ts.getRetries(), ts.getDuration(TimeUnit.MILLISECONDS))),
            this::tryLoopStateCompletion,
            config)) {

      CompactionStats stats = new CompactionStats();
      while (true) {
        NonTransactionalOperationContext ctx = NON_TRANSACTIONAL_OPERATION_CONTEXT;

        GlobalStatePointer pointer = fetchGlobalPointer(ctx);

        // Collect the old global-log-ids, to delete those after compaction
        List<Hash> oldLogIds = new ArrayList<>();
        // Map with all global contents.
        Map<String, ByteString> globalContents = new HashMap<>();
        // Content-IDs, most recently updated contents first.
        List<String> contentIdsByRecency = new ArrayList<>();

        // Read the global log - from the most recent global-log entry to the oldest.
        try (Stream<GlobalStateLogEntry> globalLog = globalLogFetcher(ctx, pointer)) {
          globalLog.forEach(
              e -> {
                if (stats.read < globalLogCompactionParams.getNoCompactionWhenCompactedWithin()
                    && stats.puts > stats.read) {
                  // First page in the global-log contains at least one compacted entry, so
                  // do not compact.
                  throw COMPACTION_NOT_NECESSARY_WITHIN;
                }
                stats.read++;
                oldLogIds.add(Hash.of(e.getId()));
                for (ContentIdWithBytes put : e.getPutsList()) {
                  stats.puts++;
                  String cid = put.getContentId().getId();
                  if (globalContents.putIfAbsent(cid, put.getValue()) == null) {
                    stats.uniquePuts++;
                    contentIdsByRecency.add(cid);
                  }
                }
              });

          if (stats.read < globalLogCompactionParams.getNoCompactionUpToLength()) {
            // Global log does not have more global-log entries than can be fetched with a
            // single-bulk read, so do not compact at all.
            throw COMPACTION_NOT_NECESSARY_LENGTH;
          }
        } catch (RuntimeException e) {
          if (e == COMPACTION_NOT_NECESSARY_WITHIN) {
            tryState.success(null);
            return ImmutableMap.of(
                "compacted",
                "false",
                "reason",
                String.format(
                    "compacted entry within %d most recent log entries",
                    globalLogCompactionParams.getNoCompactionWhenCompactedWithin()));
          }
          if (e == COMPACTION_NOT_NECESSARY_LENGTH) {
            tryState.success(null);
            return ImmutableMap.of(
                "compacted",
                "false",
                "reason",
                String.format(
                    "less than %d entries", globalLogCompactionParams.getNoCompactionUpToLength()));
          }
          throw e;
        }

        // Collect the IDs of the written global-log-entries, to delete those when the CAS
        // operation failed
        List<ByteString> newLogIds = new ArrayList<>();

        // Reverse the order of content-IDs, most recently updated contents LAST.
        // Do this to have the active contents closer to the HEAD of the global log.
        Collections.reverse(contentIdsByRecency);

        // Maintain the list of global-log-entry parent IDs, but in reverse order as in
        // GlobalLogEntry for easier management here.
        List<ByteString> globalParentsReverse = new ArrayList<>(config.getParentsPerGlobalCommit());
        globalParentsReverse.add(NO_ANCESTOR.asBytes());

        GlobalStateLogEntry.Builder currentEntry =
            newGlobalLogEntryBuilder(commitTimeInMicros()).addParents(globalParentsReverse.get(0));

        for (String cid : contentIdsByRecency) {
          if (currentEntry.buildPartial().getSerializedSize() >= config.getGlobalLogEntrySize()) {
            compactGlobalLogWriteEntry(ctx, stats, globalParentsReverse, currentEntry, newLogIds);

            // Prepare new entry
            currentEntry = newGlobalLogEntryBuilder(commitTimeInMicros());
            for (int i = globalParentsReverse.size() - 1; i >= 0; i--) {
              currentEntry.addParents(globalParentsReverse.get(i));
            }
          }

          ByteString value = globalContents.get(cid);
          currentEntry.addPuts(
              ContentIdWithBytes.newBuilder()
                  .setContentId(AdapterTypes.ContentId.newBuilder().setId(cid))
                  .setTypeUnused(0)
                  .setValue(value)
                  .build());
        }

        compactGlobalLogWriteEntry(ctx, stats, globalParentsReverse, currentEntry, newLogIds);

        GlobalStatePointer newPointer =
            GlobalStatePointer.newBuilder()
                .addAllNamedReferences(pointer.getNamedReferencesList())
                .addAllRefLogParentsInclHead(pointer.getRefLogParentsInclHeadList())
                .setRefLogId(pointer.getRefLogId())
                .setGlobalId(currentEntry.getId())
                .addGlobalParentsInclHead(currentEntry.getId())
                .addAllGlobalParentsInclHead(currentEntry.getParentsList())
                .build();

        stats.addToTotal();

        // CAS global pointer
        if (globalPointerCas(ctx, pointer, newPointer)) {
          tryState.success(null);
          cleanUpGlobalLog(ctx, oldLogIds);
          return stats.asMap(tryState);
        }

        // Note: if it turns out that there are too many CAS retries happening, the overall
        // mechanism can be updated as follows. Since the approach below is much more complex
        // and harder to test, if's not part of the initial implementation.
        //
        // 1. Read the whole global-log as currently, but outside the actual CAS-loop.
        //    Save the current HEAD of the global-log
        // 2. CAS-loop:
        // 2.1. Construct and write the new global-log
        // 2.2. Try the CAS, if it succeeds, fine
        // 2.3. If the CAS failed:
        // 2.3.1. Clean up the optimistically written new global-log
        // 2.3.2. Read the global-log from its new HEAD up to the current HEAD from step 1.
        //        Only add the most-recent values for the content-IDs in the incrementally
        //        read global-log
        // 2.3.3. Remember the "new HEAD" as the "current HEAD"
        // 2.3.4. Continue to step 2.1.

        cleanUpGlobalLog(ctx, newLogIds.stream().map(Hash::of).collect(Collectors.toList()));
        stats.onRetry();
        tryState.retry();
      }
    } catch (ReferenceConflictException e) {
      throw new RuntimeException(e);
    }
  }

  private void compactGlobalLogWriteEntry(
      NonTransactionalOperationContext ctx,
      CompactionStats stats,
      List<ByteString> globalParentsReverse,
      GlobalStateLogEntry.Builder currentEntry,
      List<ByteString> newLogIds)
      throws ReferenceConflictException {
    writeGlobalCommit(ctx, currentEntry.build());
    newLogIds.add(currentEntry.getId());
    stats.written++;

    if (globalParentsReverse.size() == config.getParentsPerGlobalCommit()) {
      globalParentsReverse.remove(0);
    }
    globalParentsReverse.add(currentEntry.getId());
  }

  private static final class CompactionStats {
    long written;
    long read;
    long puts;
    long uniquePuts;
    long totalWritten;
    long totalRead;

    Map<String, String> asMap(TryLoopState tryState) {
      return ImmutableMap.of(
          "compacted",
          "true",
          "entries.written",
          Long.toString(written),
          "entries.read",
          Long.toString(read),
          "entries.puts",
          Long.toString(puts),
          "entries.uniquePuts",
          Long.toString(uniquePuts),
          "entries.written.total",
          Long.toString(totalWritten),
          "entries.read.total",
          Long.toString(totalRead),
          "duration.millis",
          Long.toString(tryState.getDuration(TimeUnit.MILLISECONDS)),
          "cas-retries",
          Long.toString(tryState.getRetries()));
    }

    public void addToTotal() {
      totalRead += read;
      totalWritten += written;
    }

    void onRetry() {
      read = written = puts = uniquePuts = 0;
    }
  }

  @Override
  public Stream<RefLog> refLog(Hash offset) throws RefLogNotFoundException {
    return readRefLogStream(NON_TRANSACTIONAL_OPERATION_CONTEXT, offset);
  }
}
