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
import com.google.protobuf.ByteString;
import java.util.Collection;
import java.util.Collections;
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
import java.util.function.ToIntFunction;
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
import org.projectnessie.versioned.TagName;
import org.projectnessie.versioned.VersionStoreException;
import org.projectnessie.versioned.persist.adapter.CommitAttempt;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.ContentAndState;
import org.projectnessie.versioned.persist.adapter.ContentId;
import org.projectnessie.versioned.persist.adapter.ContentIdAndBytes;
import org.projectnessie.versioned.persist.adapter.ContentIdWithType;
import org.projectnessie.versioned.persist.adapter.ContentVariantSupplier;
import org.projectnessie.versioned.persist.adapter.Difference;
import org.projectnessie.versioned.persist.adapter.KeyFilterPredicate;
import org.projectnessie.versioned.persist.adapter.KeyListEntity;
import org.projectnessie.versioned.persist.adapter.KeyWithType;
import org.projectnessie.versioned.persist.adapter.RefLog;
import org.projectnessie.versioned.persist.adapter.RepoDescription;
import org.projectnessie.versioned.persist.adapter.spi.AbstractDatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.spi.Traced;
import org.projectnessie.versioned.persist.adapter.spi.TryLoopState;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.ContentIdWithBytes;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.GlobalStateLogEntry;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.GlobalStatePointer;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.NamedReference;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.RefLogEntry;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.RefLogEntry.Operation;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.RefLogEntry.RefType;
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

  protected NonTransactionalDatabaseAdapter(
      CONFIG config, ContentVariantSupplier contentVariantSupplier) {
    super(config, contentVariantSupplier);
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
  public Stream<KeyWithType> keys(Hash commit, KeyFilterPredicate keyFilter)
      throws ReferenceNotFoundException {
    return keysForCommitEntry(NON_TRANSACTIONAL_OPERATION_CONTEXT, commit, keyFilter);
  }

  @Override
  public Hash merge(
      Hash from,
      BranchName toBranch,
      Optional<Hash> expectedHead,
      Function<ByteString, ByteString> updateCommitMetadata)
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
          toBranch,
          CasOpVariant.COMMIT,
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
                    newKeyLists,
                    updateCommitMetadata);

            GlobalStateLogEntry newGlobalHead =
                writeGlobalCommit(ctx, timeInMicros, pointer, Collections.emptyList());

            RefLogEntry newRefLog =
                writeRefLogEntry(
                    ctx,
                    pointer,
                    toBranch.getName(),
                    RefLogEntry.RefType.Branch,
                    toHead,
                    RefLogEntry.Operation.MERGE,
                    timeInMicros,
                    Collections.singletonList(from));

            // Return hash of last commit (toHead) added to 'targetBranch' (via the casOpLoop)
            return updateGlobalStatePointer(toBranch, pointer, toHead, newGlobalHead, newRefLog);
          },
          () -> mergeConflictMessage("Retry-failure", from, toBranch, expectedHead));
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
      Optional<Hash> expectedHead,
      List<Hash> sequenceToTransplant,
      Function<ByteString, ByteString> updateCommitMetadata)
      throws ReferenceNotFoundException, ReferenceConflictException {
    try {
      return casOpLoop(
          "transplant",
          targetBranch,
          CasOpVariant.COMMIT,
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
                    newKeyLists,
                    updateCommitMetadata);

            GlobalStateLogEntry newGlobalHead =
                writeGlobalCommit(ctx, timeInMicros, pointer, Collections.emptyList());

            RefLogEntry newRefLog =
                writeRefLogEntry(
                    ctx,
                    pointer,
                    targetBranch.getName(),
                    RefLogEntry.RefType.Branch,
                    targetHead,
                    RefLogEntry.Operation.TRANSPLANT,
                    timeInMicros,
                    sequenceToTransplant);

            // Return hash of last commit (targetHead) added to 'targetBranch' (via the casOpLoop)
            return updateGlobalStatePointer(
                targetBranch, pointer, targetHead, newGlobalHead, newRefLog);
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
          "commit",
          commitAttempt.getCommitToBranch(),
          CasOpVariant.COMMIT,
          (ctx, pointer, x, newKeyLists) -> {
            Hash branchHead = branchHead(pointer, commitAttempt.getCommitToBranch());

            long timeInMicros = commitTimeInMicros();

            CommitLogEntry newBranchCommit =
                commitAttempt(ctx, timeInMicros, branchHead, commitAttempt, newKeyLists);

            GlobalStateLogEntry newGlobalHead =
                writeGlobalCommit(
                    ctx,
                    timeInMicros,
                    pointer,
                    commitAttempt.getGlobal().entrySet().stream()
                        .map(e -> ContentIdAndBytes.of(e.getKey(), (byte) 0, e.getValue()))
                        .collect(Collectors.toList()));

            RefLogEntry newRefLog =
                writeRefLogEntry(
                    ctx,
                    pointer,
                    commitAttempt.getCommitToBranch().getName(),
                    RefLogEntry.RefType.Branch,
                    newBranchCommit.getHash(),
                    RefLogEntry.Operation.COMMIT,
                    timeInMicros,
                    Collections.emptyList());

            return updateGlobalStatePointer(
                commitAttempt.getCommitToBranch(),
                pointer,
                newBranchCommit.getHash(),
                newGlobalHead,
                newRefLog);
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

            // Need a new empty global-log entry to be able to CAS
            GlobalStateLogEntry newGlobalHead = noopGlobalLogEntry(ctx, pointer);

            RefLogEntry.RefType refType =
                ref instanceof TagName ? RefLogEntry.RefType.Tag : RefLogEntry.RefType.Branch;
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

            return updateGlobalStatePointer(ref, pointer, hash, newGlobalHead, newRefLog);
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
            GlobalStateLogEntry newGlobalHead = noopGlobalLogEntry(ctx, pointer);

            RefLogEntry.RefType refType =
                reference instanceof TagName ? RefLogEntry.RefType.Tag : RefLogEntry.RefType.Branch;
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

            return updateGlobalStatePointer(reference, pointer, null, newGlobalHead, newRefLog);
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

            GlobalStateLogEntry newGlobalHead = noopGlobalLogEntry(ctx, pointer);

            RefLogEntry.RefType refType =
                assignee instanceof TagName ? RefLogEntry.RefType.Tag : RefLogEntry.RefType.Branch;
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

            return updateGlobalStatePointer(assignee, pointer, assignTo, newGlobalHead, newRefLog);
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
                RefLogEntry.RefType.Branch,
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
              .addNamedReferences(
                  NamedReference.newBuilder()
                      .setName(defaultBranchName)
                      .setRef(
                          RefPointer.newBuilder()
                              .setType(Type.Branch)
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
  public Stream<ContentIdWithType> globalKeys(ToIntFunction<ByteString> contentTypeExtractor) {
    return globalLogFetcher(NON_TRANSACTIONAL_OPERATION_CONTEXT)
        .flatMap(e -> e.getPutsList().stream())
        .map(ProtoSerialization::protoToContentIdAndBytes)
        .map(ContentIdAndBytes::asIdWithType)
        .distinct();
  }

  @Override
  public Optional<ContentIdAndBytes> globalContent(
      ContentId contentId, ToIntFunction<ByteString> contentTypeExtractor) {
    return globalLogFetcher(NON_TRANSACTIONAL_OPERATION_CONTEXT)
        .flatMap(e -> e.getPutsList().stream())
        .map(ProtoSerialization::protoToContentIdAndBytes)
        .filter(entry -> contentId.equals(entry.getContentId()))
        .map(
            cb ->
                ContentIdAndBytes.of(
                    cb.getContentId(),
                    (byte) contentTypeExtractor.applyAsInt(cb.getValue()),
                    cb.getValue()))
        .findFirst();
  }

  @Override
  public Stream<ContentIdAndBytes> globalContent(
      Set<ContentId> keys, ToIntFunction<ByteString> contentTypeExtractor) {
    HashSet<ContentId> remaining = new HashSet<>(keys);

    Stream<GlobalStateLogEntry> stream = globalLogFetcher(NON_TRANSACTIONAL_OPERATION_CONTEXT);

    return takeUntilIncludeLast(stream, x -> remaining.isEmpty())
        .flatMap(e -> e.getPutsList().stream())
        .map(
            c ->
                ProtoSerialization.protoToContentIdAndBytes(
                    c, contentTypeExtractor.applyAsInt(c.getValue())))
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

    GlobalStatePointer.Builder newPointer =
        GlobalStatePointer.newBuilder()
            .setGlobalId(newGlobalHead.getId())
            .setRefLogId(newRefLog.getRefLogId());
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
        .addAllRefLogParentsInclHead(newRefLog.getParentsList())
        .addGlobalParentsInclHead(newGlobalHead.getId())
        .addAllGlobalParentsInclHead(newGlobalHead.getParentsList());
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
  protected static Type protoTypeForRef(NamedRef target) {
    Type type;
    if (target instanceof BranchName) {
      type = Type.Branch;
    } else if (target instanceof TagName) {
      type = Type.Tag;
    } else {
      throw new IllegalArgumentException(target.getClass().getSimpleName());
    }
    return type;
  }

  /**
   * Transform the protobuf-enum-value for the named-reference-type plus the reference name into a
   * {@link NamedRef}.
   */
  protected static NamedRef toNamedRef(Type type, String name) {
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
          cleanUpCommitCas(
              ctx,
              Hash.of(newPointer.getGlobalId()),
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

    Hash parentHash = Hash.of(pointer.getGlobalId());
    Hash hash = randomHash();

    Stream<ByteString> newParents;
    if (pointer.getGlobalParentsInclHeadCount() == 0
        || !pointer.getGlobalId().equals(pointer.getGlobalParentsInclHead(0))) {
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

    GlobalStateLogEntry.Builder entry =
        GlobalStateLogEntry.newBuilder().setCreatedTime(timeInMicros).setId(hash.asBytes());
    newParents.forEach(entry::addParents);
    globals.forEach(g -> entry.addPuts(ProtoSerialization.toProto(g)));
    GlobalStateLogEntry globalLogEntry = entry.build();
    writeGlobalCommit(ctx, globalLogEntry);

    return globalLogEntry;
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
   * org.projectnessie.versioned.persist.adapter.DatabaseAdapter#commit(CommitAttempt)} calls this
   * function to remove the optimistically written data.
   *
   * <p>Implementation notes: non-transactional implementations <em>must</em> delete entries for the
   * given keys, no-op for transactional implementations.
   */
  protected final void cleanUpCommitCas(
      NonTransactionalOperationContext ctx,
      Hash globalId,
      Set<Hash> branchCommits,
      Set<Hash> newKeyLists,
      Hash refLogId) {
    try (Traced ignore =
        trace("cleanUpCommitCas")
            .tag(TAG_COMMIT_COUNT, branchCommits.size())
            .tag(TAG_KEY_LIST_COUNT, newKeyLists.size())) {
      doCleanUpCommitCas(ctx, globalId, branchCommits, newKeyLists, refLogId);
    }
  }

  protected abstract void doCleanUpCommitCas(
      NonTransactionalOperationContext ctx,
      Hash globalId,
      Set<Hash> branchCommits,
      Set<Hash> newKeyLists,
      Hash refLogId);

  /**
   * Writes a global-state-log-entry without any operations, just to move the global-pointer
   * forwards for a "proper" CAS operation.
   */
  // TODO maybe replace with a 2nd-ary value in global-state-pointer to prevent the empty
  //  global-log-entry
  protected GlobalStateLogEntry noopGlobalLogEntry(
      NonTransactionalOperationContext ctx, GlobalStatePointer pointer)
      throws ReferenceConflictException {
    // Need a new empty global-log entry to be able to CAS
    return writeGlobalCommit(ctx, commitTimeInMicros(), pointer, Collections.emptyList());
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

    Hash initialId = Hash.of(pointer.getGlobalId());

    GlobalStateLogEntry initial = fetchFromGlobalLog(ctx, initialId);
    if (initial == null) {
      throw new RuntimeException(
          new ReferenceNotFoundException(
              String.format("Global log entry '%s' not does not exist.", initialId.asString())));
    }
    Spliterator<GlobalStateLogEntry> split;
    if (pointer.getGlobalParentsInclHeadCount() == 0
        || !pointer.getGlobalId().equals(pointer.getGlobalParentsInclHead(0))) {
      // Before Nessie 0.21.0

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

  @Override
  public Stream<RefLog> refLog(Hash offset) throws RefLogNotFoundException {
    return readRefLogStream(NON_TRANSACTIONAL_OPERATION_CONTEXT, offset);
  }
}
