/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.versioned.storage.versionstore;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.newHashMapWithExpectedSize;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.projectnessie.model.Conflict.conflict;
import static org.projectnessie.model.Content.Type.NAMESPACE;
import static org.projectnessie.versioned.CommitValidation.CommitOperation.commitOperation;
import static org.projectnessie.versioned.CommitValidation.CommitOperationType.CREATE;
import static org.projectnessie.versioned.CommitValidation.CommitOperationType.DELETE;
import static org.projectnessie.versioned.CommitValidation.CommitOperationType.UPDATE;
import static org.projectnessie.versioned.MergeTransplantResultBase.KeyDetails.keyDetails;
import static org.projectnessie.versioned.storage.common.logic.CommitConflict.ConflictType.KEY_EXISTS;
import static org.projectnessie.versioned.storage.common.logic.CommitRetry.commitRetry;
import static org.projectnessie.versioned.storage.common.logic.Logics.commitLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.indexesLogic;
import static org.projectnessie.versioned.storage.common.persist.ObjId.EMPTY_OBJ_ID;
import static org.projectnessie.versioned.storage.versionstore.RefMapping.commitConflictToConflict;
import static org.projectnessie.versioned.storage.versionstore.RefMapping.referenceConflictException;
import static org.projectnessie.versioned.storage.versionstore.RefMapping.referenceNotFound;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.hashToObjId;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.keyToStoreKey;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.objIdToHash;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.storeKeyToKey;
import static org.projectnessie.versioned.store.DefaultStoreWorker.contentTypeForPayload;
import static org.projectnessie.versioned.store.DefaultStoreWorker.payloadForContent;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.agrona.collections.Object2IntHashMap;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.model.Conflict;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IdentifiedContentKey;
import org.projectnessie.model.MergeBehavior;
import org.projectnessie.model.MergeKeyBehavior;
import org.projectnessie.model.Namespace;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Commit;
import org.projectnessie.versioned.CommitValidation;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ImmutableCommitValidation;
import org.projectnessie.versioned.MergeTransplantResultBase;
import org.projectnessie.versioned.MergeTransplantResultBase.KeyDetails;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.ReferenceRetryFailureException;
import org.projectnessie.versioned.VersionStore.CommitValidator;
import org.projectnessie.versioned.VersionStoreException;
import org.projectnessie.versioned.storage.batching.BatchingPersist;
import org.projectnessie.versioned.storage.batching.WriteBatching;
import org.projectnessie.versioned.storage.common.exceptions.CommitConflictException;
import org.projectnessie.versioned.storage.common.exceptions.CommitWrappedException;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.exceptions.RefConditionFailedException;
import org.projectnessie.versioned.storage.common.exceptions.RefNotFoundException;
import org.projectnessie.versioned.storage.common.exceptions.RetryTimeoutException;
import org.projectnessie.versioned.storage.common.exceptions.UnknownOperationResultException;
import org.projectnessie.versioned.storage.common.indexes.StoreIndex;
import org.projectnessie.versioned.storage.common.indexes.StoreIndexElement;
import org.projectnessie.versioned.storage.common.indexes.StoreKey;
import org.projectnessie.versioned.storage.common.logic.CommitLogic;
import org.projectnessie.versioned.storage.common.logic.CommitRetry.RetryException;
import org.projectnessie.versioned.storage.common.logic.ConflictHandler.ConflictResolution;
import org.projectnessie.versioned.storage.common.logic.CreateCommit;
import org.projectnessie.versioned.storage.common.logic.IndexesLogic;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.objtypes.CommitOp;
import org.projectnessie.versioned.storage.common.objtypes.ContentValueObj;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BaseCommitHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseCommitHelper.class);

  final Persist persist;

  final BranchName branch;
  final Optional<Hash> referenceHash;
  final Reference reference;
  final CommitObj head;
  final CommitObj expected;

  BaseCommitHelper(
      @Nonnull BranchName branch,
      @Nonnull Optional<Hash> referenceHash,
      @Nonnull Persist persist,
      @Nonnull Reference reference,
      @Nullable CommitObj head)
      throws ReferenceNotFoundException {
    this.branch = branch;
    this.referenceHash = referenceHash;
    this.persist = persist;
    this.reference = reference;
    this.head = head;

    // Need to perform this "expected commit" load here as a validation that 'referenceHash' points
    // to an existing commit (and also to produce a "proper" exception message downstream)
    CommitObj e = head;
    if (referenceHash.isPresent()) {
      ObjId referenceObjId = hashToObjId(referenceHash.get());
      if (!referenceObjId.equals(headId())) {
        RefMapping refMapping = new RefMapping(persist);
        e = refMapping.commitInChain(branch, head, referenceHash, emptyList());
      }
    }
    this.expected = e;
  }

  ObjId headId() {
    return head != null ? head.id() : EMPTY_OBJ_ID;
  }

  @FunctionalInterface
  interface CommitterSupplier<I> {
    I newCommitter(
        @Nonnull BranchName branch,
        @Nonnull Optional<Hash> referenceHash,
        @Nonnull Persist persist,
        @Nonnull Reference reference,
        @Nullable CommitObj head)
        throws ReferenceNotFoundException;
  }

  /**
   * Wraps a {@link CommitterSupplier} into another {@link CommitterSupplier} that operates in
   * "dry-run" mode, i.e. it will not actually persist any changes.
   */
  public static <I> CommitterSupplier<I> dryRunCommitterSupplier(CommitterSupplier<I> supplier) {
    return (b, hash, p, ref, head) -> supplier.newCommitter(b, hash, dryRunPersist(p), ref, head);
  }

  /**
   * Wraps a {@link Persist} into another {@link Persist} that operates in "dry-run" mode, i.e. it
   * will not actually persist any changes.
   *
   * @implSpec The returned {@link Persist} is a {@link BatchingPersist} with a batch size of -1
   *     that never flushes, which effectively disables writes to the store.
   */
  public static Persist dryRunPersist(Persist persist) {
    return WriteBatching.builder().persist(persist).batchSize(-1).build().create();
  }

  @FunctionalInterface
  interface CommittingFunction<R, I> {

    /**
     * Performs a committing operation attempt.
     *
     * @param impl implementation performing the committing operation
     * @param retryState The initial call to this function will receive an empty value, subsequent
     *     calls receive the parameter passed to the previous {@link RetryException}.
     */
    R perform(I impl, Optional<?> retryState)
        throws ReferenceNotFoundException,
            ReferenceConflictException,
            RetryException,
            ObjTooLargeException;
  }

  static <R, I> R committingOperation(
      @Nonnull String operationName,
      @Nonnull BranchName branch,
      @Nonnull Optional<Hash> referenceHash,
      @Nonnull Persist persist,
      @Nonnull CommitterSupplier<I> committerSupplier,
      @Nonnull CommittingFunction<R, I> committingFunction)
      throws ReferenceConflictException, ReferenceNotFoundException {
    try {
      return commitRetry(
          persist,
          (p, retryState) -> {
            RefMapping refMapping = new RefMapping(p);
            Reference reference;
            try {
              reference = refMapping.resolveNamedRefForUpdate(branch);
            } catch (ReferenceNotFoundException e) {
              throw new CommitWrappedException(e);
            }

            try {
              CommitObj head = commitLogic(p).headCommit(reference);
              I committer =
                  committerSupplier.newCommitter(branch, referenceHash, p, reference, head);
              return committingFunction.perform(committer, retryState);
            } catch (ReferenceConflictException
                | ReferenceNotFoundException
                | ObjNotFoundException
                | ObjTooLargeException e) {
              throw new CommitWrappedException(e);
            }
          });
    } catch (CommitConflictException e) {
      throw referenceConflictException(e);
    } catch (CommitWrappedException e) {
      Throwable c = e.getCause();
      if (c instanceof ReferenceNotFoundException) {
        throw (ReferenceNotFoundException) c;
      }
      if (c instanceof ReferenceConflictException) {
        throw (ReferenceConflictException) c;
      }
      if (c instanceof RuntimeException) {
        throw (RuntimeException) c;
      }
      throw new RuntimeException(c);
    } catch (RetryTimeoutException e) {
      long millis = NANOSECONDS.toMillis(e.getTimeNanos());
      String msg =
          format(
              "The %s operation could not be performed after %d retries within the configured commit timeout after %d milliseconds",
              operationName, e.getRetry(), millis);
      LOGGER.warn("Operation timeout: {}", msg);
      throw new ReferenceRetryFailureException(msg, e.getRetry(), millis);
    }
  }

  void validateNamespaces(
      Map<ContentKey, Content> newContent,
      Object2IntHashMap<ContentKey> allKeysToDelete,
      StoreIndex<CommitOp> headIndex)
      throws ReferenceConflictException {
    Set<Conflict> conflicts = new LinkedHashSet<>();

    validateNamespacesExistForContentKeys(newContent, headIndex, conflicts::add);
    validateNamespacesToDeleteHaveNoChildren(allKeysToDelete, headIndex, conflicts::add);

    if (!conflicts.isEmpty()) {
      throw new ReferenceConflictException(conflicts);
    }
  }

  private void validateNamespacesToDeleteHaveNoChildren(
      Object2IntHashMap<ContentKey> allKeysToDelete,
      StoreIndex<CommitOp> headIndex,
      Consumer<Conflict> conflictConsumer) {
    @SuppressWarnings("removal")
    boolean validateNamespaces = persist.config().validateNamespaces();
    if (!validateNamespaces) {
      return;
    }

    int payloadNamespace = payloadForContent(NAMESPACE);

    allKeysToDelete.forEach(
        (namespaceKey, payload) -> {
          if (payloadNamespace != payload) {
            return;
          }

          StoreKey storeKey = keyToStoreKey(namespaceKey);

          for (Iterator<StoreIndexElement<CommitOp>> iter =
                  headIndex.iterator(storeKey, null, false);
              iter.hasNext(); ) {
            StoreIndexElement<CommitOp> el = iter.next();
            if (!el.content().action().exists()) {
              // element does not exist - ignore
              continue;
            }
            ContentKey elContentKey = storeKeyToKey(el.key());
            if (elContentKey == null) {
              // not a "content object" - ignore
              continue;
            }
            if (elContentKey.equals(namespaceKey)) {
              // this is our namespace - ignore
              continue;
            }
            if (allKeysToDelete.containsKey(elContentKey)) {
              // this key is being deleted as well - ignore
              continue;
            }

            int elLen = elContentKey.getElementCount();
            int nsLen = namespaceKey.getElementCount();

            // check if element is in the current namespace, fail it is true - this means,
            // there is a live content-key in the current namespace - must not delete the
            // namespace
            ContentKey truncatedElContentKey = elContentKey.truncateToLength(nsLen);
            int cmp = truncatedElContentKey.compareTo(namespaceKey);
            if (!(elLen < nsLen || cmp != 0)) {
              conflictConsumer.accept(
                  conflict(
                      Conflict.ConflictType.NAMESPACE_NOT_EMPTY,
                      namespaceKey,
                      format("namespace '%s' is not empty", namespaceKey)));
            }
            if (cmp > 0) {
              // iterated past the namespaceKey - break
              break;
            }
          }
        });
  }

  private void validateNamespacesExistForContentKeys(
      Map<ContentKey, Content> newContent,
      StoreIndex<CommitOp> headIndex,
      Consumer<Conflict> conflictConsumer) {
    @SuppressWarnings("removal")
    boolean validateNamespaces = persist.config().validateNamespaces();
    if (!validateNamespaces) {
      return;
    }
    if (newContent.isEmpty()) {
      return;
    }

    Set<ContentKey> namespaceKeys =
        newContent.keySet().stream()
            .filter(k -> k.getElementCount() > 1)
            .map(ContentKey::getParent)
            .collect(Collectors.toSet());

    for (ContentKey key : namespaceKeys) {
      Content namespaceAddedInThisCommit = newContent.get(key);
      if (namespaceAddedInThisCommit instanceof Namespace) {
        // Namespace for the current new-content-key has been added via the currently validated
        // commit. Nothing to do for `Namespace`s here.
        continue;
      }

      StoreIndexElement<CommitOp> ns = headIndex.get(keyToStoreKey(key));
      for (;
          ns == null || !ns.content().action().exists();
          key = key.getParent(), ns = headIndex.get(keyToStoreKey(key))) {
        conflictConsumer.accept(
            conflict(
                Conflict.ConflictType.NAMESPACE_ABSENT,
                key,
                format("namespace '%s' must exist", key)));
        if (key.getElementCount() == 1) {
          break;
        }
      }
      if (ns != null && ns.content().action().exists()) {
        CommitOp nsContent = ns.content();
        if (nsContent.payload() != payloadForContent(Content.Type.NAMESPACE)) {
          conflictConsumer.accept(
              conflict(
                  Conflict.ConflictType.NOT_A_NAMESPACE,
                  key,
                  format(
                      "expecting the key '%s' to be a namespace, but is not a namespace ("
                          + "using a content object that is not a namespace as a namespace is forbidden)",
                      key)));
        }
      }
    }
  }

  static ObjId idForExpectedContent(StoreKey key, StoreIndex<CommitOp> headIndex) {
    StoreIndexElement<CommitOp> expected = headIndex.get(key);
    if (expected != null) {
      CommitOp expectedContent = expected.content();
      if (expectedContent.action().exists()) {
        return expectedContent.value();
      }
    }
    return null;
  }

  void verifyMergeTransplantCommitPolicies(
      StoreIndex<CommitOp> headIndex, CommitObj inspectedCommit) throws ReferenceConflictException {

    Map<ContentKey, Content> checkContents = new HashMap<>();
    Object2IntHashMap<ContentKey> deletedKeysAndPayload = new Object2IntHashMap<>(-1);

    IndexesLogic indexesLogic = indexesLogic(persist);
    for (StoreIndexElement<CommitOp> el : indexesLogic.commitOperations(inspectedCommit)) {
      ObjId expectedId = idForExpectedContent(el.key(), headIndex);
      CommitOp op = el.content();
      if (op.action().exists()) {
        ObjId value = requireNonNull(op.value());

        if (expectedId == null) {
          ContentKey contentKey = storeKeyToKey(el.key());
          // 'contentKey' will be 'null', if the store-key is not in the MAIN_UNIVERSE or the
          // variant is not CONTENT_DISCRIMINATOR.
          checkState(
              contentKey != null,
              "Merge/transplant with non-content-object store-keys is not implemented.");

          try {
            Content content = new ContentMapping(persist).fetchContent(value);
            checkContents.put(contentKey, content);
          } catch (ObjNotFoundException e) {
            throw new RuntimeException(e);
          }
        }
      } else {
        ContentKey contentKey = storeKeyToKey(el.key());
        // 'contentKey' will be 'null', if the store-key is not in the MAIN_UNIVERSE or the
        // variant is not CONTENT_DISCRIMINATOR.
        checkState(
            contentKey != null,
            "Merge/transplant with non-content-object store-keys is not implemented.");
        deletedKeysAndPayload.put(contentKey, op.payload());
      }
    }

    validateNamespaces(checkContents, deletedKeysAndPayload, headIndex);
  }

  CommitObj createMergeTransplantCommit(
      MergeBehaviors mergeBehaviors,
      Map<ContentKey, KeyDetails> keyDetailsMap,
      CreateCommit createCommit,
      Consumer<Obj> objsToStore)
      throws ReferenceNotFoundException {

    try {
      CommitLogic commitLogic = commitLogic(persist);
      ContentMapping contentMapping = new ContentMapping(persist);
      return commitLogic.buildCommitObj(
          createCommit,
          /*
           * Conflict handling happens via this callback, which can decide how the conflict should
           * be handled.
           */
          conflict -> {
            ContentKey key = storeKeyToKey(conflict.key());
            // Note: key==null, if not the "main universe" or not a "content"
            // discriminator
            if (key != null) {
              if (conflict.conflictType() == KEY_EXISTS) {
                // This is rather a hack to ignore conflicts when merging namespaces. If both the
                // source and target payload is NAMESPACE, let the target content "win" (aka no
                // change).
                CommitOp op = conflict.op();
                CommitOp ex = conflict.existing();
                if (op != null && ex != null && op.payload() == ex.payload()) {
                  if (Objects.equals(op.value(), ex.value())) {
                    // Got another add for the exact same content that is already on the target, so
                    // drop the conflicting operation on the floor.
                    return ConflictResolution.DROP;
                  }
                  if (contentTypeForPayload(op.payload()) == NAMESPACE) {
                    return ConflictResolution.ADD;
                  }
                }
              }

              MergeBehavior mergeBehavior = mergeBehaviors.mergeBehavior(key);
              switch (mergeBehavior) {
                case FORCE:
                case DROP:
                  MergeKeyBehavior mergeKeyBe = mergeBehaviors.useKey(false, key);
                  // Do not plain ignore (due to FORCE) or drop (DROP), when the caller provided an
                  // expectedTargetContent.
                  if (mergeKeyBe.getExpectedTargetContent() == null) {
                    keyDetailsMap.put(key, keyDetails(mergeBehavior, null));
                    return mergeBehavior == MergeBehavior.FORCE
                        ? ConflictResolution.ADD
                        : ConflictResolution.DROP;
                  }
                // fall through
                case NORMAL:
                  keyDetailsMap.put(
                      key, keyDetails(mergeBehavior, commitConflictToConflict(conflict)));
                  return ConflictResolution.ADD;
                default:
                  throw new IllegalStateException("Unknown merge behavior " + mergeBehavior);
              }
            }
            return ConflictResolution.ADD;
          },
          /*
           * Callback from the commit-logic telling us the value-ObjId for a key.
           */
          (storeKey, valueId) -> {
            ContentKey key = storeKeyToKey(storeKey);
            // Note: key==null, if not the "main universe" or not a "content" discriminator
            if (key != null) {
              keyDetailsMap.putIfAbsent(key, keyDetails(mergeBehaviors.mergeBehavior(key), null));
            }
          },
          /*
           * Following callback implements the functionality to handle MergeKeyBehavior.expectedTargetContent,
           * to explicitly validate the current value on the target branch/commit.
           *
           * This is mandatory, if MergeKeyBehavior.resolvedContent != null.
           */
          (add, storeKey, expectedValueId) -> {
            // "replace" the ObjId expected for a commit-ADD action
            ContentKey key = storeKeyToKey(storeKey);
            // Note: key==null, if not the "main universe" or not a "content" discriminator
            if (key == null) {
              return expectedValueId;
            }
            Content expectedTarget = mergeBehaviors.useKey(add, key).getExpectedTargetContent();
            // If there is an expected-target-content, we only need the ObjId for it to let the
            // commit code perform the check. An object load is not needed.
            return expectedTarget != null
                ? contentMapping
                    .buildContent(expectedTarget, payloadForContent(expectedTarget))
                    .id()
                : expectedValueId;
          },
          /*
           * Following callback implements the functionality to handle MergeKeyBehavior.resolvedContent,
           * so when a (squashing) merge-operation requests to explicitly use merge a different content
           * object, to handle externally resolved conflicts.
           *
           * Non-squashing merges are prohibited to have a non-null MergeKeyBehavior.resolvedContent.
           */
          (add, storeKey, commitValueId) -> {
            ContentKey key = storeKeyToKey(storeKey);
            // Note: key==null, if not the "main universe" or not a "content" discriminator
            if (key == null) {
              return commitValueId;
            }
            MergeKeyBehavior mergeKeyBehavior = mergeBehaviors.useKey(add, key);
            Content resolvedContent = mergeKeyBehavior.getResolvedContent();
            if (resolvedContent == null) {
              // Nothing to resolve, use the value from the source.
              return commitValueId;
            }

            // Build the "resolved" content value object and add it to the objects to persist.
            ContentValueObj resolvedValue =
                contentMapping.buildContent(resolvedContent, payloadForContent(resolvedContent));
            objsToStore.accept(resolvedValue);
            return resolvedValue.id();
          });
    } catch (CommitConflictException conflict) {
      // Data conflicts are handled, if we get here, it's an internal error OR unimplemented
      // feature or data condition.
      throw new IllegalStateException("Unhandled conflict", conflict);
    } catch (ObjNotFoundException e) {
      throw referenceNotFound(e);
    }
  }

  void validateMergeTransplantCommit(
      CreateCommit createCommit, CommitValidator commitValidator, StoreIndex<CommitOp> index) {
    ImmutableCommitValidation.Builder commitValidation = CommitValidation.builder();
    for (CreateCommit.Remove remove : createCommit.removes()) {
      ContentKey key = storeKeyToKey(remove.key());
      if (key == null) {
        continue;
      }
      IdentifiedContentKey identifiedKey =
          VersionStoreImpl.buildIdentifiedKey(
              key, index, remove.payload(), remove.contentId(), x -> null);
      commitValidation.addOperations(commitOperation(identifiedKey, DELETE));
    }

    Map<ContentKey, UUID> seenContentIds = newHashMapWithExpectedSize(createCommit.adds().size());
    for (CreateCommit.Add add : createCommit.adds()) {
      ContentKey key = storeKeyToKey(add.key());
      if (key == null) {
        continue;
      }
      boolean exists = index.contains(add.key());
      seenContentIds.put(key, add.contentId());
      IdentifiedContentKey identifiedKey =
          VersionStoreImpl.buildIdentifiedKey(
              key,
              index,
              add.payload(),
              add.contentId(),
              elements -> seenContentIds.get(ContentKey.of(elements)));
      commitValidation.addOperations(commitOperation(identifiedKey, exists ? UPDATE : CREATE));
    }

    try {
      commitValidator.validate(commitValidation.build());
    } catch (BaseNessieClientServerException | VersionStoreException e) {
      throw new RuntimeException(e);
    }
  }

  void bumpReferencePointer(ObjId newHead, Optional<?> retryState) throws RetryException {
    try {
      persist.updateReferencePointer(reference, newHead);
    } catch (UnknownOperationResultException e) {
      // If the above pointer-bump returned an "unknown result", we check once (and only once!)
      // whether the reference-pointer-change succeeded. This mitigation may not always work,
      // especially not in highly concurrent update situations.
      Reference r = persist.fetchReferenceForUpdate(reference.name());
      if (!reference.forNewPointer(newHead, persist.config()).equals(r)) {
        throw new RetryException(retryState);
      }
    } catch (RefConditionFailedException e) {
      throw new RetryException(retryState);
    } catch (RefNotFoundException e) {
      throw new RuntimeException("Internal reference not found", e);
    }
  }

  <R extends MergeTransplantResultBase, B extends MergeTransplantResultBase.Builder<R, B>>
      boolean recordKeyDetailsAndCheckConflicts(
          B mergeResult, Map<ContentKey, KeyDetails> keyDetailsMap) {
    boolean hasConflicts = false;
    for (Entry<ContentKey, KeyDetails> keyDetail : keyDetailsMap.entrySet()) {
      KeyDetails details = keyDetail.getValue();
      hasConflicts |= details.getConflict() != null;
      mergeResult.putDetails(keyDetail.getKey(), details);
    }
    return hasConflicts;
  }

  <R extends MergeTransplantResultBase, B extends MergeTransplantResultBase.Builder<R, B>>
      R finishMergeTransplant(
          boolean isEmpty, B mergeResult, ObjId newHead, boolean dryRun, boolean hasConflicts)
          throws RetryException {

    if (!hasConflicts) {
      mergeResult.wasSuccessful(true);
    }

    if (dryRun || hasConflicts) {
      return mergeResult.build();
    }

    mergeResult.resultantTargetHash(objIdToHash(newHead));

    if (!isEmpty) {
      bumpReferencePointer(newHead, Optional.empty());
      mergeResult.wasApplied(true);
    }

    return mergeResult.build();
  }

  Commit commitObjToCommit(CommitObj newCommit) {
    try {
      ContentMapping contentMapping = new ContentMapping(persist);
      return contentMapping.commitObjToCommit(true, newCommit);
    } catch (ObjNotFoundException e) {
      // This should never happen, since we just created the commit object;
      // if it does, it's a pretty serious state corruption.
      throw new RuntimeException(e);
    }
  }
}
