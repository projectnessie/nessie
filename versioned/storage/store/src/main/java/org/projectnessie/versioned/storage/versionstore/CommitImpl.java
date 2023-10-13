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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.newHashMapWithExpectedSize;
import static com.google.common.collect.Sets.newHashSetWithExpectedSize;
import static java.util.Objects.requireNonNull;
import static org.agrona.collections.Hashing.DEFAULT_LOAD_FACTOR;
import static org.projectnessie.versioned.CommitValidation.CommitOperation.commitOperation;
import static org.projectnessie.versioned.CommitValidation.CommitOperationType.CREATE;
import static org.projectnessie.versioned.CommitValidation.CommitOperationType.DELETE;
import static org.projectnessie.versioned.CommitValidation.CommitOperationType.UPDATE;
import static org.projectnessie.versioned.storage.common.indexes.StoreIndexes.lazyStoreIndex;
import static org.projectnessie.versioned.storage.common.logic.CreateCommit.Add.commitAdd;
import static org.projectnessie.versioned.storage.common.logic.CreateCommit.Remove.commitRemove;
import static org.projectnessie.versioned.storage.common.logic.CreateCommit.Unchanged.commitUnchanged;
import static org.projectnessie.versioned.storage.common.logic.CreateCommit.newCommitBuilder;
import static org.projectnessie.versioned.storage.common.logic.Logics.commitLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.indexesLogic;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.contentIdMaybe;
import static org.projectnessie.versioned.storage.common.persist.ObjId.EMPTY_OBJ_ID;
import static org.projectnessie.versioned.storage.versionstore.RefMapping.referenceConflictException;
import static org.projectnessie.versioned.storage.versionstore.RefMapping.referenceNotFound;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.fromCommitMeta;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.keyToStoreKey;
import static org.projectnessie.versioned.storage.versionstore.VersionStoreImpl.buildIdentifiedKey;
import static org.projectnessie.versioned.store.DefaultStoreWorker.contentTypeForPayload;
import static org.projectnessie.versioned.store.DefaultStoreWorker.payloadForContent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.ObjIntConsumer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.agrona.collections.Object2IntHashMap;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Commit;
import org.projectnessie.versioned.CommitResult;
import org.projectnessie.versioned.CommitValidation;
import org.projectnessie.versioned.Delete;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ImmutableCommitResult;
import org.projectnessie.versioned.ImmutableCommitValidation;
import org.projectnessie.versioned.Operation;
import org.projectnessie.versioned.Put;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.Unchanged;
import org.projectnessie.versioned.VersionStore.CommitValidator;
import org.projectnessie.versioned.VersionStoreException;
import org.projectnessie.versioned.storage.common.exceptions.CommitConflictException;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.indexes.StoreIndex;
import org.projectnessie.versioned.storage.common.indexes.StoreIndexElement;
import org.projectnessie.versioned.storage.common.indexes.StoreKey;
import org.projectnessie.versioned.storage.common.logic.CommitLogic;
import org.projectnessie.versioned.storage.common.logic.CommitRetry.RetryException;
import org.projectnessie.versioned.storage.common.logic.CreateCommit;
import org.projectnessie.versioned.storage.common.logic.IndexesLogic;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.objtypes.CommitOp;
import org.projectnessie.versioned.storage.common.objtypes.ContentValueObj;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;

class CommitImpl extends BaseCommitHelper {
  private final StoreIndex<CommitOp> headIndex;
  private final StoreIndex<CommitOp> expectedIndex;
  private final ContentMapping contentMapping;
  private final CommitLogic commitLogic;

  CommitImpl(
      @Nonnull @jakarta.annotation.Nonnull BranchName branch,
      @Nonnull @jakarta.annotation.Nonnull Optional<Hash> referenceHash,
      @Nonnull @jakarta.annotation.Nonnull Persist persist,
      @Nonnull @jakarta.annotation.Nonnull Reference reference,
      @Nullable @jakarta.annotation.Nullable CommitObj head)
      throws ReferenceNotFoundException {
    super(branch, referenceHash, persist, reference, head);
    commitLogic = commitLogic(persist);
    contentMapping = new ContentMapping(persist);
    this.headIndex =
        lazyStoreIndex(
            () -> {
              IndexesLogic indexesLogic = indexesLogic(persist);
              return indexesLogic.buildCompleteIndexOrEmpty(head);
            });
    this.expectedIndex =
        expected == head
            ? headIndex
            : lazyStoreIndex(
                () -> {
                  IndexesLogic indexesLogic = indexesLogic(persist);
                  return indexesLogic.buildCompleteIndexOrEmpty(expected);
                });
  }

  StoreIndex<CommitOp> headIndex() {
    return headIndex;
  }

  StoreIndex<CommitOp> expectedIndex() {
    return expectedIndex;
  }

  /**
   * Keeps state between commit retries to avoid duplicate {@link
   * org.projectnessie.versioned.storage.common.objtypes.ContentValueObj value objects} for new
   * contents, which would otherwise get a new content-id during every commit retry, therefore
   * pollute the database. Also keeps track of which objects have already been successfully stored
   * in the database, which speeds up retries, which do not need to persist the same content values
   * again.
   */
  static class CommitRetryState {
    final Set<ObjId> storedContents = new HashSet<>();
    final Map<ContentKey, String> generatedContentIds = new HashMap<>();
  }

  CommitResult<Commit> commit(
      @Nonnull @jakarta.annotation.Nonnull Optional<?> retryState,
      @Nonnull @jakarta.annotation.Nonnull CommitMeta metadata,
      @Nonnull @jakarta.annotation.Nonnull List<Operation> operations,
      @Nonnull @jakarta.annotation.Nonnull CommitValidator validator,
      @Nonnull @jakarta.annotation.Nonnull BiConsumer<ContentKey, String> addedContents)
      throws ReferenceNotFoundException,
          ReferenceConflictException,
          RetryException,
          ObjTooLargeException {
    CreateCommit.Builder commit = newCommitBuilder().parentCommitId(headId());
    List<Obj> objectsToStore = new ArrayList<>(operations.size() + 1);

    CommitRetryState commitRetryState =
        retryState.map(x -> (CommitRetryState) x).orElseGet(CommitRetryState::new);

    Consumer<Obj> valueConsumer =
        obj -> {
          if (commitRetryState.storedContents.add(obj.id())) {
            objectsToStore.add(obj);
          }
        };

    ImmutableCommitValidation.Builder commitValidation = CommitValidation.builder();

    try {
      commitAddOperations(operations, commit, valueConsumer, commitRetryState, commitValidation);
    } catch (ObjNotFoundException e) {
      throw new IllegalStateException("Content value objects not found", e);
    }

    try {
      validator.validate(commitValidation.build());
    } catch (BaseNessieClientServerException | VersionStoreException e) {
      throw new RuntimeException(e);
    }

    fromCommitMeta(metadata, commit);

    try {
      CommitObj newHead = commitLogic.doCommit(commit.build(), objectsToStore);

      checkState(
          newHead != null,
          "Hash collision detected, a commit with the same parent commit, commit message, "
              + "headers/commit-metadata and operations already exists");

      bumpReferencePointer(newHead.id(), Optional.of(commitRetryState));

      commitRetryState.generatedContentIds.forEach(addedContents);

      return ImmutableCommitResult.<Commit>builder()
          .commit(contentMapping.commitObjToCommit(true, newHead))
          .targetBranch((BranchName) RefMapping.referenceToNamedRef(reference))
          .build();

    } catch (CommitConflictException e) {
      throw referenceConflictException(e);
    } catch (ObjNotFoundException e) {
      throw referenceNotFound(e);
    }
  }

  void commitAddOperations(
      List<Operation> operations,
      CreateCommit.Builder commit,
      Consumer<Obj> contentToStore,
      CommitRetryState commitRetryState,
      ImmutableCommitValidation.Builder commitValidation)
      throws ObjNotFoundException, ReferenceConflictException {
    int num = operations.size();
    Map<ContentKey, Operation> allKeys = newHashMapWithExpectedSize(num);

    Set<StoreKey> storeKeysForHead =
        expectedIndex() != headIndex() ? newHashSetWithExpectedSize(operations.size()) : null;

    List<StoreKey> storeKeys = new ArrayList<>(num);
    for (int i = 0; i < num; i++) {
      Operation operation = operations.get(i);
      ContentKey key = operation.getKey();
      Operation previous = allKeys.put(key, operation);
      checkDuplicateKey(previous, operation);
      StoreKey storeKey = keyToStoreKey(key);
      storeKeys.add(storeKey);
      if (storeKeysForHead != null && operation instanceof Unchanged) {
        storeKeysForHead.add(storeKey);
      }
    }

    expectedIndex().loadIfNecessary(new HashSet<>(storeKeys));
    if (storeKeysForHead != null && !storeKeysForHead.isEmpty()) {
      headIndex().loadIfNecessary(storeKeysForHead);
    }

    Map<UUID, StoreKey> deleted = new HashMap<>();
    Map<ContentKey, Content> newContent = new HashMap<>();
    Object2IntHashMap<ContentKey> deletedKeysAndPayload =
        new Object2IntHashMap<>(operations.size() * 2, DEFAULT_LOAD_FACTOR, -1);
    // Must handle delete operations before put operations
    for (int i = 0; i < operations.size(); i++) {
      Operation operation = operations.get(i);
      StoreKey storeKey = storeKeys.get(i);

      if (operation instanceof Delete) {
        commitAddDelete(
            expectedIndex(),
            commit,
            (Delete) operation,
            storeKey,
            deleted,
            deletedKeysAndPayload::put,
            commitValidation);
      }
    }
    for (int i = 0; i < operations.size(); i++) {
      Operation operation = operations.get(i);
      StoreKey storeKey = storeKeys.get(i);

      if (operation instanceof Put) {
        commitAddPut(
            expectedIndex(),
            commit,
            (Put) operation,
            storeKey,
            contentToStore,
            commitRetryState,
            deleted,
            newContent,
            commitValidation);
      } else if (operation instanceof Delete) {
        // handled above
      } else if (operation instanceof Unchanged) {
        commitAddUnchanged(headIndex(), expectedIndex(), commit, storeKey);
      } else {
        throw new IllegalArgumentException(
            "Unknown operation type " + operation.getClass().getSimpleName());
      }
    }

    validateNamespaces(newContent, deletedKeysAndPayload, headIndex());
  }

  private static void checkDuplicateKey(Operation previous, Operation current) {
    if (previous != null) {
      boolean reAdd =
          previous instanceof Delete
              && current instanceof Put
              && ((Put) current).getValue().getId() == null;
      if (!reAdd) {
        throw new IllegalArgumentException(
            "Duplicate key in commit operations: " + current.getKey());
      }
    }
  }

  private static void commitAddUnchanged(
      StoreIndex<CommitOp> headIndex,
      StoreIndex<CommitOp> expectedIndex,
      CreateCommit.Builder commit,
      StoreKey storeKey) {
    // nothing to do, if head == expected
    if (headIndex != expectedIndex) {
      StoreIndexElement<CommitOp> expectedElement = expectedIndex.get(storeKey);

      int payload = 0;
      ObjId expectedValue = null;
      UUID expectedContentID = null;

      // TODO add much stricter handling of Delete against existing content, but that requires
      //  changes to the model
      // TODO validate content-ID in store-index against content-ID in operation

      if (expectedElement != null) {
        CommitOp content = expectedElement.content();
        if (content.action().exists()) {
          payload = content.payload();
          expectedValue = content.value();
          expectedContentID = content.contentId();
        }
      }

      if (expectedValue == null) {
        expectedValue = EMPTY_OBJ_ID;
      }

      commit.addUnchanged(commitUnchanged(storeKey, payload, expectedValue, expectedContentID));
    }
  }

  private void commitAddDelete(
      StoreIndex<CommitOp> expectedIndex,
      CreateCommit.Builder commit,
      Delete operation,
      StoreKey storeKey,
      Map<UUID, StoreKey> deleted,
      ObjIntConsumer<ContentKey> deletedKeys,
      ImmutableCommitValidation.Builder commitValidation) {
    StoreIndexElement<CommitOp> existingElement = expectedIndex.get(storeKey);

    int payload = 0;
    ObjId existingValue = null;
    UUID existingContentID = null;

    // TODO add much stricter handling of Delete against existing content, but that requires changes
    //  to the model
    // TODO require expectedContent for existing content
    // TODO validate content-ID in store-index against content-ID in operation

    if (existingElement != null) {
      CommitOp content = existingElement.content();
      if (content.action().exists()) {
        payload = content.payload();
        existingValue = content.value();
        existingContentID = content.contentId();
        deleted.put(existingContentID, storeKey);

        ContentKey contentKey = operation.getKey();

        deletedKeys.accept(contentKey, payload);

        commitValidation.addOperations(
            commitOperation(
                buildIdentifiedKey(
                    contentKey,
                    expectedIndex,
                    contentTypeForPayload(payload),
                    existingContentID != null ? existingContentID.toString() : null,
                    x -> null),
                DELETE));
      }
    }

    if (existingValue == null) {
      existingValue = EMPTY_OBJ_ID;
    }

    // TODO remove other existing variants beside TypeMapping.CONTENT_DISCRIMINATOR.
    //  Plan: shorten the storeKey (remove CONTENT_DISCRIMINATOR), iterate over the headIndex()
    //  and add removes for all the found keys.

    commit.addRemoves(commitRemove(storeKey, payload, existingValue, existingContentID));
  }

  private void commitAddPut(
      StoreIndex<CommitOp> expectedIndex,
      CreateCommit.Builder commit,
      Put put,
      StoreKey storeKey,
      Consumer<Obj> contentToStore,
      CommitRetryState commitRetryState,
      Map<UUID, StoreKey> deleted,
      Map<ContentKey, Content> newContent,
      ImmutableCommitValidation.Builder commitValidation)
      throws ObjNotFoundException {
    Content putValue = put.getValue();
    ContentKey putKey = put.getKey();
    String putValueId = putValue.getId();

    int payload = payloadForContent(putValue);
    ObjId existingValue = null;
    UUID existingContentID;
    String expectedContentIDString;

    StoreIndexElement<CommitOp> existing = expectedIndex.get(storeKey);

    StoreKey deletedKey = null;
    boolean storeKeyExists = existing != null && existing.content().action().exists();
    if (!storeKeyExists && putValueId != null) {
      // Check for a Delete-op in the same commit, representing a rename operation.
      UUID expectedContentID = UUID.fromString(putValueId);
      deletedKey = deleted.remove(expectedContentID);
    }
    if (storeKeyExists && putValueId == null && deleted.containsValue(storeKey)) {
      // Check for a Delete-op with same key in the same commit, representing a re-add operation.
      deletedKey = storeKey;
    }
    if (deletedKey != null) {
      existing = expectedIndex.get(deletedKey);
    }

    boolean exists = false;
    if (existing != null) {
      CommitOp content = existing.content();
      if (content.action().exists()) {
        payload = content.payload();
        existingValue = requireNonNull(content.value());
        existingContentID = content.contentId();
        expectedContentIDString =
            existingContentID != null
                ? existingContentID.toString()
                : contentIdFromContent(existingValue);

        if (putValueId == null) {

          // re-add case: the existing content is deleted and the new content is added in the same
          // commit
          checkArgument(
              deletedKey != null,
              "New value to update existing key '%s' has no content ID",
              putKey);

          existingValue = null;

        } else {

          checkArgument(
              expectedContentIDString.equals(putValueId),
              "Key '%s' already exists with content ID %s, which is different from "
                  + "the content ID %s in the operation",
              putKey,
              expectedContentIDString,
              putValueId);

          exists = true;
        }
      }
    }

    if (!exists) {
      checkArgument(
          putValueId == null, "New value for key '%s' must not have a content ID", putKey);

      putValueId =
          commitRetryState.generatedContentIds.computeIfAbsent(
              putKey, x -> UUID.randomUUID().toString());
      putValue = putValue.withId(putValueId);

      newContent.put(putKey, putValue);
    }

    checkState(
        putValueId != null, "INTERNAL: Must only persist a Content object with a content ID");

    ContentValueObj value = contentMapping.buildContent(putValue, payload);
    contentToStore.accept(value);
    ObjId valueId = requireNonNull(value.id());

    // Note: the content-ID from legacy, imported Nessie repositories could theoretically been
    // any string value. If it's a UUID, use it, otherwise ignore it down the road.
    UUID contentId = contentIdMaybe(putValueId);
    commit.addAdds(commitAdd(storeKey, payload, valueId, existingValue, contentId));

    commitValidation.addOperations(
        commitOperation(
            buildIdentifiedKey(
                putKey,
                expectedIndex,
                contentTypeForPayload(payload),
                contentId != null ? contentId.toString() : null,
                elements -> {
                  Content content = newContent.get(ContentKey.of(elements));
                  return content != null ? contentIdMaybe(content.getId()) : null;
                }),
            exists ? UPDATE : CREATE));
  }

  private String contentIdFromContent(@Nonnull @jakarta.annotation.Nonnull ObjId contentValueId)
      throws ObjNotFoundException {
    // Ideally this should pre-load all required objects, so they are bulk-loaded and in turn
    // available via the cache. But the probability of this function being called is effectively 0,
    // so it is not worth the effort.
    return contentMapping.fetchContent(contentValueId).getId();
  }
}
