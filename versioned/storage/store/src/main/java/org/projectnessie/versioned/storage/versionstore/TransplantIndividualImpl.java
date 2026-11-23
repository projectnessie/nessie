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
import static java.util.Objects.requireNonNull;
import static org.projectnessie.versioned.storage.common.logic.CreateCommit.Add.commitAdd;
import static org.projectnessie.versioned.storage.common.logic.CreateCommit.Remove.commitRemove;
import static org.projectnessie.versioned.storage.common.logic.CreateCommit.newCommitBuilder;
import static org.projectnessie.versioned.storage.common.logic.Logics.commitLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.indexesLogic;
import static org.projectnessie.versioned.storage.common.objtypes.StandardObjType.COMMIT;
import static org.projectnessie.versioned.storage.common.persist.ObjId.EMPTY_OBJ_ID;
import static org.projectnessie.versioned.storage.versionstore.RefMapping.referenceNotFound;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.fromCommitMeta;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.objIdToHash;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.storeKeyToKey;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.toCommitMeta;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.MergeBehavior;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.MergeResult;
import org.projectnessie.versioned.MetadataRewriter;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.TransplantResult;
import org.projectnessie.versioned.VersionStore.TransplantOp;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.indexes.StoreIndex;
import org.projectnessie.versioned.storage.common.indexes.StoreIndexElement;
import org.projectnessie.versioned.storage.common.indexes.StoreKey;
import org.projectnessie.versioned.storage.common.logic.CommitLogic;
import org.projectnessie.versioned.storage.common.logic.CommitRetry.RetryException;
import org.projectnessie.versioned.storage.common.logic.CreateCommit;
import org.projectnessie.versioned.storage.common.logic.ImmutableCreateCommit;
import org.projectnessie.versioned.storage.common.logic.IndexesLogic;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.objtypes.CommitOp;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;
import org.projectnessie.versioned.storage.common.persist.StoredObjResult;

final class TransplantIndividualImpl extends BaseCommitHelper implements Transplant {

  TransplantIndividualImpl(
      @Nonnull BranchName branch,
      @Nonnull Optional<Hash> referenceHash,
      @Nonnull Persist persist,
      @Nonnull Reference reference,
      @Nullable CommitObj head)
      throws ReferenceNotFoundException {
    super(branch, referenceHash, persist, reference, head);
  }

  @Override
  public TransplantResult transplant(Optional<?> retryState, TransplantOp transplantOp)
      throws ReferenceNotFoundException, RetryException, ReferenceConflictException {
    MergeTransplantContext mergeTransplantContext = loadSourceCommitsForTransplant(transplantOp);

    TransplantResult.Builder transplantResult =
        TransplantResult.builder()
            .targetBranch(branch)
            .effectiveTargetHash(objIdToHash(headId()))
            .sourceRef(transplantOp.fromRef());

    referenceHash.ifPresent(transplantResult::expectedHash);

    IndexesLogic indexesLogic = indexesLogic(persist);
    StoreIndex<CommitOp> sourceParentIndex =
        indexesLogic.buildCompleteIndexOrEmpty(mergeTransplantContext.baseCommit());
    StoreIndex<CommitOp> targetParentIndex = indexesLogic.buildCompleteIndexOrEmpty(head);

    MergeBehaviors mergeBehaviors = new MergeBehaviors(transplantOp);

    CommitLogic commitLogic = commitLogic(persist);
    ObjId newHead = headId();
    boolean empty = true;
    @SuppressWarnings("NonCanonicalType")
    Map<ContentKey, MergeResult.KeyDetails> keyDetailsMap = new HashMap<>();
    Map<UUID, StoreKey> retainedContentKeys = new HashMap<>();
    for (CommitObj sourceCommit : mergeTransplantContext.sourceCommits()) {
      CreateCommit createCommit =
          cloneCommit(
              transplantOp.updateCommitMetadata(), sourceCommit, sourceParentIndex, newHead);

      validateMergeTransplantCommit(createCommit, transplantOp.validator(), targetParentIndex);

      verifyMergeTransplantCommitPolicies(targetParentIndex, sourceCommit);

      createCommit =
          withoutCopiesOfRetainedContent(
              createCommit, targetParentIndex, mergeBehaviors, retainedContentKeys);
      for (CreateCommit.Remove remove : createCommit.removes()) {
        ContentKey key = storeKeyToKey(remove.key());
        UUID id = remove.contentId();
        if (key != null
            && id != null
            && mergeBehaviors.mergeBehavior(key) == MergeBehavior.DROP
            && hasContentId(targetParentIndex, remove.key(), id)) {
          retainedContentKeys.put(id, remove.key());
        }
      }

      List<Obj> objsToStore = new ArrayList<>();
      CommitObj newCommit =
          createMergeTransplantCommit(
              mergeBehaviors, keyDetailsMap, createCommit, objsToStore::add);

      // The next source commit follows this one even if the transplant skips all its operations.
      sourceParentIndex = indexesLogic.buildCompleteIndex(sourceCommit, Optional.empty());
      if (!indexesLogic.commitOperations(newCommit).iterator().hasNext()) {
        // No operations in this commit, skip it.
        continue;
      }

      empty = false;
      if (!transplantOp.dryRun()) {
        newHead = newCommit.id();
        StoredObjResult<CommitObj> committed = commitLogic.storeCommit(newCommit, objsToStore);
        // Here we have to know whether "our" 'newCommit' object has been persisted or not.
        // If not equal, we have to assume that the commit already existed - aka a "fast-forward
        // transplant". This is only to maintain compatibility with (pre-)existing behavior.
        // (This .equals has been introduced with https://github.com/projectnessie/nessie/pull/8533)
        if (committed.stored()) {
          newCommit = committed.obj().orElseThrow();
          transplantResult.addCreatedCommits(commitObjToCommit(newCommit));
        }
      }

      targetParentIndex = indexesLogic.buildCompleteIndex(newCommit, Optional.empty());
    }

    boolean hasConflicts = recordKeyDetailsAndCheckConflicts(transplantResult, keyDetailsMap);

    return finishMergeTransplant(
        empty, transplantResult, newHead, transplantOp.dryRun(), hasConflicts);
  }

  private static CreateCommit withoutCopiesOfRetainedContent(
      CreateCommit createCommit,
      StoreIndex<CommitOp> targetIndex,
      MergeBehaviors mergeBehaviors,
      Map<UUID, StoreKey> retainedContentKeys) {
    if (retainedContentKeys.isEmpty() || createCommit.adds().isEmpty()) {
      return createCommit;
    }

    Map<UUID, StoreKey> skip = new HashMap<>();
    for (CreateCommit.Add add : createCommit.adds()) {
      UUID id = add.contentId();
      ContentKey key = storeKeyToKey(add.key());
      StoreKey retainedKey = retainedContentKeys.get(id);
      if (id != null
          && key != null
          && retainedKey != null
          && !retainedKey.equals(add.key())
          && hasContentId(targetIndex, retainedKey, id)
          && (add.expectedValue() == null
              || mergeBehaviors.mergeBehavior(key) == MergeBehavior.FORCE)) {
        skip.put(id, add.key());
      }
    }

    // The commit logic checks both sides of a paired rename, including the destination conflict.
    for (CreateCommit.Remove remove : createCommit.removes()) {
      StoreKey destination = skip.get(remove.contentId());
      if (destination != null && !destination.equals(remove.key())) {
        skip.remove(remove.contentId());
      }
    }

    if (skip.isEmpty()) {
      return createCommit;
    }
    return ImmutableCreateCommit.copyOf(createCommit)
        .withAdds(
            createCommit.adds().stream().filter(add -> !skip.containsKey(add.contentId())).toList())
        .withRemoves(
            createCommit.removes().stream()
                .filter(remove -> !skip.containsKey(remove.contentId()))
                .toList());
  }

  private static boolean hasContentId(StoreIndex<CommitOp> index, StoreKey key, UUID id) {
    CommitOp existing = expectedContentAt(key, index);
    return existing != null && id.equals(existing.contentId());
  }

  private CreateCommit cloneCommit(
      MetadataRewriter<CommitMeta> updateCommitMetadata,
      CommitObj sourceCommit,
      StoreIndex<CommitOp> sourceParentIndex,
      ObjId newHead) {
    CreateCommit.Builder createCommitBuilder = newCommitBuilder().parentCommitId(newHead);

    CommitMeta commitMeta = toCommitMeta(sourceCommit);
    CommitMeta updatedMeta = updateCommitMetadata.rewriteSingle(commitMeta);
    fromCommitMeta(updatedMeta, createCommitBuilder);

    IndexesLogic indexesLogic = indexesLogic(persist);
    Set<UUID> addedContentIds = new HashSet<>();
    List<CreateCommit.Remove> replacedContent = new ArrayList<>();
    for (StoreIndexElement<CommitOp> el : indexesLogic.commitOperations(sourceCommit)) {
      CommitOp previous = expectedContentAt(el.key(), sourceParentIndex);
      ObjId expectedId = previous != null ? previous.value() : null;
      CommitOp op = el.content();
      if (op.action().exists()) {
        if (op.contentId() != null) {
          addedContentIds.add(op.contentId());
        }
        if (previous != null
            && expectedId != null
            && previous.contentId() != null
            && !previous.contentId().equals(op.contentId())) {
          replacedContent.add(
              commitRemove(el.key(), previous.payload(), expectedId, previous.contentId()));
        }
        createCommitBuilder.addAdds(
            commitAdd(
                el.key(), op.payload(), requireNonNull(op.value()), expectedId, op.contentId()));
      } else {
        createCommitBuilder.addRemoves(
            commitRemove(
                el.key(), op.payload(), requireNonNull(expectedId, "expectedId"), op.contentId()));
      }
    }

    // The source index stores the final ADD at a reused key. Recover its paired REMOVE when the
    // previous ID is put at another key by this same commit.
    for (CreateCommit.Remove remove : replacedContent) {
      if (addedContentIds.contains(remove.contentId())) {
        createCommitBuilder.addRemoves(remove);
      }
    }

    return createCommitBuilder.build();
  }

  MergeTransplantContext loadSourceCommitsForTransplant(TransplantOp transplantOp)
      throws ReferenceNotFoundException {
    List<Hash> commitHashes = transplantOp.sequenceToTransplant();

    checkArgument(
        !commitHashes.isEmpty(),
        "No hashes to transplant onto %s @ %s, expected commit ID from request was %s.",
        head != null ? head.id() : EMPTY_OBJ_ID,
        branch.getName(),
        referenceHash.map(Hash::asString).orElse("not specified"));

    CommitObj[] objs;
    try {
      objs =
          persist.fetchTypedObjs(
              commitHashes.stream().map(TypeMapping::hashToObjId).toArray(ObjId[]::new),
              COMMIT,
              CommitObj.class);
    } catch (ObjNotFoundException e) {
      throw referenceNotFound(e);
    }
    List<CommitObj> commits = new ArrayList<>(commitHashes.size());
    CommitObj parent = null;
    CommitLogic commitLogic = commitLogic(persist);
    for (int i = 0; i < objs.length; i++) {
      CommitObj commit = objs[i];
      if (i > 0) {
        if (!commit.directParent().equals(commits.get(i - 1).id())) {
          throw new IllegalArgumentException("Sequence of hashes is not contiguous.");
        }
      } else {
        try {
          parent = commitLogic.fetchCommit(commit.directParent());
        } catch (ObjNotFoundException e) {
          throw referenceNotFound(e);
        }
      }
      commits.add(commit);
    }

    List<CommitMeta> commitsMetadata = new ArrayList<>(commits.size());
    for (CommitObj sourceCommit : commits) {
      commitsMetadata.add(toCommitMeta(sourceCommit));
    }
    CommitMeta metadata =
        transplantOp.updateCommitMetadata().squash(commitsMetadata, commits.size());

    return new MergeTransplantContext(commits, parent, metadata);
  }
}
