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

import static org.projectnessie.versioned.storage.common.logic.CreateCommit.newCommitBuilder;
import static org.projectnessie.versioned.storage.common.logic.DiffQuery.diffQuery;
import static org.projectnessie.versioned.storage.common.logic.Logics.commitLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.indexesLogic;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.fromCommitMeta;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.storeKeyToKey;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.MergeBehavior;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.MergeResult;
import org.projectnessie.versioned.MergeTransplantResultBase.KeyDetails;
import org.projectnessie.versioned.MetadataRewriter;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.VersionStore.MergeTransplantOpBase;
import org.projectnessie.versioned.storage.common.indexes.StoreIndex;
import org.projectnessie.versioned.storage.common.indexes.StoreKey;
import org.projectnessie.versioned.storage.common.logic.CommitLogic;
import org.projectnessie.versioned.storage.common.logic.CommitRetry.RetryException;
import org.projectnessie.versioned.storage.common.logic.CreateCommit;
import org.projectnessie.versioned.storage.common.logic.DiffEntry;
import org.projectnessie.versioned.storage.common.logic.IndexesLogic;
import org.projectnessie.versioned.storage.common.logic.PagedResult;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.objtypes.CommitOp;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;
import org.projectnessie.versioned.storage.common.persist.StoredObjResult;

class BaseMergeTransplantSquash extends BaseCommitHelper {

  BaseMergeTransplantSquash(
      @Nonnull BranchName branch,
      @Nonnull Optional<Hash> referenceHash,
      @Nonnull Persist persist,
      @Nonnull Reference reference,
      @Nullable CommitObj head)
      throws ReferenceNotFoundException {
    super(branch, referenceHash, persist, reference, head);
  }

  MergeResult squash(
      MergeTransplantOpBase mergeTransplantOpBase,
      MergeResult.Builder mergeResult,
      MergeTransplantContext mergeTransplantContext,
      @Nullable ObjId mergeFromId)
      throws RetryException, ReferenceNotFoundException, ReferenceConflictException {

    Map<ContentKey, KeyDetails> keyDetailsMap = new HashMap<>();

    MergeBehaviors mergeBehaviors = new MergeBehaviors(mergeTransplantOpBase);

    CreateCommit createCommit =
        createSquashCommit(
            mergeBehaviors,
            keyDetailsMap,
            mergeTransplantOpBase.updateCommitMetadata(),
            mergeTransplantContext,
            mergeFromId);

    List<Obj> objsToStore = new ArrayList<>();
    CommitObj mergeCommit =
        createMergeTransplantCommit(mergeBehaviors, keyDetailsMap, createCommit, objsToStore::add);

    // It's okay to do the fetchCommit() here and not complicate the surrounding logic (think:
    // local cache)
    StoreIndex<CommitOp> headIndex = indexesLogic(persist).buildCompleteIndexOrEmpty(head);

    validateMergeTransplantCommit(createCommit, mergeTransplantOpBase.validator(), headIndex);

    verifyMergeTransplantCommitPolicies(headIndex, mergeCommit);

    mergeBehaviors.postValidate();

    boolean hasConflicts = recordKeyDetailsAndCheckConflicts(mergeResult, keyDetailsMap);

    IndexesLogic indexesLogic = indexesLogic(persist);
    if (!indexesLogic.commitOperations(mergeCommit).iterator().hasNext()) {
      // The squashed commit is empty, i.e. it doesn't contain any operations: don't persist it.
      return finishMergeTransplant(
          true, mergeResult, headId(), mergeTransplantOpBase.dryRun(), hasConflicts);
    }

    ObjId newHead;
    if (mergeTransplantOpBase.dryRun() || hasConflicts) {
      newHead = headId();
    } else {
      CommitLogic commitLogic = commitLogic(persist);
      newHead = mergeCommit.id();
      StoredObjResult<CommitObj> committed = commitLogic.storeCommit(mergeCommit, objsToStore);
      if (committed.stored()) {
        mergeCommit = committed.obj().orElseThrow();
        mergeResult.addCreatedCommits(commitObjToCommit(mergeCommit));
      }
    }

    return finishMergeTransplant(
        false, mergeResult, newHead, mergeTransplantOpBase.dryRun(), hasConflicts);
  }

  private CreateCommit createSquashCommit(
      MergeBehaviors mergeBehaviors,
      Map<ContentKey, KeyDetails> keyDetailsMap,
      MetadataRewriter<CommitMeta> updateCommitMetadata,
      MergeTransplantContext mergeTransplantContext,
      @Nullable ObjId mergeFromId) {
    CreateCommit.Builder commitBuilder = newCommitBuilder().parentCommitId(headId());

    fromCommitMeta(
        updateCommitMetadata.squash(
            Collections.singletonList(mergeTransplantContext.metadata()),
            mergeTransplantContext.numCommits()),
        commitBuilder);

    if (mergeFromId != null) {
      commitBuilder.addSecondaryParents(mergeFromId);
    }

    Predicate<StoreKey> filter =
        storeKey -> {
          ContentKey key = storeKeyToKey(storeKey);
          MergeBehavior behavior = mergeBehaviors.mergeBehavior(key);
          if (behavior != MergeBehavior.DROP) {
            return true;
          }
          mergeBehaviors.useKey(false, key);
          keyDetailsMap.put(key, KeyDetails.keyDetails(MergeBehavior.DROP, null));
          return false;
        };

    CommitLogic commitLogic = commitLogic(persist);
    PagedResult<DiffEntry, StoreKey> diff =
        commitLogic.diff(
            diffQuery(
                mergeTransplantContext.baseCommit(),
                mergeTransplantContext.headCommit(),
                true,
                filter));

    return commitLogic.diffToCreateCommit(diff, commitBuilder).build();
  }
}
