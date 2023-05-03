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

import static java.util.Objects.requireNonNull;
import static org.projectnessie.versioned.storage.common.logic.CreateCommit.Add.commitAdd;
import static org.projectnessie.versioned.storage.common.logic.CreateCommit.Remove.commitRemove;
import static org.projectnessie.versioned.storage.common.logic.CreateCommit.newCommitBuilder;
import static org.projectnessie.versioned.storage.common.logic.Logics.commitLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.indexesLogic;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.fromCommitMeta;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.toCommitMeta;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Commit;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ImmutableMergeResult;
import org.projectnessie.versioned.MergeResult;
import org.projectnessie.versioned.MergeResult.KeyDetails;
import org.projectnessie.versioned.MetadataRewriter;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.storage.common.indexes.StoreIndex;
import org.projectnessie.versioned.storage.common.indexes.StoreIndexElement;
import org.projectnessie.versioned.storage.common.logic.CommitLogic;
import org.projectnessie.versioned.storage.common.logic.CommitRetry.RetryException;
import org.projectnessie.versioned.storage.common.logic.CreateCommit;
import org.projectnessie.versioned.storage.common.logic.IndexesLogic;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.objtypes.CommitOp;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;

class BaseMergeTransplantIndividual extends BaseCommitHelper {

  BaseMergeTransplantIndividual(
      @Nonnull @jakarta.annotation.Nonnull BranchName branch,
      @Nonnull @jakarta.annotation.Nonnull Optional<Hash> referenceHash,
      @Nonnull @jakarta.annotation.Nonnull Persist persist,
      @Nonnull @jakarta.annotation.Nonnull Reference reference,
      @Nullable @jakarta.annotation.Nullable CommitObj head)
      throws ReferenceNotFoundException {
    super(branch, referenceHash, persist, reference, head);
  }

  MergeResult<Commit> individualCommits(
      MetadataRewriter<CommitMeta> updateCommitMetadata,
      boolean dryRun,
      ImmutableMergeResult.Builder<Commit> mergeResult,
      MergeBehaviors mergeBehaviors,
      SourceCommitsAndParent sourceCommits)
      throws RetryException, ReferenceNotFoundException, ReferenceConflictException {
    IndexesLogic indexesLogic = indexesLogic(persist);
    StoreIndex<CommitOp> sourceParentIndex =
        indexesLogic.buildCompleteIndexOrEmpty(sourceCommits.sourceParent);
    StoreIndex<CommitOp> targetParentIndex = indexesLogic.buildCompleteIndexOrEmpty(head);

    CommitLogic commitLogic = commitLogic(persist);
    ObjId newHead = headId();
    boolean empty = true;
    Map<ContentKey, KeyDetails> keyDetailsMap = new HashMap<>();
    for (CommitObj sourceCommit : sourceCommits.sourceCommits) {
      CreateCommit createCommit =
          cloneCommit(updateCommitMetadata, sourceCommit, sourceParentIndex, newHead);

      verifyMergeTransplantCommitPolicies(targetParentIndex, sourceCommit);

      List<Obj> objsToStore = new ArrayList<>();
      CommitObj newCommit =
          createMergeTransplantCommit(
              mergeBehaviors, keyDetailsMap, createCommit, objsToStore::add);

      if (!indexesLogic.commitOperations(newCommit).iterator().hasNext()) {
        // No operations in this commit, skip it.
        continue;
      }

      empty = false;
      if (!dryRun) {
        commitLogic.storeCommit(newCommit, objsToStore);
        newHead = newCommit.id();
      }

      sourceParentIndex = indexesLogic.buildCompleteIndex(sourceCommit, Optional.empty());
      targetParentIndex = indexesLogic.buildCompleteIndex(newCommit, Optional.empty());
    }

    boolean hasConflicts = recordKeyDetailsAndCheckConflicts(mergeResult, keyDetailsMap);

    return finishMergeTransplant(empty, mergeResult, newHead, dryRun, hasConflicts);
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
    for (StoreIndexElement<CommitOp> el : indexesLogic.commitOperations(sourceCommit)) {
      StoreIndexElement<CommitOp> expected = sourceParentIndex.get(el.key());
      ObjId expectedId = null;
      if (expected != null) {
        CommitOp expectedContent = expected.content();
        if (expectedContent.action().exists()) {
          expectedId = expectedContent.value();
        }
      }

      CommitOp op = el.content();
      if (op.action().exists()) {
        createCommitBuilder.addAdds(
            commitAdd(
                el.key(), op.payload(), requireNonNull(op.value()), expectedId, op.contentId()));
      } else {
        createCommitBuilder.addRemoves(
            commitRemove(el.key(), op.payload(), requireNonNull(expectedId), op.contentId()));
      }
    }

    return createCommitBuilder.build();
  }
}
