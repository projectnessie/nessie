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

import static org.projectnessie.model.CommitMeta.fromMessage;
import static org.projectnessie.versioned.storage.common.logic.Logics.commitLogic;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.hashToObjId;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.objIdToHash;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.NoSuchElementException;
import java.util.Optional;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.MergeResult;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.VersionStore.MergeOp;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.logic.CommitLogic;
import org.projectnessie.versioned.storage.common.logic.CommitRetry.RetryException;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;

final class MergeSquashImpl extends BaseMergeTransplantSquash implements Merge {

  MergeSquashImpl(
      @Nonnull BranchName branch,
      @Nonnull Optional<Hash> referenceHash,
      @Nonnull Persist persist,
      @Nonnull Reference reference,
      @Nullable CommitObj head)
      throws ReferenceNotFoundException {
    super(branch, referenceHash, persist, reference, head);
  }

  @Override
  public MergeResult merge(Optional<?> retryState, MergeOp mergeOp)
      throws ReferenceNotFoundException, RetryException, ReferenceConflictException {
    ObjId fromId = hashToObjId(mergeOp.fromHash());
    ObjId commonAncestorId = identifyMergeBase(fromId);

    MergeTransplantContext mergeTransplantContext =
        loadSourceCommitsForMerge(fromId, commonAncestorId, mergeOp);

    MergeResult.Builder mergeResult =
        MergeResult.builder()
            .targetBranch(branch)
            .effectiveTargetHash(objIdToHash(headId()))
            .sourceRef(mergeOp.fromRef())
            .sourceHash(mergeOp.fromHash())
            .commonAncestor(objIdToHash(commonAncestorId));

    referenceHash.ifPresent(mergeResult::expectedHash);

    if (fromId.equals(commonAncestorId)) {
      return mergeResult
          .wasSuccessful(true)
          .wasApplied(false)
          .resultantTargetHash(objIdToHash(headId()))
          .build();
    }

    return squash(mergeOp, mergeResult, mergeTransplantContext, fromId);
  }

  private ObjId identifyMergeBase(ObjId fromId) throws ReferenceNotFoundException {
    CommitLogic commitLogic = commitLogic(persist);
    try {
      return commitLogic.findMergeBase(headId(), fromId);
    } catch (NoSuchElementException notFound) {
      throw new ReferenceNotFoundException(notFound.getMessage());
    }
  }

  private MergeTransplantContext loadSourceCommitsForMerge(
      @Nonnull ObjId startCommitId, @Nonnull ObjId endCommitId, @Nonnull MergeOp mergeOp) {
    CommitObj[] startEndCommits;
    try {
      startEndCommits = commitLogic(persist).fetchCommits(startCommitId, endCommitId);
    } catch (ObjNotFoundException e) {
      throw new RuntimeException(e);
    }

    CommitMeta metadata = mergeOp.updateCommitMetadata().rewriteSingle(fromMessage(""));

    return new MergeTransplantContext(startEndCommits[0], startEndCommits[1], metadata);
  }
}
