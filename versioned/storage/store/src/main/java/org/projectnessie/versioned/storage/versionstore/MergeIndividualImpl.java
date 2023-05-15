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
import static org.projectnessie.versioned.storage.common.persist.ObjId.EMPTY_OBJ_ID;
import static org.projectnessie.versioned.storage.common.persist.ObjType.COMMIT;
import static org.projectnessie.versioned.storage.versionstore.RefMapping.referenceNotFound;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.hashToObjId;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.objIdToHash;

import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Commit;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ImmutableMergeResult;
import org.projectnessie.versioned.MergeResult;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.ResultType;
import org.projectnessie.versioned.VersionStore.MergeOp;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.logic.CommitRetry.RetryException;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;

final class MergeIndividualImpl extends BaseMergeTransplantIndividual implements Merge {

  MergeIndividualImpl(
      @Nonnull @jakarta.annotation.Nonnull BranchName branch,
      @Nonnull @jakarta.annotation.Nonnull Optional<Hash> referenceHash,
      @Nonnull @jakarta.annotation.Nonnull Persist persist,
      @Nonnull @jakarta.annotation.Nonnull Reference reference,
      @Nullable @jakarta.annotation.Nullable CommitObj head)
      throws ReferenceNotFoundException {
    super(branch, referenceHash, persist, reference, head);
  }

  @Override
  public MergeResult<Commit> merge(Optional<?> retryState, MergeOp mergeOp)
      throws ReferenceNotFoundException, RetryException, ReferenceConflictException {
    ObjId fromId = hashToObjId(mergeOp.fromHash());
    ObjId commonAncestorId = identifyMergeBase(fromId).id();

    CommitObj source;
    try {
      source = persist.fetchTypedObj(fromId, COMMIT, CommitObj.class);
    } catch (ObjNotFoundException e) {
      throw referenceNotFound(e);
    }

    ImmutableMergeResult.Builder<Commit> mergeResult =
        prepareMergeResult()
            .resultType(ResultType.MERGE)
            .sourceRef(mergeOp.fromRef())
            .commonAncestor(objIdToHash(commonAncestorId));

    // Fast-forward, if possible
    if (commonAncestorId.equals(headId())
        // TODO the following is a ROUGH port of the existing fast-forward restriction.
        //  Need to check whether the commit-metadata has changed as well.
        && source.directParent().equals(commonAncestorId)) {

      return mergeSquashFastForward(mergeOp, fromId, source, mergeResult);
    }

    SourceCommitsAndParent sourceCommits = loadSourceCommitsForMerge(fromId, commonAncestorId);

    checkArgument(
        !sourceCommits.sourceCommits.isEmpty(),
        "No hashes to merge from %s onto %s @ %s using common ancestor %s, expected commit ID from request was %s.",
        fromId,
        head != null ? head.id() : EMPTY_OBJ_ID,
        branch.getName(),
        commonAncestorId,
        referenceHash.map(Hash::asString).orElse("not specified"));

    return individualCommits(mergeOp, mergeResult, sourceCommits);
  }
}
