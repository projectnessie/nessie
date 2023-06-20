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
package org.projectnessie.versioned.storage.common.logic;

import static org.projectnessie.versioned.storage.common.indexes.StoreIndexes.emptyImmutableIndex;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.COMMIT_OP_SERIALIZER;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.indexes.StoreIndex;
import org.projectnessie.versioned.storage.common.indexes.StoreIndexElement;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.objtypes.CommitOp;
import org.projectnessie.versioned.storage.common.objtypes.IndexStripe;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;

public interface IndexesLogic {

  @Nonnull
  @jakarta.annotation.Nonnull
  Supplier<SuppliedCommitIndex> createIndexSupplier(
      @Nonnull @jakarta.annotation.Nonnull Supplier<ObjId> commitIdSupplier);

  @Nonnull
  @jakarta.annotation.Nonnull
  StoreIndex<CommitOp> buildCompleteIndex(
      @Nonnull @jakarta.annotation.Nonnull CommitObj commit,
      Optional<StoreIndex<CommitOp>> loadedIncrementalIndex);

  /**
   * Similar to {@link #buildCompleteIndex(CommitObj, Optional)}, but returns an empty and immutable
   * index for a {@code null} value for {@code commit}.
   */
  @Nonnull
  @jakarta.annotation.Nonnull
  default StoreIndex<CommitOp> buildCompleteIndexOrEmpty(
      @Nullable @jakarta.annotation.Nullable CommitObj commit) {
    return commit != null
        ? buildCompleteIndex(commit, Optional.empty())
        : emptyImmutableIndex(COMMIT_OP_SERIALIZER);
  }

  @Nullable
  @jakarta.annotation.Nullable
  StoreIndex<CommitOp> buildReferenceIndexOnly(
      @Nonnull @jakarta.annotation.Nonnull CommitObj commit);

  @Nonnull
  @jakarta.annotation.Nonnull
  StoreIndex<CommitOp> buildReferenceIndexOnly(
      @Nonnull @jakarta.annotation.Nonnull ObjId indexId,
      @Nonnull @jakarta.annotation.Nonnull ObjId commitId);

  @Nonnull
  @jakarta.annotation.Nonnull
  StoreIndex<CommitOp> incrementalIndexForUpdate(
      @Nonnull @jakarta.annotation.Nonnull CommitObj commit,
      Optional<StoreIndex<CommitOp>> loadedIncrementalIndex);

  @Nonnull
  @jakarta.annotation.Nonnull
  StoreIndex<CommitOp> incrementalIndexFromCommit(
      @Nonnull @jakarta.annotation.Nonnull CommitObj commit);

  @Nonnull
  @jakarta.annotation.Nonnull
  Iterable<StoreIndexElement<CommitOp>> commitOperations(
      @Nonnull @jakarta.annotation.Nonnull CommitObj commitObj);

  @Nonnull
  @jakarta.annotation.Nonnull
  Iterable<StoreIndexElement<CommitOp>> commitOperations(
      @Nonnull @jakarta.annotation.Nonnull StoreIndex<CommitOp> index);

  /**
   * Store the given striped index, also storing the nested stripes, if necessary.
   *
   * @param stripedIndex the index to store
   * @return non-{@code null} ID, even if the content value already exists
   * @throws ObjTooLargeException see {@link Persist#storeObj(Obj)}
   */
  @Nonnull
  @jakarta.annotation.Nonnull
  ObjId persistStripedIndex(@Nonnull @jakarta.annotation.Nonnull StoreIndex<CommitOp> stripedIndex)
      throws ObjTooLargeException;

  @Nonnull
  @jakarta.annotation.Nonnull
  List<IndexStripe> persistIndexStripesFromIndex(
      @Nonnull @jakarta.annotation.Nonnull StoreIndex<CommitOp> stripedIndex)
      throws ObjTooLargeException;

  /**
   * Updates, if necessary, all commits in the given commit and all its predecessors to contain
   * {@link CommitObj#incompleteIndex() complete indexes}.
   */
  void completeIndexesInCommitChain(
      @Nonnull @jakarta.annotation.Nonnull ObjId commitId, Runnable progressCallback)
      throws ObjNotFoundException;
}
