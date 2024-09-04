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

import static org.projectnessie.versioned.storage.common.logic.CommitLogic.ValueReplacement.NO_VALUE_REPLACEMENT;
import static org.projectnessie.versioned.storage.common.logic.ConflictHandler.ConflictResolution.CONFLICT;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Consumer;
import org.projectnessie.versioned.storage.common.config.StoreConfig;
import org.projectnessie.versioned.storage.common.exceptions.CommitConflictException;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.indexes.StoreIndex;
import org.projectnessie.versioned.storage.common.indexes.StoreKey;
import org.projectnessie.versioned.storage.common.logic.ConflictHandler.ConflictResolution;
import org.projectnessie.versioned.storage.common.logic.CreateCommit.Add;
import org.projectnessie.versioned.storage.common.logic.CreateCommit.Builder;
import org.projectnessie.versioned.storage.common.logic.CreateCommit.Remove;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.objtypes.CommitOp;
import org.projectnessie.versioned.storage.common.objtypes.CommitType;
import org.projectnessie.versioned.storage.common.objtypes.ContentValueObj;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Reference;
import org.projectnessie.versioned.storage.common.persist.StoredObjResult;

/** Logic to read commits and perform commits including conflict checks. */
public interface CommitLogic {

  @Nonnull
  PagedResult<CommitObj, ObjId> commitLog(@Nonnull CommitLogQuery commitLogQuery);

  @Nonnull
  PagedResult<ObjId, ObjId> commitIdLog(@Nonnull CommitLogQuery commitLogQuery);

  @Nonnull
  DiffPagedResult<DiffEntry, StoreKey> diff(@Nonnull DiffQuery diffQuery);

  /**
   * Convenience method that combines {@link #buildCommitObj(CreateCommit, ConflictHandler,
   * CommitOpHandler, ValueReplacement, ValueReplacement)} and {@link #storeCommit(CommitObj,
   * List)}.
   *
   * @param createCommit parameters for {@link #buildCommitObj(CreateCommit, ConflictHandler,
   *     CommitOpHandler, ValueReplacement, ValueReplacement)}
   * @param additionalObjects additional {@link Obj}s to store, for example {@link ContentValueObj}
   * @return the non-{@code null} commit, if it was stored as a new record or already existed with
   *     the same values. Returns {@code null} if an object with the same ID already exists
   *     (collision).
   */
  @Nullable
  CommitObj doCommit(@Nonnull CreateCommit createCommit, @Nonnull List<Obj> additionalObjects)
      throws CommitConflictException, ObjNotFoundException;

  /**
   * Stores a new commit and handles storing the (external) {@link CommitObj#referenceIndex()
   * reference index}, when the {@link CommitObj#incrementalIndex() incremental index} becomes too
   * big.
   *
   * @param commit commit to store
   * @param additionalObjects additional {@link Obj}s to store, for example {@link ContentValueObj}
   * @return container holding the information whether the {@link CommitObj} was persisted as a new
   *     commit object, existed with the same values or was not persisted due to a commit-ID/hash
   *     collision.
   * @see #doCommit(CreateCommit, List)
   * @see #buildCommitObj(CreateCommit, ConflictHandler, CommitOpHandler, ValueReplacement,
   *     ValueReplacement)
   * @see #updateCommit(CommitObj)
   */
  @NotNull
  StoredObjResult<CommitObj> storeCommit(
      @Nonnull CommitObj commit, @Nonnull List<Obj> additionalObjects);

  /**
   * Updates an <em>existing</em> commit and handles storing the (external) {@link
   * CommitObj#referenceIndex() reference index}, when the {@link CommitObj#incrementalIndex()
   * incremental index} becomes too big.
   *
   * @param commit the commit to update
   * @return the persisted commit, containing the updated incremental and reference indexes
   */
  CommitObj updateCommit(@Nonnull CommitObj commit);

  /**
   * Adds a new commit on top of its parent commit, performing checks of the existing vs expected
   * contents of the {@link CreateCommit#adds() adds} and {@link CreateCommit#removes() removes}.
   *
   * <p>Similar to {@link #doCommit(CreateCommit, List)}, but does not persist the {@link CommitObj}
   * and allows conflict handling.
   *
   * <h3>{@link CommitObj#tail Parent tail}</h3>
   *
   * The {@link CreateCommit#parentCommitId() direct parent commit ID} is added as the first element
   * in the persisted {@link CommitObj#tail()}, with up to {@link StoreConfig#parentsPerCommit()
   * parentsPerCommit - 1} entries from the parent commit tail.
   *
   * <h3>Checks on each {@link Add Add} in {@link CreateCommit#adds()}</h3>
   *
   * <ol>
   *   <li>If {@link Add#expectedValue() expected value} is {@code null}:
   *       <ol>
   *         <li>The {@link Add#key() key} to add must not exist.
   *       </ol>
   *   <li>If {@link Add#expectedValue() expected value} is not {@code null}:
   *       <ol>
   *         <li>The {@link Add#key() key} to add must exist.
   *         <li>The {@link Add#payload()} must match the {@link CommitOp#payload()} in the {@link
   *             StoreIndex store-index} of the {@link CreateCommit#parentCommitId() parent commit}.
   *         <li>The {@link Add#expectedValue() expected value} must match the {@link
   *             CommitOp#value() value} in the {@link StoreIndex store-index} of the {@link
   *             CreateCommit#parentCommitId() parent commit}.
   *       </ol>
   * </ol>
   *
   * <h3>Checks on each {@link Remove Remove} in {@link CreateCommit#removes()}</h3>
   *
   * <ol>
   *   <li>The {@link Remove#key() key} to remove must exist.
   *   <li>The {@link Remove#expectedValue() expected value} of the key to remove must match the
   *       {@link CommitOp#value() value} in the {@link StoreIndex store-index} of the {@link
   *       CreateCommit#parentCommitId() parent commit}.
   *   <li>The {@link Remove#payload() payload} in the {@link Remove Remove} must match the {@link
   *       CommitOp#payload() payload} in the {@link StoreIndex store-index} of the {@link
   *       CreateCommit#parentCommitId() parent commit}.
   * </ol>
   *
   * <h3>Initial commit (parent equals "no ancestor hash")</h3>
   *
   * All checks and operations described above apply.
   *
   * @param createCommit Contains/describes the commit object to be committed.
   * @param conflictHandler Callback that decides how a particular {@link CommitConflict} shall be
   *     handled.
   *     <p>The callback can decide among the simple resolutions {@link ConflictResolution#CONFLICT}
   *     to propagate the conflict, {@link ConflictResolution#ADD} to commit the conflict and {@link
   *     ConflictResolution#DROP} to not commit the conflict.
   *     <p>Advanced conflict resolutions can be implemented via the {@code
   *     expectedValueReplacement} and {@code committedValueReplacement} callbacks, which are
   *     evaluated before conflict detection happens..
   * @param commitOpHandler Callback telling the value's {@link ObjId} for a {@link StoreKey} in the
   *     resulting commit.
   * @param expectedValueReplacement The commit logic identifies the current {@link ObjId} for
   *     {@link StoreKey} from the commit to commit against. If that value needs to be overridden,
   *     this callback can be used to let the commit logic use a different {@link ObjId}.
   *     <p>This is useful for (squashing) merge and transplant operations.
   * @param committedValueReplacement The commit logic (naturally) retrieves the new {@link ObjId}
   *     from the given {@link CreateCommit} object.
   *     <p>Since merge operations usually calculate the {@link CreateCommit} from a {@link
   *     #diffToCreateCommit(PagedResult, Builder) diff} operation, it is necessary to replace the
   *     committed {@link ObjId} for the committed value, when not the result of the diff but an
   *     externally resolved/created object shall be committed instead.
   *     <p>This is useful for (squashing) merge and transplant operations.
   * @see #doCommit(CreateCommit, List)
   * @see #storeCommit(CommitObj, List)
   */
  @Nonnull
  CommitObj buildCommitObj(
      @Nonnull CreateCommit createCommit,
      @Nonnull ConflictHandler conflictHandler,
      CommitOpHandler commitOpHandler,
      @Nonnull ValueReplacement expectedValueReplacement,
      @Nonnull ValueReplacement committedValueReplacement)
      throws CommitConflictException, ObjNotFoundException;

  @Nonnull
  default CommitObj buildCommitObj(@Nonnull CreateCommit createCommit)
      throws CommitConflictException, ObjNotFoundException {
    return buildCommitObj(
        createCommit, c -> CONFLICT, (k, v) -> {}, NO_VALUE_REPLACEMENT, NO_VALUE_REPLACEMENT);
  }

  @FunctionalInterface
  interface ValueReplacement {
    ValueReplacement NO_VALUE_REPLACEMENT = (add, key, id) -> id;

    @Nullable
    ObjId maybeReplaceValue(boolean add, StoreKey storeKey, ObjId currentId);
  }

  @Nonnull
  ObjId findCommonAncestor(@Nonnull ObjId targetId, @Nonnull ObjId sourceId)
      throws NoSuchElementException;

  @Nonnull
  ObjId findMergeBase(@Nonnull ObjId targetId, @Nonnull ObjId sourceId)
      throws NoSuchElementException;

  /** Retrieves the {@link CommitObj commit object} referenced by {@code commitId}. */
  @Nullable
  CommitObj fetchCommit(@Nonnull ObjId commitId) throws ObjNotFoundException;

  @Nonnull
  CommitObj[] fetchCommits(@Nonnull ObjId startCommitId, @Nonnull ObjId endCommitId)
      throws ObjNotFoundException;

  /**
   * Applies the changes between {@code base} and {@code mostRecent} to the commit builder.
   *
   * <p>Used to squash multiple commits and optionally, when using a different {@link
   * CreateCommit.Builder#parentCommitId(ObjId) parent commit}, provide the operations for a merge
   * commit.
   *
   * @return value of {@code createCommit}
   */
  @Nonnull
  CreateCommit.Builder diffToCreateCommit(
      @Nonnull PagedResult<DiffEntry, StoreKey> diff, @Nonnull CreateCommit.Builder createCommit);

  @Nullable
  CommitObj headCommit(@Nonnull Reference reference) throws ObjNotFoundException;

  /**
   * Identifies all heads and fork-points.
   *
   * <ul>
   *   <li>"Heads" are commits that are not referenced by other commits.
   *   <li>"Fork points" are commits that are the parent of more than one other commit. Knowing
   *       these commits can help to optimize the traversal of commit logs of multiple heads.
   * </ul>
   *
   * <p>{@link CommitType#INTERNAL internal commits} are excluded from this calculation.
   *
   * <p>It is possible that databases have to scan all rows/items in the tables/collections, which
   * can lead to a <em>very</em> long runtime of this method.
   *
   * @param expectedCommitCount it is recommended to tell the implementation the total number of
   *     commits in the Nessie repository
   * @param commitHandler called for every commit while scanning all commits
   */
  HeadsAndForkPoints identifyAllHeadsAndForkPoints(
      int expectedCommitCount, Consumer<CommitObj> commitHandler);
}
