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
package org.projectnessie.versioned.storage.common.objtypes;

import static org.projectnessie.versioned.storage.common.persist.ObjId.EMPTY_OBJ_ID;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import jakarta.annotation.Nullable;
import java.util.List;
import org.immutables.value.Value;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.common.config.StoreConfig;
import org.projectnessie.versioned.storage.common.indexes.ElementSerializer;
import org.projectnessie.versioned.storage.common.indexes.StoreIndexes;
import org.projectnessie.versioned.storage.common.indexes.StoreKey;
import org.projectnessie.versioned.storage.common.objtypes.CommitOp.Action;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;

/**
 * Describes a commit.
 *
 * <h2>Full and incremental indexes</h2>
 *
 * <p>The mapping of all reachable {@link StoreKey keys} to {@link Obj values} is maintained in two
 * indexes: the {@link CommitObj#referenceIndex() full} and {@link CommitObj#incrementalIndex()
 * incremental} indexes.
 *
 * <p>{@link CommitOp.Action} defines whether a key is present, modified in a previous commit or
 * modified in the current commit is necessary for the {@link CommitObj#referenceIndex() full} and
 * {@link CommitObj#incrementalIndex() incremental} indexes in {@link CommitObj}, because
 * incremental indexes contain the "diff" to the last full indexes, where full indexes shall only be
 * created or changed when it is really necessary, which means when the incremental index becomes
 * too big so that the remaining space in a database row requires the system to store the index
 * externally as a full index.
 *
 * <ul>
 *   <li>Keys added in the <em>current</em> commit are maintained in the {@link #incrementalIndex()
 *       incremental index} using {@link CommitOp.Action#ADD} and {@link CommitOp.Action#REMOVE}.
 *   <li>Keys added in a previous commit can be maintained in the {@link #incrementalIndex()
 *       incremental index} or in the {@link #referenceIndex() full index}.
 *   <li>If key modifications from previous commits are maintained in the in the {@link
 *       #incrementalIndex() incremental index}, the actions {@link CommitOp.Action#INCREMENTAL_ADD}
 *       and {@link CommitOp.Action#INCREMENTAL_REMOVE} are used.
 *   <li>When loading an incremental index <em>for modification</em>, the existing actions {@link
 *       CommitOp.Action#ADD} and {@link CommitOp.Action#REMOVE} must be updated to {@link
 *       CommitOp.Action#INCREMENTAL_ADD} and {@link CommitOp.Action#INCREMENTAL_REMOVE}. This can
 *       be done by using the {@code updater} function of {@link
 *       StoreIndexes#deserializeStoreIndex(ByteString, ElementSerializer)}
 *   <li>Key modifications in a {@link #referenceIndex() full index} do not need an action and use
 *       {@link CommitOp.Action#NONE} as a placeholder.
 *   <li>When key updates from an {@link #incrementalIndex() incremental index} are offloaded into
 *       the {@link #referenceIndex() full index}, {@link CommitOp.Action#INCREMENTAL_ADD}
 *       add/update elements in the full index and {@link CommitOp.Action#INCREMENTAL_REMOVE} remove
 *       elements in the full index.
 * </ul>
 *
 * See {@link org.projectnessie.versioned.storage.common.logic.IndexesLogic} and {@link
 * org.projectnessie.versioned.storage.common.logic.CommitLogic}.
 */
@Value.Immutable
public interface CommitObj extends Obj {

  @Override
  default ObjType type() {
    return StandardObjType.COMMIT;
  }

  static Builder commitBuilder() {
    return ImmutableCommitObj.builder();
  }

  interface Builder {
    @CanIgnoreReturnValue
    Builder from(CommitObj instance);

    @CanIgnoreReturnValue
    Builder id(@Nullable ObjId id);

    @CanIgnoreReturnValue
    Builder referenced(long referenced);

    @CanIgnoreReturnValue
    Builder created(long created);

    @CanIgnoreReturnValue
    Builder seq(long seq);

    @CanIgnoreReturnValue
    Builder addTail(ObjId element);

    @CanIgnoreReturnValue
    Builder tail(Iterable<? extends ObjId> elements);

    @CanIgnoreReturnValue
    Builder secondaryParents(Iterable<? extends ObjId> elements);

    @CanIgnoreReturnValue
    Builder addSecondaryParents(ObjId element);

    @CanIgnoreReturnValue
    Builder addAllSecondaryParents(Iterable<? extends ObjId> elements);

    @CanIgnoreReturnValue
    Builder headers(CommitHeaders headers);

    @CanIgnoreReturnValue
    Builder message(String message);

    @CanIgnoreReturnValue
    Builder referenceIndex(@Nullable ObjId referenceIndex);

    @CanIgnoreReturnValue
    Builder addAllReferenceIndexStripes(Iterable<? extends IndexStripe> referenceIndexStripes);

    @CanIgnoreReturnValue
    Builder referenceIndexStripes(Iterable<? extends IndexStripe> referenceIndexStripes);

    @CanIgnoreReturnValue
    Builder addReferenceIndexStripes(IndexStripe referenceIndexStripe);

    @CanIgnoreReturnValue
    Builder incrementalIndex(ByteString incrementalIndex);

    Builder incompleteIndex(boolean incompleteIndex);

    Builder commitType(CommitType commitType);

    CommitObj build();
  }

  /** Creation timestamp in microseconds since epoch. */
  @Value.Auxiliary
  long created();

  /**
   * Monotonically increasing counter representing the number of commits since the "beginning of
   * time".
   */
  long seq();

  /**
   * Zero, one or more parent-entry hashes of this commit, the nearest parent first.
   *
   * <p>This is an internal attribute used to more efficiently page through the commit log.
   *
   * <p>Only the first, the nearest parent, shall be exposed to clients.
   */
  List<ObjId> tail();

  default ObjId directParent() {
    List<ObjId> t = tail();
    return t.isEmpty() ? EMPTY_OBJ_ID : t.get(0);
  }

  /** Additional parent commits, for example the ID of a merged commit. */
  List<ObjId> secondaryParents();

  /** All headers and values. Headers are multi-valued. */
  CommitHeaders headers();

  /** The commit message as plain text. */
  String message();

  /**
   * Flag indicating that {@link #incrementalIndex()} (and {@link #referenceIndex()} contain only
   * the {@link CommitOp commit operations} of the current commit, omitting the cumulated operations
   * from previous commits. If the value of this attribute is {@code true}, a complete index
   * covering all {@link StoreKey keys} cannot be computed from the values of {@link
   * #incrementalIndex()} (and {@link #referenceIndex()} + {@link #referenceIndexStripes()}}.
   */
  @Value.Default
  default boolean incompleteIndex() {
    return false;
  }

  /**
   * Pointer to the reference {@link StoreKey}-to-{@link CommitOp} index for this commit.
   *
   * <p>This value, if not {@code null}, can point to {@link StandardObjType#INDEX_SEGMENTS}, in
   * which case the reference index is already big and organized in multiple segments, or to {@link
   * StandardObjType#INDEX} when the full index is small enough to fit into a single segment and
   * indirection is not necessary.
   *
   * <p>A {@code null} value means that the "embedded" {@link #incrementalIndex()} was never big
   * enough, a "reference index" does not exist and {@link #incrementalIndex()} contains everything.
   *
   * <p>An external {@link StandardObjType#INDEX_SEGMENTS} object will only be created, if the
   * number of stripes is higher than {@link StoreConfig#maxReferenceStripesPerCommit()}, otherwise
   * the stripes for the reference index will be stored {@link #referenceIndexStripes() inside} the
   * commit.
   *
   * @see #incrementalIndex()
   * @see #incompleteIndex()
   * @see #referenceIndexStripes()
   */
  @Nullable
  ObjId referenceIndex();

  /**
   * Pointers to the composite reference index stripes, an "embedded" version of {@link
   * StandardObjType#INDEX_SEGMENTS}. Commits that require to "externalize" index elements to a
   * reference index, which requires up to {@link StoreConfig#maxReferenceStripesPerCommit()} will
   * be kept here and not create another indirection via a {@link IndexSegmentsObj}.
   *
   * @see #incrementalIndex()
   * @see #incompleteIndex()
   * @see #referenceIndex()
   */
  List<IndexStripe> referenceIndexStripes();

  default boolean hasReferenceIndex() {
    return referenceIndex() != null || !referenceIndexStripes().isEmpty();
  }

  /**
   * Combined structure that contains the actual operations of this commit, those with {@link
   * Action} {@code !=} {@code CommitEntry.Operation.NONE}, plus the difference to the last {@link
   * #referenceIndex()} / {@link #referenceIndexStripes()}.
   *
   * <p>The benefit of having a combined structure holding both the incremental index update and the
   * commit operations is space efficiency. The overhead is that a new commit must reset the {@link
   * Action}s from the previous commit to {@link Action#NONE} before adding its own operations.
   *
   * <p><em>IMPORTANT</em> databases that have hard row size restrictions (e.g. DynamoDB's 400k
   * limit) must have a mechanism to store this incremental index in an adjacent row using a row-key
   * derived from {@link #id()}.
   *
   * @see #referenceIndex()
   * @see #incompleteIndex()
   * @see #referenceIndexStripes()
   */
  ByteString incrementalIndex();

  @Value.Default
  default CommitType commitType() {
    return CommitType.NORMAL;
  }
}
