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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Maps.newHashMapWithExpectedSize;
import static com.google.common.collect.Sets.newHashSetWithExpectedSize;
import static java.util.Collections.emptyIterator;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static org.projectnessie.nessie.relocated.protobuf.ByteString.copyFromUtf8;
import static org.projectnessie.versioned.storage.common.indexes.StoreIndexElement.indexElement;
import static org.projectnessie.versioned.storage.common.indexes.StoreIndexes.indexFromStripes;
import static org.projectnessie.versioned.storage.common.indexes.StoreIndexes.newStoreIndex;
import static org.projectnessie.versioned.storage.common.indexes.StoreKey.keyFromString;
import static org.projectnessie.versioned.storage.common.logic.CommitConflict.ConflictType.CONTENT_ID_DIFFERS;
import static org.projectnessie.versioned.storage.common.logic.CommitConflict.ConflictType.KEY_DOES_NOT_EXIST;
import static org.projectnessie.versioned.storage.common.logic.CommitConflict.ConflictType.KEY_EXISTS;
import static org.projectnessie.versioned.storage.common.logic.CommitConflict.ConflictType.PAYLOAD_DIFFERS;
import static org.projectnessie.versioned.storage.common.logic.CommitConflict.ConflictType.VALUE_DIFFERS;
import static org.projectnessie.versioned.storage.common.logic.CommitConflict.commitConflict;
import static org.projectnessie.versioned.storage.common.logic.CreateCommit.Add.commitAdd;
import static org.projectnessie.versioned.storage.common.logic.CreateCommit.Remove.commitRemove;
import static org.projectnessie.versioned.storage.common.logic.DiffEntry.diffEntry;
import static org.projectnessie.versioned.storage.common.logic.Logics.indexesLogic;
import static org.projectnessie.versioned.storage.common.logic.PagingToken.emptyPagingToken;
import static org.projectnessie.versioned.storage.common.logic.PagingToken.pagingToken;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.Action.ADD;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.Action.NONE;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.Action.REMOVE;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.COMMIT_OP_SERIALIZER;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.commitOp;
import static org.projectnessie.versioned.storage.common.objtypes.StandardObjType.COMMIT;
import static org.projectnessie.versioned.storage.common.persist.ObjId.EMPTY_OBJ_ID;
import static org.projectnessie.versioned.storage.common.persist.ObjIdHasher.objIdHasher;
import static org.projectnessie.versioned.storage.common.persist.StoredObjResult.storedObjResult;

import com.google.common.collect.AbstractIterator;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.projectnessie.versioned.storage.common.config.StoreConfig;
import org.projectnessie.versioned.storage.common.exceptions.CommitConflictException;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.indexes.StoreIndex;
import org.projectnessie.versioned.storage.common.indexes.StoreIndexElement;
import org.projectnessie.versioned.storage.common.indexes.StoreKey;
import org.projectnessie.versioned.storage.common.logic.ConflictHandler.ConflictResolution;
import org.projectnessie.versioned.storage.common.logic.CreateCommit.Add;
import org.projectnessie.versioned.storage.common.logic.CreateCommit.Remove;
import org.projectnessie.versioned.storage.common.logic.CreateCommit.Unchanged;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.objtypes.CommitOp;
import org.projectnessie.versioned.storage.common.objtypes.CommitOp.Action;
import org.projectnessie.versioned.storage.common.objtypes.CommitType;
import org.projectnessie.versioned.storage.common.objtypes.IndexStripe;
import org.projectnessie.versioned.storage.common.persist.CloseableIterator;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjIdHasher;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;
import org.projectnessie.versioned.storage.common.persist.StoredObjResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Logic to read commits and perform commits including conflict checks. */
final class CommitLogicImpl implements CommitLogic {

  private static final Logger LOGGER = LoggerFactory.getLogger(CommitLogicImpl.class);

  static final String NO_COMMON_ANCESTOR_IN_PARENTS_OF = "No common ancestor in parents of ";
  private final Persist persist;

  CommitLogicImpl(Persist persist) {
    this.persist = persist;
  }

  @Override
  @Nonnull
  public PagedResult<CommitObj, ObjId> commitLog(@Nonnull CommitLogQuery commitLogQuery) {
    ObjId startCommitId =
        commitLogQuery
            .pagingToken()
            .map(PagingToken::token)
            .map(ObjId::objIdFromBytes)
            .orElse(commitLogQuery.commitId());

    return new CommitLogIter(startCommitId, commitLogQuery.endCommitId().orElse(null));
  }

  private final class CommitLogIter extends AbstractIterator<CommitObj>
      implements PagedResult<CommitObj, ObjId> {
    private final ObjId endCommitId;

    private Iterator<Obj> batch;
    private List<ObjId> next;

    CommitLogIter(ObjId startCommitId, ObjId endCommitId) {
      this.next = singletonList(startCommitId);
      this.endCommitId = endCommitId;
    }

    @Override
    protected CommitObj computeNext() {
      while (true) {
        Iterator<Obj> b = batch;
        if (b == null || !b.hasNext()) {
          List<ObjId> n = next;
          next = null;

          if (n == null) {
            return endOfData();
          }
          int i = n.indexOf(EMPTY_OBJ_ID);
          if (i != -1) {
            n = n.subList(0, i);
          }
          if (n.isEmpty()) {
            return endOfData();
          }

          try {
            b = batch = Arrays.asList(persist.fetchObjs(n.toArray(new ObjId[0]))).iterator();
          } catch (ObjNotFoundException e) {
            List<ObjId> ids = e.objIds();
            throw new NoSuchElementException(
                ids.size() == 1
                    ? "Commit '" + ids.get(0) + "' not found"
                    : "Commit(s) "
                        + ids.stream().map(ObjId::toString).collect(Collectors.joining(", "))
                        + " not found");
          }
        }

        if (b.hasNext()) {
          CommitObj c = (CommitObj) b.next();

          if (c == null) {
            // oops, commit not found...
            return endOfData();
          }

          if (c.id().equals(endCommitId)) {
            batch = emptyIterator();
            next = null;
          } else if (!b.hasNext()) {
            next = c.tail();
          }

          return c;
        }
      }
    }

    @Nonnull
    @Override
    public PagingToken tokenForKey(ObjId key) {
      return key != null ? pagingToken(key.asBytes()) : emptyPagingToken();
    }
  }

  @Override
  @Nonnull
  public PagedResult<ObjId, ObjId> commitIdLog(@Nonnull CommitLogQuery commitLogQuery) {
    ObjId startCommitId =
        commitLogQuery
            .pagingToken()
            .map(PagingToken::token)
            .map(ObjId::objIdFromBytes)
            .orElse(commitLogQuery.commitId());

    return new CommitIdIter(startCommitId, commitLogQuery.endCommitId().orElse(null));
  }

  private final class CommitIdIter extends AbstractIterator<ObjId>
      implements PagedResult<ObjId, ObjId> {
    private final ObjId endCommitId;

    private Iterator<ObjId> batch;
    private List<ObjId> next;

    CommitIdIter(ObjId startCommitId, ObjId endCommitId) {
      this.next = singletonList(startCommitId);
      this.endCommitId = endCommitId;
    }

    @Override
    protected ObjId computeNext() {
      while (true) {
        Iterator<ObjId> b = batch;
        if (b == null || !b.hasNext()) {
          List<ObjId> n = next;
          next = null;

          if (n == null) {
            return endOfData();
          }
          int i = n.indexOf(EMPTY_OBJ_ID);
          if (i != -1) {
            n = n.subList(0, i);
          }
          if (n.isEmpty()) {
            return endOfData();
          }

          b = batch = n.iterator();
        }

        if (b.hasNext()) {
          ObjId c = b.next();

          if (c.equals(endCommitId)) {
            batch = emptyIterator();
            next = null;
          } else if (!b.hasNext()) {
            CommitObj obj;
            try {
              obj = fetchCommit(c);
            } catch (ObjNotFoundException e) {
              throw new NoSuchElementException("Commit '" + c + "' not found");
            }
            if (obj == null) {
              // commit not found, oops
              return endOfData();
            }
            next = obj.tail();
          }

          return c;
        }
      }
    }

    @Nonnull
    @Override
    public PagingToken tokenForKey(ObjId key) {
      return key != null ? pagingToken(key.asBytes()) : emptyPagingToken();
    }
  }

  @Nullable
  @Override
  public CommitObj doCommit(
      @Nonnull CreateCommit createCommit, @Nonnull List<Obj> additionalObjects)
      throws CommitConflictException, ObjNotFoundException {
    CommitObj commit = buildCommitObj(createCommit);
    return storeCommit(commit, additionalObjects).obj().orElse(null);
  }

  @NotNull
  @Override
  public StoredObjResult<CommitObj> storeCommit(
      @Nonnull CommitObj commit, @Nonnull List<Obj> additionalObjects) {
    int numAdditional = additionalObjects.size();
    try {
      Obj[] allObjs = additionalObjects.toArray(new Obj[numAdditional + 1]);
      allObjs[numAdditional] = commit;

      boolean[] stored = persist.storeObjs(allObjs);
      return mitigateHashCollision(stored[numAdditional], commit);
    } catch (ObjTooLargeException e) {
      // The incremental index became too big - need to spill out the INCREMENTAL_* operations to
      // the reference index.

      try {
        persist.storeObjs(additionalObjects.toArray(new Obj[numAdditional]));
      } catch (ObjTooLargeException ex) {
        throw new RuntimeException(ex);
      }

      commit = indexTooBigStoreUpdate(commit);

      try {
        return mitigateHashCollision(persist.storeObj(commit, true), commit);
      } catch (ObjTooLargeException ex) {
        // Hit the "Hard database object size limit"
        throw new RuntimeException(ex);
      }
    }
  }

  /**
   * Called from the above {@link #storeCommit(CommitObj, List)}, handles the case when it could not
   * persist the {@link CommitObj} (duplicate object-id). Checks whether the persisted object is
   * actually the object that's expected to be persisted - and yields "OK" in that case. This
   * mitigates the risk of false-positive hash-collision errors in case the backend database runs
   * into timeout situations with an undefined outcome.
   */
  @NotNull
  private StoredObjResult<CommitObj> mitigateHashCollision(boolean storeResult, CommitObj commit) {
    if (storeResult) {
      return storedObjResult(commit, true);
    }

    // Check whether the existing object is the same commit (w/o considering the internal "created"
    // timestamp of the commit-obj).
    try {
      CommitObj existing = persist.fetchTypedObj(commit.id(), COMMIT, CommitObj.class);
      CommitObj commitWithNewCreatedTimestamp =
          CommitObj.commitBuilder().from(commit).created(existing.created()).build();
      return storedObjResult(
          commitWithNewCreatedTimestamp.equals(existing) ? existing : null, false);
    } catch (ObjNotFoundException e) {
      return null;
    }
  }

  @Override
  public CommitObj updateCommit(@Nonnull CommitObj commit) {
    try {
      persist.upsertObj(commit);
    } catch (ObjTooLargeException e) {
      // The incremental index became too big - need to spill out the INCREMENTAL_* operations to
      // the reference index.

      commit = indexTooBigStoreUpdate(commit);
      try {
        persist.upsertObj(commit);
      } catch (ObjTooLargeException ex) {
        // Hit the "Hard database object size limit"
        throw new RuntimeException(ex);
      }
    }
    return commit;
  }

  private CommitObj indexTooBigStoreUpdate(CommitObj commit) {
    StoreIndex<CommitOp> newIncremental = newStoreIndex(COMMIT_OP_SERIALIZER);
    StoreIndex<CommitOp> referenceIndex = createReferenceIndexForCommit(commit, newIncremental);

    try {
      commit = persistReferenceIndexForCommit(commit, newIncremental, referenceIndex);
    } catch (ObjTooLargeException ex) {
      throw new RuntimeException(ex);
    }
    return commit;
  }

  private CommitObj persistReferenceIndexForCommit(
      CommitObj commit, StoreIndex<CommitOp> newIncremental, StoreIndex<CommitOp> referenceIndex)
      throws ObjTooLargeException {
    IndexesLogic indexesLogic = indexesLogic(persist);
    ObjId referenceIndexId = null;
    List<IndexStripe> referenceIndexStripes = emptyList();
    // 'referenceIndex' can be null, if it became empty (aka all keys have been deleted)
    if (referenceIndex != null) {
      if (referenceIndex.stripes().size() <= persist.config().maxReferenceStripesPerCommit()) {
        referenceIndexStripes = indexesLogic.persistIndexStripesFromIndex(referenceIndex);
      } else {
        referenceIndexId = indexesLogic.persistStripedIndex(referenceIndex);
      }
    }
    commit =
        CommitObj.commitBuilder()
            .from(commit)
            .incrementalIndex(newIncremental.serialize())
            .referenceIndex(referenceIndexId)
            .referenceIndexStripes(referenceIndexStripes)
            .build();
    return commit;
  }

  private StoreIndex<CommitOp> createReferenceIndexForCommit(
      CommitObj commit, StoreIndex<CommitOp> newIncremental) {
    List<StoreIndex<CommitOp>> stripes;
    if (commit.hasReferenceIndex()) {
      // There is already an existing reference index, spill incremental index to existing ones.
      stripes = updateExistingReferenceIndex(commit, newIncremental);
    } else {
      // The commit does not refer to a reference index yet.
      stripes = createNewReferenceIndex(commit, newIncremental);
    }

    // The reference index is empty now (someone deleted all keys...)
    if (stripes.isEmpty()) {
      return null;
    } else if (stripes.size() == 1) {
      return stripes.get(0);
    } else {
      return indexFromStripes(stripes);
    }
  }

  private List<StoreIndex<CommitOp>> updateExistingReferenceIndex(
      CommitObj commitObj, StoreIndex<CommitOp> newIncremental) {
    int maxSize = persist.effectiveIndexSegmentSizeLimit();
    // use halt of the max as the initial size for _new_ segments/splits
    int newSegmentSize = maxSize / 2;

    IndexesLogic indexesLogic = indexesLogic(persist);
    StoreIndex<CommitOp> referenceIndex =
        requireNonNull(
                indexesLogic.buildReferenceIndexOnly(commitObj),
                "Commit is expected to have a reference index here")
            .asMutableIndex();

    List<StoreIndex<CommitOp>> currentStripes = referenceIndex.stripes();

    StoreIndex<CommitOp> incrementalIndex = indexesLogic.incrementalIndexFromCommit(commitObj);

    // Prefetch the stripes that will be touched
    Set<StoreKey> prefetch = new HashSet<>();
    for (StoreIndexElement<CommitOp> el : incrementalIndex) {
      CommitOp c = el.content();
      Action action = c.action();
      if (!action.currentCommit()) {
        prefetch.add(el.key());
      }
    }
    referenceIndex.loadIfNecessary(prefetch);

    for (StoreIndexElement<CommitOp> el : incrementalIndex) {
      CommitOp c = el.content();
      Action action = c.action();
      if (action.currentCommit()) {
        // Only keep the operations for the commit itself in the incremental index.
        newIncremental.add(el);
      } else {
        if (action.exists()) {
          // Add to the reference index using the `NONE` action, if it still exists.
          referenceIndex.add(
              indexElement(el.key(), commitOp(NONE, c.payload(), c.value(), c.contentId())));
        } else {
          // The element's been removed in the incremental index, can remove it from the reference
          // index here.
          referenceIndex.remove(el.key());
        }
      }
    }

    int newStripes = 0;
    int removedStripes = 0;
    int touched = 0;
    List<StoreIndex<CommitOp>> stripes = new ArrayList<>(currentStripes.size() * 2);
    for (StoreIndex<CommitOp> s : currentStripes) {
      if (s.isMutable()) {
        touched++;
        // a stripe has been modified, if it is mutable
        if (s.estimatedSerializedSize() > maxSize) {
          // Further split an existing stripe into at least two stripes
          int parts = Math.max(s.estimatedSerializedSize() / newSegmentSize + 1, 2);
          List<StoreIndex<CommitOp>> divided = s.divide(parts);
          newStripes += divided.size() - 1;
          stripes.addAll(divided);
          removedStripes++;
        } else {
          if (s.elementCount() > 0) { // Do not add empty stripes
            stripes.add(s);
          } else {
            removedStripes++;
          }
        }
      } else {
        stripes.add(s);
      }
    }

    LOGGER.info(
        "Generated reference index with {} stripes ({} touched, {} new, {} removed) for commit {} at seq # {}",
        stripes.size(),
        touched,
        newStripes,
        removedStripes,
        commitObj.id(),
        commitObj.seq());

    return stripes;
  }

  private List<StoreIndex<CommitOp>> createNewReferenceIndex(
      CommitObj commitObj, StoreIndex<CommitOp> newIncremental) {
    int maxSize = persist.effectiveIndexSegmentSizeLimit();
    // use half of the max as the initial size for _new_ segments/splits
    int newSegmentSize = maxSize / 2;

    List<StoreIndex<CommitOp>> stripes = new ArrayList<>();

    StoreIndex<CommitOp> current = newStoreIndex(COMMIT_OP_SERIALIZER);

    IndexesLogic indexesLogic = indexesLogic(persist);

    for (StoreIndexElement<CommitOp> el : indexesLogic.incrementalIndexFromCommit(commitObj)) {
      CommitOp content = el.content();
      if (!content.action().currentCommit()) {
        current.add(
            indexElement(
                el.key(), commitOp(NONE, content.payload(), content.value(), content.contentId())));
      } else {
        newIncremental.add(el);
      }
      if (current.estimatedSerializedSize() > newSegmentSize) {
        stripes.add(current);
        current = newStoreIndex(COMMIT_OP_SERIALIZER);
      }
    }
    if (current.elementCount() > 0) {
      stripes.add(current);
    }

    int sz = stripes.size();
    LOGGER.info(
        "Generated reference index with {} stripes ({} touched, {} new) for commit {} at seq # {}",
        sz,
        sz,
        sz,
        commitObj.id(),
        commitObj.seq());

    return stripes;
  }

  @Nonnull
  @Override
  public CommitObj buildCommitObj(
      @Nonnull CreateCommit createCommit,
      @Nonnull ConflictHandler conflictHandler,
      @Nonnull CommitOpHandler commitOpHandler,
      @Nonnull ValueReplacement expectedValueReplacement,
      @Nonnull ValueReplacement committedValueReplacement)
      throws CommitConflictException, ObjNotFoundException {
    StoreConfig config = persist.config();

    ObjId parentCommitId = createCommit.parentCommitId();
    CommitObj.Builder c =
        CommitObj.commitBuilder()
            .created(config.currentTimeMicros())
            .addAllSecondaryParents(createCommit.secondaryParents())
            .addTail(parentCommitId)
            .message(createCommit.message())
            .headers(createCommit.headers())
            .commitType(createCommit.commitType());

    ObjIdHasher hasher =
        objIdHasher(COMMIT)
            .hash(parentCommitId)
            .hash(createCommit.message())
            .hash(createCommit.headers());

    CommitObj parent = fetchCommit(parentCommitId);
    StoreIndex<CommitOp> index;
    StoreIndex<CommitOp> fullIndex;
    if (parent != null) {
      List<ObjId> parentTail = parent.tail();
      int amount = Math.min(config.parentsPerCommit() - 1, parentTail.size());
      for (int i = 0; i < amount; i++) {
        c.addTail(parentTail.get(i));
      }

      IndexesLogic indexesLogic = indexesLogic(persist);

      StoreIndex<CommitOp> incrementalIndex = indexesLogic.incrementalIndexFromCommit(parent);
      index = indexesLogic.incrementalIndexForUpdate(parent, Optional.of(incrementalIndex));
      c.seq(parent.seq() + 1)
          .referenceIndex(parent.referenceIndex())
          .addAllReferenceIndexStripes(parent.referenceIndexStripes());
      fullIndex = indexesLogic.buildCompleteIndex(parent, Optional.of(incrementalIndex));
    } else {
      checkArgument(
          EMPTY_OBJ_ID.equals(parentCommitId),
          "Commit to build points to non-existing parent commit %s",
          parentCommitId);
      fullIndex = index = newStoreIndex(COMMIT_OP_SERIALIZER);
      c.seq(1L);
    }

    List<CommitConflict> conflicts = new ArrayList<>();

    // Keys used in all "add", "unchanged" and "remove" actions.
    Set<StoreKey> keys =
        newHashSetWithExpectedSize(createCommit.adds().size() + createCommit.removes().size());
    Set<StoreKey> reAddedKeys = newHashSetWithExpectedSize(createCommit.removes().size());

    preprocessCommitActions(createCommit, keys, reAddedKeys);

    // Results in a bulk-(pre)fetch of the requested index stripes
    fullIndex.loadIfNecessary(keys);

    Map<UUID, CommitOp> removes = newHashMapWithExpectedSize(createCommit.removes().size());
    for (Remove remove : createCommit.removes()) {
      StoreKey key = remove.key();
      UUID contentId = remove.contentId();
      int payload = remove.payload();

      StoreIndexElement<CommitOp> existing = existingFromIndex(fullIndex, key);
      CommitOp existingContent = existing != null ? existing.content() : null;

      ObjId expectedValue =
          expectedValueReplacement.maybeReplaceValue(false, key, remove.expectedValue());
      CommitOp op = commitOp(REMOVE, payload, expectedValue, contentId);
      if (contentId != null) {
        removes.put(contentId, op);
      }

      CommitConflict conflict =
          checkForConflict(key, contentId, payload, op, existingContent, expectedValue);

      if (conflict != null) {
        if (handleConflict(conflictHandler, conflicts, conflict)) {
          continue;
        }
      } else {
        commitOpHandler.nonConflicting(key, null);
      }

      // No conflict, add "remove action" to index
      hasher.hash(2).hash(payload).hash(key.rawString()).hash(contentId);
      index.add(indexElement(key, op));
    }

    for (Unchanged unchanged : createCommit.unchanged()) {
      StoreKey key = unchanged.key();
      UUID contentId = unchanged.contentId();
      int payload = unchanged.payload();

      boolean reAdded = reAddedKeys.contains(key);
      CommitOp existingContent = existingContentForCommit(fullIndex, key, reAdded);

      ObjId expectedValue =
          expectedValueReplacement.maybeReplaceValue(false, key, unchanged.expectedValue());
      CommitConflict conflict =
          checkForConflict(
              key, contentId, payload, null, existingContent, reAdded ? null : expectedValue);

      if (conflict != null) {
        handleConflict(conflictHandler, conflicts, conflict);
      }
    }

    for (Add add : createCommit.adds()) {
      StoreKey key = add.key();
      UUID contentId = add.contentId();
      int payload = add.payload();

      ObjId addValue = committedValueReplacement.maybeReplaceValue(true, key, add.value());
      if (addValue != null && !addValue.equals(add.value())) {
        add = commitAdd(key, payload, addValue, add.expectedValue(), contentId);
      }

      CommitOp op = commitOp(ADD, payload, addValue, add.contentId());
      CommitConflict conflict = null;

      boolean reAdded = reAddedKeys.contains(key);
      CommitOp existingContent = existingContentForCommit(fullIndex, key, reAdded);

      if (!reAdded) {
        // Check whether the content-ID has been removed above. If yes, check whether the content-ID
        // is being reused. If so, then the commit contains a rename-operation.
        CommitOp removeOp = removes.remove(contentId);
        if (removeOp != null) {
          if (existingContent != null) {
            conflict = commitConflict(key, KEY_EXISTS, op, existingContent);
          }
          existingContent = removeOp;
        }
      }

      ObjId expectedValue =
          expectedValueReplacement.maybeReplaceValue(true, key, add.expectedValue());
      if (conflict == null) {
        conflict =
            checkForConflict(
                key, contentId, payload, op, existingContent, reAdded ? null : expectedValue);
      }

      if (conflict != null) {
        if (handleConflict(conflictHandler, conflicts, conflict)) {
          continue;
        }
      } else {
        commitOpHandler.nonConflicting(key, add.value());
      }

      // No conflict, add "add action" to index
      hasher
          .hash(1)
          .hash(key.rawString())
          .hash(payload)
          .hash(add.value().asByteBuffer())
          .hash(contentId);
      index.add(indexElement(key, op));
    }

    if (!conflicts.isEmpty()) {
      throw new CommitConflictException(conflicts);
    }

    return c.incrementalIndex(index.serialize()).id(hasher.generate()).build();
  }

  private static void preprocessCommitActions(
      CreateCommit createCommit, Set<StoreKey> keys, Set<StoreKey> reAddedKeys) {
    Set<StoreKey> removedKeys = newHashSetWithExpectedSize(createCommit.removes().size());

    // Collect the removed keys - both for "pure" deletes, renames and re-adds.
    for (Remove remove : createCommit.removes()) {
      StoreKey key = remove.key();
      checkArgument(keys.add(key), "Duplicate key: " + key);
      removedKeys.add(key);
    }

    // Collect the keys that are re-added w/ potentially different payload and content-ID:
    // A "re-add" is the scenario when for example a data lake table is dropped and another one
    // created with the same name.
    Set<UUID> seenContentIds = newHashSetWithExpectedSize(createCommit.adds().size());
    for (Add add : createCommit.adds()) {
      StoreKey key = add.key();
      // Check that the key to be 'added' is either not contained in the set of keys being
      // 'removed' or that it is only removed+added once. The latter covers the scenario that a
      // key is _explicitly_ removed to be added with a different payload as a _new_ key.
      boolean unseenKey = keys.add(key);
      if (!unseenKey && removedKeys.remove(key)) {
        reAddedKeys.add(key);
      } else {
        checkArgument(unseenKey, "Duplicate key: " + key);
      }
      UUID contentId = add.contentId();
      if (contentId != null) {
        checkArgument(
            seenContentIds.add(contentId),
            "Duplicate content ID: " + contentId + " for key: " + key);
      }
    }

    // Collect keys from "unchanged" actions.
    for (Unchanged unchanged : createCommit.unchanged()) {
      StoreKey key = unchanged.key();
      checkArgument(keys.add(key), "Duplicate key: " + key);
    }
  }

  private static CommitConflict checkForConflict(
      StoreKey key,
      UUID contentId,
      int payload,
      CommitOp op,
      CommitOp existingContent,
      ObjId expectedValue) {
    CommitConflict conflict = null;
    if (expectedValue == null) {
      if (existingContent != null) {
        conflict = commitConflict(key, KEY_EXISTS, op, existingContent);
      }
    } else {
      if (existingContent != null) {
        if (payload != existingContent.payload()) {
          conflict = commitConflict(key, PAYLOAD_DIFFERS, op, existingContent);
        } else if (!Objects.equals(contentId, existingContent.contentId())) {
          conflict = commitConflict(key, CONTENT_ID_DIFFERS, op, existingContent);
        } else if (!expectedValue.equals(existingContent.value())) {
          conflict = commitConflict(key, VALUE_DIFFERS, op, existingContent);
        }
      } else {
        conflict = commitConflict(key, KEY_DOES_NOT_EXIST, op);
      }
    }
    //

    return conflict;
  }

  private static CommitOp existingContentForCommit(
      StoreIndex<CommitOp> fullIndex, StoreKey key, boolean readded) {
    StoreIndexElement<CommitOp> existing =
        // Need to "manually" consult 'readdedKeys', because the 'index' updated above in
        // the "remove-loop" is a _new_ index object.
        readded ? null : existingFromIndex(fullIndex, key);
    return existing != null ? existing.content() : null;
  }

  private static boolean handleConflict(
      @Nonnull ConflictHandler conflictHandler,
      @Nonnull List<CommitConflict> conflicts,
      @Nonnull CommitConflict conflict) {
    ConflictResolution resolution = conflictHandler.onConflict(conflict);
    switch (resolution) {
      case CONFLICT:
        conflicts.add(conflict);
        // Do not the conflicting action to the commit, report the conflict
        return true;
      case ADD:
        // Add the conflicting action to the resulting commit, not reporting as a conflict
        return false;
      case DROP:
        // Do not add the conflicting action to the resulting commit, not reporting as a conflict
        return true;
      default:
        throw new IllegalStateException("Unknown resolution " + resolution);
    }
  }

  private static StoreIndexElement<CommitOp> existingFromIndex(
      StoreIndex<CommitOp> index, StoreKey key) {
    StoreIndexElement<CommitOp> existing = index.get(key);
    return existing != null && existing.content().action().exists() ? existing : null;
  }

  @Nonnull
  @Override
  public ObjId findCommonAncestor(@Nonnull ObjId targetId, @Nonnull ObjId sourceId)
      throws NoSuchElementException {
    return identifyMergeBase(targetId, sourceId, false);
  }

  @Nonnull
  @Override
  public ObjId findMergeBase(@Nonnull ObjId targetId, @Nonnull ObjId sourceId)
      throws NoSuchElementException {
    return identifyMergeBase(targetId, sourceId, true);
  }

  private ObjId identifyMergeBase(ObjId targetId, ObjId sourceId, boolean respectMergeParents) {
    return MergeBase.builder()
        .loadCommit(
            commitId -> {
              try {
                return fetchCommit(commitId);
              } catch (ObjNotFoundException e) {
                return null;
              }
            })
        .targetCommitId(targetId)
        .fromCommitId(sourceId)
        .respectMergeParents(respectMergeParents)
        .build()
        .identifyMergeBase();
  }

  @Nullable
  @Override
  public CommitObj fetchCommit(@Nonnull ObjId commitId) throws ObjNotFoundException {
    if (EMPTY_OBJ_ID.equals(commitId)) {
      return null;
    }
    return persist.fetchTypedObj(commitId, COMMIT, CommitObj.class);
  }

  @Nonnull
  @Override
  public CommitObj[] fetchCommits(@Nonnull ObjId startCommitId, @Nonnull ObjId endCommitId)
      throws ObjNotFoundException {
    CommitObj[] commitObjs = new CommitObj[2];
    if (startCommitId.equals(endCommitId)) {
      commitObjs[0] = commitObjs[1] = fetchCommit(startCommitId);
      return commitObjs;
    }

    if (EMPTY_OBJ_ID.equals(startCommitId)) {
      startCommitId = null;
    }
    if (EMPTY_OBJ_ID.equals(endCommitId)) {
      endCommitId = null;
    }

    CommitObj[] r = new CommitObj[2];
    if (startCommitId != null || endCommitId != null) {
      r = persist.fetchTypedObjs(new ObjId[] {startCommitId, endCommitId}, COMMIT, CommitObj.class);
    }
    return r;
  }

  @Nonnull
  @Override
  public DiffPagedResult<DiffEntry, StoreKey> diff(@Nonnull DiffQuery diffQuery) {
    IndexesLogic indexesLogic = indexesLogic(persist);

    StoreKey start =
        diffQuery
            .pagingToken()
            .map(t -> keyFromString(t.token().toStringUtf8()))
            .orElse(diffQuery.start());
    StoreKey end = diffQuery.end();

    StoreIndex<CommitOp> fromIndex = indexesLogic.buildCompleteIndexOrEmpty(diffQuery.fromCommit());
    StoreIndex<CommitOp> toIndex = indexesLogic.buildCompleteIndexOrEmpty(diffQuery.toCommit());

    Iterator<StoreIndexElement<CommitOp>> fromIter =
        fromIndex.iterator(start, end, diffQuery.prefetch());
    Iterator<StoreIndexElement<CommitOp>> toIter =
        toIndex.iterator(start, end, diffQuery.prefetch());

    return new DiffEntryIter(fromIndex, toIndex, fromIter, toIter, diffQuery.filter());
  }

  private static final class DiffEntryIter extends AbstractIterator<DiffEntry>
      implements DiffPagedResult<DiffEntry, StoreKey> {
    private final Iterator<StoreIndexElement<CommitOp>> fromIter;
    private final Iterator<StoreIndexElement<CommitOp>> toIter;
    private final Predicate<StoreKey> filter;

    private StoreIndexElement<CommitOp> fromElement;
    private StoreIndexElement<CommitOp> toElement;

    private final StoreIndex<CommitOp> fromIndex;
    private final StoreIndex<CommitOp> toIndex;

    DiffEntryIter(
        StoreIndex<CommitOp> fromIndex,
        StoreIndex<CommitOp> toIndex,
        Iterator<StoreIndexElement<CommitOp>> fromIter,
        Iterator<StoreIndexElement<CommitOp>> toIter,
        Predicate<StoreKey> filter) {
      this.fromIndex = fromIndex;
      this.toIndex = toIndex;
      this.fromIter = fromIter;
      this.toIter = toIter;
      this.filter = filter != null ? filter : x -> true;
    }

    @Override
    public StoreIndex<CommitOp> fromIndex() {
      return fromIndex;
    }

    @Override
    public StoreIndex<CommitOp> toIndex() {
      return toIndex;
    }

    @Override
    protected DiffEntry computeNext() {
      while (true) {
        if (fromElement == null) {
          fromElement = next(fromIter);
        }
        if (toElement == null) {
          toElement = next(toIter);
        }

        if (fromElement == null && toElement == null) {
          // No more elements, done.
          return endOfData();
        }

        if (fromElement == null) {
          // No more "from" elements, consume from "to"
          return consumeTo();
        }
        if (toElement == null) {
          // No more "to" elements, consume from "from"
          return consumeFrom();
        }

        StoreKey fromKey = fromElement.key();
        StoreKey toKey = toElement.key();

        int cmp = fromKey.compareTo(toKey);
        // Consume either the "from" element, the "to" element or produce a diff between
        // "from" and "to".
        if (cmp < 0) {
          return consumeFrom();
        } else if (cmp > 0) {
          return consumeTo();
        } else {
          DiffEntry r = consumeBoth();
          if (r != null) {
            return r;
          }
        }
      }
    }

    private DiffEntry consumeBoth() {
      DiffEntry e = null;
      StoreIndexElement<CommitOp> f = fromElement;
      StoreIndexElement<CommitOp> t = toElement;
      CommitOp fc = f.content();
      CommitOp tc = t.content();
      if (!Objects.equals(f.content().value(), t.content().value())) {
        e =
            diffEntry(
                t.key(),
                fc.value(),
                fc.payload(),
                fc.contentId(),
                tc.value(),
                tc.payload(),
                tc.contentId());
      }
      fromElement = null;
      toElement = null;
      return e;
    }

    private DiffEntry consumeTo() {
      StoreIndexElement<CommitOp> t = toElement;
      toElement = null;
      CommitOp c = t.content();
      return diffEntry(t.key(), null, 0, null, c.value(), c.payload(), c.contentId());
    }

    private DiffEntry consumeFrom() {
      StoreIndexElement<CommitOp> f = fromElement;
      fromElement = null;
      CommitOp c = f.content();
      return diffEntry(f.key(), c.value(), c.payload(), c.contentId(), null, 0, null);
    }

    private StoreIndexElement<CommitOp> next(Iterator<StoreIndexElement<CommitOp>> iter) {
      while (iter.hasNext()) {
        StoreIndexElement<CommitOp> el = iter.next();
        if (el.content().action().exists() && filter.test(el.key())) {
          return el;
        }
      }
      return null;
    }

    @Nonnull
    @Override
    public PagingToken tokenForKey(StoreKey key) {
      return key != null ? pagingToken(copyFromUtf8(key.rawString())) : emptyPagingToken();
    }
  }

  @Nonnull
  @Override
  public CreateCommit.Builder diffToCreateCommit(
      @Nonnull PagedResult<DiffEntry, StoreKey> diff, @Nonnull CreateCommit.Builder createCommit) {
    while (diff.hasNext()) {
      DiffEntry d = diff.next();
      if (d.fromId() == null) {
        // key added
        createCommit.addAdds(
            commitAdd(d.key(), d.toPayload(), requireNonNull(d.toId()), null, d.toContentId()));
      } else if (d.toId() == null) {
        // key removed
        createCommit.addRemoves(
            commitRemove(d.key(), d.fromPayload(), requireNonNull(d.fromId()), d.fromContentId()));
      } else if (Objects.equals(d.fromContentId(), d.toContentId())) {
        // key updated
        createCommit.addAdds(
            commitAdd(
                d.key(), d.toPayload(), requireNonNull(d.toId()), d.fromId(), d.toContentId()));
      } else {
        // key re-added
        createCommit.addRemoves(
            commitRemove(d.key(), d.fromPayload(), requireNonNull(d.fromId()), d.fromContentId()));
        createCommit.addAdds(
            commitAdd(
                d.key(),
                d.toPayload(),
                requireNonNull(d.toId()),
                requireNonNull(d.fromId()),
                d.toContentId()));
      }
    }
    return createCommit;
  }

  @Override
  @Nullable
  public CommitObj headCommit(@Nonnull Reference reference) throws ObjNotFoundException {
    return fetchCommit(reference.pointer());
  }

  @Override
  public HeadsAndForkPoints identifyAllHeadsAndForkPoints(
      int expectedCommitCount, Consumer<CommitObj> commitHandler) {

    // Need to remember the time when the identification started, so that a follow-up
    // identifyReferencedAndUnreferencedHeads() knows when it can stop scanning a named-reference's
    // commit-log. identifyReferencedAndUnreferencedHeads() has to read up to the first commit
    // _before_ this timestamp to not commit-IDs as "unreferenced".
    //
    // Note: keep in mind, that scanAllCommitLogEntries() returns all commits in a
    // non-deterministic order. Example: if (at least) two commits are added to a branch while this
    // function is running, the original HEAD of that branch could otherwise be returned as
    // "unreferenced".
    long scanStartedAtInMicros = persist.config().currentTimeMicros();

    IdentifyHeadsAndForkPoints identify =
        new IdentifyHeadsAndForkPoints(expectedCommitCount, scanStartedAtInMicros);

    // scanAllCommitLogEntries() returns all commits in no specific order, parents may be scanned
    // before or after their children.
    try (CloseableIterator<Obj> scan = persist.scanAllObjects(Collections.singleton(COMMIT))) {
      while (scan.hasNext()) {
        CommitObj commit = (CommitObj) scan.next();

        // Ignore commits on internal references
        if (commit.commitType() == CommitType.INTERNAL) {
          continue;
        }

        if (identify.handleCommit(commit)) {
          commitHandler.accept(commit);
          // no need to bother with secondary parents, we are scanning everything anyway
        }
      }
    }

    return identify.finish();
  }
}
