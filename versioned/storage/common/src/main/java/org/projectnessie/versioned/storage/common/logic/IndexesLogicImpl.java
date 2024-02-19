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
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.projectnessie.versioned.storage.common.indexes.StoreIndexes.deserializeStoreIndex;
import static org.projectnessie.versioned.storage.common.indexes.StoreIndexes.indexFromSplits;
import static org.projectnessie.versioned.storage.common.indexes.StoreIndexes.layeredIndex;
import static org.projectnessie.versioned.storage.common.indexes.StoreIndexes.lazyStoreIndex;
import static org.projectnessie.versioned.storage.common.indexes.StoreIndexes.newStoreIndex;
import static org.projectnessie.versioned.storage.common.logic.CommitLogQuery.commitLogQuery;
import static org.projectnessie.versioned.storage.common.logic.Logics.commitLogic;
import static org.projectnessie.versioned.storage.common.logic.SuppliedCommitIndex.suppliedCommitIndex;
import static org.projectnessie.versioned.storage.common.objtypes.CommitObj.commitBuilder;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.Action.INCREMENTAL_ADD;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.Action.INCREMENTAL_REMOVE;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.COMMIT_OP_SERIALIZER;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.commitOp;
import static org.projectnessie.versioned.storage.common.objtypes.IndexObj.index;
import static org.projectnessie.versioned.storage.common.objtypes.IndexSegmentsObj.indexSegments;
import static org.projectnessie.versioned.storage.common.objtypes.IndexStripe.indexStripe;
import static org.projectnessie.versioned.storage.common.objtypes.StandardObjType.COMMIT;
import static org.projectnessie.versioned.storage.common.objtypes.StandardObjType.INDEX;
import static org.projectnessie.versioned.storage.common.persist.ObjId.EMPTY_OBJ_ID;
import static org.projectnessie.versioned.storage.common.util.SupplyOnce.memoize;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.AbstractIterator;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.indexes.IndexLoader;
import org.projectnessie.versioned.storage.common.indexes.StoreIndex;
import org.projectnessie.versioned.storage.common.indexes.StoreIndexElement;
import org.projectnessie.versioned.storage.common.indexes.StoreKey;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.objtypes.CommitOp;
import org.projectnessie.versioned.storage.common.objtypes.IndexObj;
import org.projectnessie.versioned.storage.common.objtypes.IndexSegmentsObj;
import org.projectnessie.versioned.storage.common.objtypes.IndexStripe;
import org.projectnessie.versioned.storage.common.objtypes.StandardObjType;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class IndexesLogicImpl implements IndexesLogic {
  private static final Logger LOGGER = LoggerFactory.getLogger(IndexesLogicImpl.class);
  private final Persist persist;

  IndexesLogicImpl(Persist persist) {
    this.persist = persist;
  }

  @Override
  @Nonnull
  public Supplier<SuppliedCommitIndex> createIndexSupplier(
      @Nonnull Supplier<ObjId> commitIdSupplier) {
    return memoize(
        () -> {
          try {
            ObjId commitId = commitIdSupplier.get();
            CommitObj c =
                EMPTY_OBJ_ID.equals(commitId)
                    ? null
                    : persist.fetchTypedObj(commitId, COMMIT, CommitObj.class);
            return suppliedCommitIndex(commitId, buildCompleteIndexOrEmpty(c));
          } catch (ObjNotFoundException e) {
            throw new RuntimeException(e);
          }
        });
  }

  @Override
  @Nonnull
  public StoreIndex<CommitOp> incrementalIndexForUpdate(
      @Nonnull CommitObj commit, Optional<StoreIndex<CommitOp>> loadedIncrementalIndex) {
    checkArgument(!commit.incompleteIndex(), "Commit %s has no complete key index", commit.id());

    boolean hasReferenceIndex = commit.hasReferenceIndex();

    StoreIndex<CommitOp> i =
        loadedIncrementalIndex.orElseGet(() -> incrementalIndexFromCommit(commit));
    i.updateAll(
        el -> {
          CommitOp c = el.content();
          switch (c.action()) {
            case ADD:
              c = commitOp(INCREMENTAL_ADD, c.payload(), c.value(), c.contentId());
              break;
            case REMOVE:
              if (hasReferenceIndex) {
                c = commitOp(INCREMENTAL_REMOVE, c.payload(), c.value(), c.contentId());
              } else {
                // purge old removes, if there is no reference-index that might contain those
                c = null;
              }
              break;
            case INCREMENTAL_REMOVE:
              if (!hasReferenceIndex) {
                // purge old removes, if there is no reference-index that might contain those
                c = null;
              }
              break;
            default:
              break;
          }
          return c;
        });
    return i;
  }

  @Nonnull
  @Override
  public Iterable<StoreIndexElement<CommitOp>> commitOperations(@Nonnull CommitObj commitObj) {
    return commitOperations(incrementalIndexFromCommit(commitObj));
  }

  @Nonnull
  @Override
  public Iterable<StoreIndexElement<CommitOp>> commitOperations(
      @Nonnull StoreIndex<CommitOp> index) {
    return () ->
        new AbstractIterator<>() {
          final Iterator<StoreIndexElement<CommitOp>> delegate = index.iterator();

          @Override
          protected StoreIndexElement<CommitOp> computeNext() {
            Iterator<StoreIndexElement<CommitOp>> d = delegate;
            while (true) {
              if (!d.hasNext()) {
                return endOfData();
              }
              StoreIndexElement<CommitOp> el = d.next();
              if (el.content().action().currentCommit()) {
                return el;
              }
            }
          }
        };
  }

  @Nonnull
  @Override
  public StoreIndex<CommitOp> incrementalIndexFromCommit(@Nonnull CommitObj commit) {
    return deserializeIndex(commit.incrementalIndex());
  }

  @Override
  @Nonnull
  public StoreIndex<CommitOp> buildCompleteIndex(
      @Nonnull CommitObj commit, Optional<StoreIndex<CommitOp>> loadedIncrementalIndex) {
    checkArgument(!commit.incompleteIndex(), "Commit %s has no complete key index", commit.id());

    StoreIndex<CommitOp> incremental =
        loadedIncrementalIndex.orElseGet(() -> incrementalIndexFromCommit(commit));
    StoreIndex<CommitOp> index = incremental;

    ObjId referenceIndexId = commit.referenceIndex();
    List<IndexStripe> commitStripes = commit.referenceIndexStripes();
    if (!commitStripes.isEmpty()) {
      checkState(
          referenceIndexId == null,
          "Commit %s: must not have both pointer to a reference index and stripes",
          commit.id());
      StoreIndex<CommitOp> referenceIndex = referenceIndexFromStripes(commitStripes, commit.id());
      index = layeredIndex(referenceIndex, incremental);
    } else if (referenceIndexId != null) {
      StoreIndex<CommitOp> referenceIndex = buildReferenceIndexOnly(referenceIndexId, commit.id());
      index = layeredIndex(referenceIndex, incremental);
    }

    return index;
  }

  @Override
  @Nullable
  public StoreIndex<CommitOp> buildReferenceIndexOnly(@Nonnull CommitObj commit) {
    ObjId referenceIndexId = commit.referenceIndex();
    List<IndexStripe> commitStripes = commit.referenceIndexStripes();
    if (!commitStripes.isEmpty()) {
      checkState(
          referenceIndexId == null,
          "Commit %s: must not have both pointer to a reference index and stripes",
          commit.id());
      return referenceIndexFromStripes(commitStripes, commit.id());
    }
    if (referenceIndexId != null) {
      return buildReferenceIndexOnly(referenceIndexId, commit.id());
    }
    return null;
  }

  @Override
  @Nonnull
  public StoreIndex<CommitOp> buildReferenceIndexOnly(
      @Nonnull ObjId indexId, @Nonnull ObjId commitId) {
    return lazyStoreIndex(() -> loadReferenceIndex(indexId, commitId));
  }

  private StoreIndex<CommitOp> loadReferenceIndex(@Nonnull ObjId indexId, @Nonnull ObjId commitId) {
    Obj keyIndex;
    try {
      keyIndex = persist.fetchObj(indexId);
    } catch (ObjNotFoundException e) {
      throw new IllegalStateException(
          format("Commit %s references a reference index, which does not exist", indexId));
    }
    ObjType indexType = keyIndex.type();
    if (indexType instanceof StandardObjType) {
      switch ((StandardObjType) indexType) {
        case INDEX_SEGMENTS:
          IndexSegmentsObj split = (IndexSegmentsObj) keyIndex;
          List<IndexStripe> indexStripes = split.stripes();
          return referenceIndexFromStripes(indexStripes, commitId);
        case INDEX:
          return deserializeIndex(((IndexObj) keyIndex).index()).setObjId(keyIndex.id());
        default:
          // fall through
      }
    }
    throw new IllegalStateException(
        "Commit %s references a reference index, which is of unsupported key index type "
            + indexType);
  }

  static StoreIndex<CommitOp> deserializeIndex(ByteString serialized) {
    return deserializeStoreIndex(serialized, COMMIT_OP_SERIALIZER);
  }

  private StoreIndex<CommitOp> loadIndexSegment(@Nonnull ObjId indexId) {
    IndexObj index;
    try {
      index = persist.fetchTypedObj(indexId, INDEX, IndexObj.class);
    } catch (ObjNotFoundException e) {
      throw new IllegalStateException(
          format("Commit %s references a reference index, which does not exist", indexId));
    }
    return deserializeIndex(index.index()).setObjId(indexId);
  }

  private StoreIndex<CommitOp>[] loadIndexSegments(@Nonnull ObjId[] indexes) {
    try {
      IndexObj[] objs = persist.fetchTypedObjs(indexes, INDEX, IndexObj.class);
      @SuppressWarnings("unchecked")
      StoreIndex<CommitOp>[] r = new StoreIndex[indexes.length];
      for (int i = 0; i < objs.length; i++) {
        IndexObj index = objs[i];
        if (index != null) {
          r[i] = deserializeIndex(index.index()).setObjId(indexes[i]);
        }
      }
      return r;
    } catch (ObjNotFoundException e) {
      throw new IllegalStateException(format("Reference index segments %s not found", e.objIds()));
    }
  }

  private StoreIndex<CommitOp> referenceIndexFromStripes(
      List<IndexStripe> indexStripes, ObjId commitId) {
    List<StoreIndex<CommitOp>> stripes = new ArrayList<>(indexStripes.size());
    List<StoreKey> firstLastKeys = new ArrayList<>(indexStripes.size() * 2);

    @SuppressWarnings("unchecked")
    StoreIndex<CommitOp>[] loaded = new StoreIndex[indexStripes.size()];

    for (int i = 0; i < indexStripes.size(); i++) {
      IndexStripe s = indexStripes.get(i);
      int idx = i;
      stripes.add(
          lazyStoreIndex(
                  () -> {
                    StoreIndex<CommitOp> l = loaded[idx];
                    if (l == null) {
                      LOGGER.debug(
                          "Individual fetch of stripe #{} of {} stripes for commit {}",
                          idx,
                          loaded.length,
                          commitId);
                      l = loadIndexSegment(s.segment());
                      loaded[idx] = l;
                    }
                    return l;
                  },
                  s.firstKey(),
                  s.lastKey())
              .setObjId(s.segment()));
      firstLastKeys.add(s.firstKey());
      firstLastKeys.add(s.lastKey());
    }
    if (stripes.size() == 1) {
      return stripes.get(0);
    }

    IndexLoader<CommitOp> indexLoader =
        indexesToLoad -> {
          checkArgument(indexesToLoad.length == loaded.length);
          ObjId[] ids = new ObjId[indexesToLoad.length];
          int cnt = 0;
          for (int i = 0; i < indexesToLoad.length; i++) {
            StoreIndex<CommitOp> idx = indexesToLoad[i];
            if (idx != null) {
              ObjId segmentId = idx.getObjId();
              if (segmentId != null) {
                ids[i] = idx.getObjId();
                cnt++;
              } else {
                LOGGER.warn("Reference index Segment #{} has no objId for commit {}", i, commitId);
              }
            }
          }
          LOGGER.debug("Fetching {} of {} index segments for commit {}", cnt, ids.length, commitId);
          StoreIndex<CommitOp>[] indexes = loadIndexSegments(ids);
          for (int i = 0; i < indexes.length; i++) {
            StoreIndex<CommitOp> idx = indexes[i];
            if (idx != null) {
              loaded[i] = idx;
            } else if (ids[i] != null) {
              LOGGER.warn(
                  "Reference index Segment #{} has with id {} not loaded for commit {}",
                  i,
                  ids[i],
                  commitId);
            }
          }
          return indexes;
        };

    return indexFromSplits(stripes, firstLastKeys, indexLoader);
  }

  @Nonnull
  @Override
  public ObjId persistStripedIndex(@Nonnull StoreIndex<CommitOp> stripedIndex)
      throws ObjTooLargeException {
    List<StoreIndex<CommitOp>> stripes = stripedIndex.stripes();
    if (stripes.isEmpty()) {
      return persistIndex(stripedIndex);
    }
    if (stripes.size() == 1) {
      return persistIndex(stripes.get(0));
    }

    List<Obj> toStore = new ArrayList<>();
    List<IndexStripe> indexStripes = buildIndexStripes(stripes, toStore);

    IndexSegmentsObj referenceIndex = indexSegments(indexStripes);
    toStore.add(referenceIndex);

    persist.storeObjs(toStore.toArray(new Obj[0]));

    return requireNonNull(referenceIndex.id());
  }

  @Nonnull
  @Override
  public List<IndexStripe> persistIndexStripesFromIndex(@Nonnull StoreIndex<CommitOp> stripedIndex)
      throws ObjTooLargeException {
    List<StoreIndex<CommitOp>> stripes = stripedIndex.stripes();
    List<Obj> toStore = new ArrayList<>();
    List<IndexStripe> indexStripes = buildIndexStripes(stripes, toStore);
    persist.storeObjs(toStore.toArray(new Obj[0]));
    return indexStripes;
  }

  private List<IndexStripe> buildIndexStripes(
      List<StoreIndex<CommitOp>> stripes, List<Obj> toStore) {
    List<IndexStripe> indexStripes = new ArrayList<>(stripes.size());
    for (StoreIndex<CommitOp> indexSegment : stripes) {
      ObjId segId;
      if (!indexSegment.isModified()) {
        segId =
            requireNonNull(
                indexSegment.getObjId(), "Loaded index segment does not contain its ObjId");
      } else {
        IndexObj segment = index(indexSegment.serialize());
        toStore.add(segment);
        segId = segment.id();
      }

      StoreKey first = indexSegment.first();
      StoreKey last = indexSegment.last();
      checkState(first != null && last != null);

      indexStripes.add(indexStripe(first, last, segId));
    }

    return indexStripes;
  }

  private ObjId persistIndex(StoreIndex<CommitOp> indexSegment) throws ObjTooLargeException {
    if (!indexSegment.isModified()) {
      return requireNonNull(
          indexSegment.getObjId(), "Loaded index segment does not contain its ObjId");
    }
    IndexObj segment = index(indexSegment.serialize());
    persist.storeObj(segment);
    return segment.id();
  }

  @Override
  public void completeIndexesInCommitChain(@Nonnull ObjId commitId, Runnable progressCallback)
      throws ObjNotFoundException {
    Deque<ObjId> idsToProcess = new ArrayDeque<>();
    idsToProcess.add(commitId);

    while (!idsToProcess.isEmpty()) {
      ObjId id = idsToProcess.pollFirst();
      completeIndexesInCommitChain(id, idsToProcess, progressCallback);
    }
  }

  @VisibleForTesting
  void completeIndexesInCommitChain(
      @Nonnull ObjId commitId, @Nonnull Deque<ObjId> idsToProcess, Runnable progressCallback)
      throws ObjNotFoundException {
    CommitLogic commitLogic = commitLogic(persist);

    // Handle the case when 'commitId' accidentally points to a CommitObjReference, e.g. TagObj
    CommitObj head = commitLogic.fetchCommit(commitId);
    if (head == null) {
      return;
    }

    commitId = head.id();

    if (!head.incompleteIndex()) {
      return;
    }

    // HEAD commit first
    List<ObjId> commitsToUpdate = findCommitsWithIncompleteIndex(commitId);

    if (commitsToUpdate.isEmpty()) {
      return;
    }

    // Let the HEAD be the last element in the list, and the oldest commit being at index #0
    Collections.reverse(commitsToUpdate);

    int totalCommits = commitsToUpdate.size();
    ObjId oldestCommitId = commitsToUpdate.get(0);

    IntFunction<ObjId[]> prefetchIds =
        i -> commitsToUpdate.subList(i, Math.min(totalCommits, i + 100)).toArray(new ObjId[0]);

    // perform a bulk-load against the database, populates the cache
    persist.fetchObjs(prefetchIds.apply(0));

    CommitObj current = persist.fetchTypedObj(oldestCommitId, COMMIT, CommitObj.class);
    CommitObj parent =
        EMPTY_OBJ_ID.equals(current.directParent())
            ? null
            : persist.fetchTypedObj(current.directParent(), COMMIT, CommitObj.class);

    int parentsPerCommit = persist.config().parentsPerCommit();

    for (int i = 0; i < totalCommits; i++) {
      if (i > 0 && (i % 100) == 0) {
        // perform a bulk-load against the database, populates the cache
        persist.fetchObjs(prefetchIds.apply(i));
      }

      ObjId currentId = commitsToUpdate.get(i);
      if (current == null) {
        try {
          current = commitLogic.fetchCommit(currentId);
        } catch (ObjNotFoundException e) {
          throw new IllegalStateException(
              format(
                  "Commit %s has been seen while walking the commit log, but no longer exists",
                  currentId));
        }
      }

      checkState(
          current != null,
          "Commit %s has been seen while walking the commit log, but no longer exists",
          currentId);

      progressCallback.run();

      idsToProcess.addAll(current.secondaryParents());

      StoreIndex<CommitOp> newIndex;
      ObjId referenceIndex;
      List<IndexStripe> indexStripes;
      if (parent != null) {
        newIndex = incrementalIndexForUpdate(parent, Optional.empty());
        referenceIndex = parent.referenceIndex();
        indexStripes = parent.referenceIndexStripes();
      } else {
        newIndex = newStoreIndex(COMMIT_OP_SERIALIZER);
        referenceIndex = null;
        indexStripes = Collections.emptyList();
      }

      commitOperations(current).forEach(newIndex::add);

      CommitObj.Builder c =
          commitBuilder()
              .from(current)
              .incompleteIndex(false)
              .referenceIndex(referenceIndex)
              .referenceIndexStripes(indexStripes)
              .incrementalIndex(newIndex.serialize());

      if (parent != null) {
        int parents = Math.min(parentsPerCommit - 1, parent.tail().size());
        List<ObjId> tail = new ArrayList<>(parents + 1);
        tail.add(parent.id());
        tail.addAll(parent.tail().subList(0, parents));
        c.tail(tail);
      }

      parent = commitLogic.updateCommit(c.build());
      current = null;
    }
  }

  @VisibleForTesting
  List<ObjId> findCommitsWithIncompleteIndex(@Nonnull ObjId commitId) {
    ArrayList<ObjId> commitsToUpdate = new ArrayList<>();
    CommitLogic commitLogic = commitLogic(persist);
    for (PagedResult<CommitObj, ObjId> iter = commitLogic.commitLog(commitLogQuery(commitId));
        iter.hasNext(); ) {
      CommitObj c = iter.next();
      if (!c.incompleteIndex()) {
        break;
      }
      commitsToUpdate.add(c.id());
    }
    commitsToUpdate.trimToSize();
    return commitsToUpdate;
  }
}
