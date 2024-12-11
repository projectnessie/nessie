/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.versioned.storage.cleanup;

import static com.google.common.base.Preconditions.checkState;
import static org.projectnessie.versioned.storage.cleanup.HeapSizes.memSizeToStringMB;
import static org.projectnessie.versioned.storage.cleanup.PurgeObjectsContext.purgeObjectsContext;
import static org.projectnessie.versioned.storage.common.logic.CommitLogQuery.commitLogQuery;
import static org.projectnessie.versioned.storage.common.logic.Logics.commitLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.indexesLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.referenceLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.repositoryLogic;
import static org.projectnessie.versioned.storage.common.logic.ReferencesQuery.referencesQuery;
import static org.projectnessie.versioned.storage.common.objtypes.StandardObjType.INDEX_SEGMENTS;
import static org.projectnessie.versioned.storage.common.objtypes.StandardObjType.VALUE;
import static org.projectnessie.versioned.storage.common.persist.ObjId.EMPTY_OBJ_ID;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntFunction;
import org.agrona.collections.ObjectHashSet;
import org.projectnessie.model.Content;
import org.projectnessie.versioned.storage.common.indexes.StoreIndexElement;
import org.projectnessie.versioned.storage.common.logic.CommitLogic;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.objtypes.CommitOp;
import org.projectnessie.versioned.storage.common.objtypes.ContentValueObj;
import org.projectnessie.versioned.storage.common.objtypes.IndexSegmentsObj;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;
import org.projectnessie.versioned.store.DefaultStoreWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ReferencedObjectsResolverImpl implements ReferencedObjectsResolver {
  private static final Logger LOGGER = LoggerFactory.getLogger(ReferencedObjectsResolverImpl.class);

  private final ObjectHashSet<ObjId> pendingObjs = new ObjectHashSet<>();
  private final Deque<ObjId> pendingHeads = new ArrayDeque<>();

  /**
   * Set of recently handled 'ObjId's to prevent re-processing the same objects multiple times. This
   * happens, when the values referenced from the commit index are iterated, because it iterates
   * over all keys, not only the keys added by a particular commit.
   */
  private final RecentObjIdFilter recentObjIds;

  private final ReferencedObjectsContext referencedObjectsContext;

  private final ResolveStatsBuilder stats;
  private final RateLimit commitRateLimiter;
  private final RateLimit objRateLimiter;

  private final AtomicBoolean used = new AtomicBoolean();

  ReferencedObjectsResolverImpl(
      ReferencedObjectsContext referencedObjectsContext,
      IntFunction<RateLimit> rateLimitIntFunction) {
    this.referencedObjectsContext = referencedObjectsContext;
    this.stats = new ResolveStatsBuilder();
    this.commitRateLimiter =
        rateLimitIntFunction.apply(referencedObjectsContext.params().resolveCommitRatePerSecond());
    this.objRateLimiter =
        rateLimitIntFunction.apply(referencedObjectsContext.params().resolveObjRatePerSecond());
    this.recentObjIds =
        new RecentObjIdFilterImpl(referencedObjectsContext.params().recentObjIdsFilterSize());
  }

  @Override
  public long estimatedHeapPressure() {
    return referencedObjectsContext.referencedObjects().estimatedHeapPressure()
        + referencedObjectsContext.visitedCommitFilter().estimatedHeapPressure()
        + recentObjIds.estimatedHeapPressure();
  }

  @Override
  public ResolveResult resolve() throws MustRestartWithBiggerFilterException {
    checkState(used.compareAndSet(false, true), "resolve() has already been called.");

    LOGGER.info(
        "Identifying referenced objects in repository '{}', processing {} commits per second, processing {} objects per second, estimated context heap pressure: {}",
        referencedObjectsContext.persist().config().repositoryId(),
        commitRateLimiter,
        objRateLimiter,
        memSizeToStringMB(estimatedHeapPressure()));

    var persist = referencedObjectsContext.persist();
    var params = referencedObjectsContext.params();

    ResolveStats finalStats = null;
    try {
      finalStats = doResolve(persist, params);

      LOGGER.info(
          "Successfully finished identifying referenced objects after {} in repository '{}', resolve stats: {}, estimated context heap pressure: {}",
          finalStats.duration(),
          persist.config().repositoryId(),
          finalStats,
          memSizeToStringMB(estimatedHeapPressure()));
    } catch (MustRestartWithBiggerFilterRuntimeException mustRestart) {
      LOGGER.warn(
          "Must restart identifying referenced objects for repository '{}', current parameters: expected object count: {}, FPP: {}, allowed FPP: {}, resolve stats: {}, estimated context heap pressure: {}",
          persist.config().repositoryId(),
          params.expectedObjCount(),
          params.falsePositiveProbability(),
          params.allowedFalsePositiveProbability(),
          finalStats,
          memSizeToStringMB(estimatedHeapPressure()));
      throw new MustRestartWithBiggerFilterException(mustRestart.getMessage(), mustRestart);
    } catch (RuntimeException e) {
      if (finalStats != null) {
        LOGGER.warn(
            "Error while identifying referenced objects after {} in repository '{}', stats: {}, estimated context heap pressure: {}",
            finalStats.duration(),
            persist.config().repositoryId(),
            finalStats,
            memSizeToStringMB(estimatedHeapPressure()),
            e);
      } else {
        LOGGER.warn(
            "Error while identifying referenced objects after {} in repository '{}'",
            persist.config().repositoryId(),
            e);
      }
      throw e;
    }

    return ImmutableResolveResult.of(stats.build(), purgeObjectsContext(referencedObjectsContext));
  }

  private ResolveStats doResolve(Persist persist, CleanupParams params) {
    var clock = persist.config().clock();

    ResolveStats finalStats;
    try {
      stats.started = clock.instant();

      checkState(
          repositoryLogic(persist).repositoryExists(),
          "The provided repository has not been initialized.");

      params.relatedObjects().repositoryRelatedObjects().forEach(this::pendingObj);

      var referenceLogic = referenceLogic(persist);
      var commitLogic = commitLogic(persist);

      for (String internalReferenceName : params.internalReferenceNames()) {
        var intRef = persist.fetchReference(internalReferenceName);
        checkState(intRef != null, "Internal reference %s not found!", internalReferenceName);
        handleReference(intRef);
        processPendingHeads(commitLogic);
      }

      for (var referencesIter = referenceLogic.queryReferences(referencesQuery());
          referencesIter.hasNext(); ) {
        var reference = referencesIter.next();
        handleReference(reference);
        processPendingHeads(commitLogic);
      }

      processPendingHeads(commitLogic);

      while (!pendingObjs.isEmpty()) {
        processPendingObjs();
      }
    } catch (RuntimeException e) {
      stats.mustRestart = e instanceof MustRestartWithBiggerFilterRuntimeException;
      stats.failure = e;
      throw e;
    } finally {
      stats.ended = clock.instant();
      finalStats = stats.build();
    }
    return finalStats;
  }

  private void processPendingHeads(CommitLogic commitLogic) {
    while (!pendingHeads.isEmpty()) {
      var head = pendingHeads.removeFirst();
      commitLogic.commitLog(commitLogQuery(head)).forEachRemaining(this::handleCommit);
    }
  }

  @Override
  public ResolveStats getStats() {
    return stats.build();
  }

  private void handleReference(Reference reference) {
    stats.numReferences++;

    var persist = referencedObjectsContext.persist();

    if (reference.deleted()) {
      LOGGER.trace(
          "Skipping deleted reference {} in repository '{}'",
          reference.name(),
          persist.config().repositoryId());
      return;
    }

    LOGGER.debug(
        "Walking reference {} in repository '{}' starting at commit {}",
        reference.name(),
        persist.config().repositoryId(),
        reference.pointer());

    referencedObjectsContext
        .params()
        .relatedObjects()
        .referenceRelatedObjects(reference)
        .forEach(this::pendingObj);

    commitChain(reference.pointer());

    var extendedInfo = reference.extendedInfoObj();
    if (extendedInfo != null) {
      referencedObjectsContext.referencedObjects().markReferenced(extendedInfo);
    }
  }

  private void commitChain(ObjId head) {
    if (EMPTY_OBJ_ID.equals(head)) {
      // Prevent visiting the same commit more often than once
      return;
    }

    stats.numCommitChainHeads++;

    if (referencedObjectsContext.visitedCommitFilter().alreadyVisited(head)) {
      // Prevent visiting the same commit more often than once
      return;
    }

    pendingHeads.addLast(head);
  }

  private void handleCommit(CommitObj commit) {
    stats.numCommits++;

    if (!referencedObjectsContext.visitedCommitFilter().mustVisit(commit.id())) {
      // Prevent visiting the same commit more often than once
      return;
    }

    commitRateLimiter.acquire();

    var persist = referencedObjectsContext.persist();

    LOGGER.debug(
        "Handling commit {} in repository '{}'", commit.id(), persist.config().repositoryId());

    stats.numUniqueCommits++;

    referencedObjectsContext.referencedObjects().markReferenced(commit.id());

    referencedObjectsContext
        .params()
        .relatedObjects()
        .commitRelatedObjects(commit)
        .forEach(this::pendingObj);

    commit
        .referenceIndexStripes()
        .forEach(
            indexStripe ->
                referencedObjectsContext.referencedObjects().markReferenced(indexStripe.segment()));

    if (commit.referenceIndex() != null) {
      pendingObj(commit.referenceIndex());
    }

    var indexesLogic = indexesLogic(referencedObjectsContext.persist());
    var index = indexesLogic.buildCompleteIndexOrEmpty(commit);
    for (StoreIndexElement<CommitOp> indexElement : index) {
      var content = indexElement.content();
      if (content.action().exists()) {
        var value = content.value();
        pendingObj(value);
      }
    }

    commit.secondaryParents().forEach(this::commitChain);
  }

  private void pendingObj(ObjId objId) {
    if (recentObjIds.contains(objId)) {
      return;
    }

    if (!pendingObjs.add(objId)) {
      return;
    }

    stats.numQueuedObjs++;

    if (pendingObjs.size() >= referencedObjectsContext.params().pendingObjsBatchSize()) {
      processPendingObjs();
    }
  }

  private void processPendingObjs() {
    stats.numQueuedObjsBulkFetches++;

    var persist = referencedObjectsContext.persist();

    LOGGER.debug(
        "Fetching {} pending objects in repository '{}'",
        pendingObjs.size(),
        persist.config().repositoryId());

    var objs = persist.fetchObjsIfExist(pendingObjs.toArray(ObjId[]::new));
    // Must clear 'pendingObjs' here, because handleObj can add more objects to it
    pendingObjs.clear();

    for (Obj obj : objs) {
      if (obj != null) {
        handleObj(obj);
      }
    }
  }

  private void handleObj(Obj obj) {
    objRateLimiter.acquire();

    if (!recentObjIds.add(obj.id())) {
      // already handled
      return;
    }

    stats.numObjs++;

    var persist = referencedObjectsContext.persist();

    var objType = obj.type();

    LOGGER.debug(
        "Handling obj {} of type {}/{} in repository '{}'",
        obj.id(),
        objType.name(),
        objType.shortName(),
        persist.config().repositoryId());

    referencedObjectsContext.referencedObjects().markReferenced(obj.id());

    if (VALUE.equals(objType)) {
      var contentValueObj = (ContentValueObj) obj;
      var content =
          DefaultStoreWorker.instance()
              .valueFromStore(contentValueObj.payload(), contentValueObj.data());

      handleContent(content);
    } else if (INDEX_SEGMENTS.equals(objType)) {
      var segments = (IndexSegmentsObj) obj;
      segments
          .stripes()
          .forEach(s -> referencedObjectsContext.referencedObjects().markReferenced(s.segment()));
    }
  }

  private void handleContent(Content content) {
    stats.numContents++;

    referencedObjectsContext
        .params()
        .relatedObjects()
        .contentRelatedObjects(content)
        .forEach(this::pendingObj);
  }
}
