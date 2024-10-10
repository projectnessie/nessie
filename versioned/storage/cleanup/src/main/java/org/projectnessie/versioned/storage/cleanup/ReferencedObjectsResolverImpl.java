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
import static org.projectnessie.versioned.storage.cleanup.PurgeObjectsContext.purgeObjectsContext;
import static org.projectnessie.versioned.storage.common.logic.CommitLogQuery.commitLogQuery;
import static org.projectnessie.versioned.storage.common.logic.Logics.commitLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.indexesLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.referenceLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.repositoryLogic;
import static org.projectnessie.versioned.storage.common.logic.ReferencesQuery.referencesQuery;
import static org.projectnessie.versioned.storage.common.objtypes.StandardObjType.COMMIT;
import static org.projectnessie.versioned.storage.common.objtypes.StandardObjType.VALUE;

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.atomic.AtomicBoolean;
import org.agrona.collections.ObjectHashSet;
import org.projectnessie.model.Content;
import org.projectnessie.versioned.storage.common.indexes.StoreIndexElement;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.objtypes.CommitOp;
import org.projectnessie.versioned.storage.common.objtypes.ContentValueObj;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Reference;
import org.projectnessie.versioned.store.DefaultStoreWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ReferencedObjectsResolverImpl implements ReferencedObjectsResolver {
  private static final Logger LOGGER = LoggerFactory.getLogger(ReferencedObjectsResolverImpl.class);

  private final ObjectHashSet<ObjId> pendingObjs = new ObjectHashSet<>();
  private final ObjectHashSet<ObjId> pendingCommits = new ObjectHashSet<>();

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

  ReferencedObjectsResolverImpl(ReferencedObjectsContext referencedObjectsContext) {
    this.referencedObjectsContext = referencedObjectsContext;
    this.stats = new ResolveStatsBuilder();
    this.commitRateLimiter =
        RateLimit.create(referencedObjectsContext.params().resolveCommitRatePerSecond());
    this.objRateLimiter =
        RateLimit.create(referencedObjectsContext.params().resolveObjRatePerSecond());
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

    LOGGER.debug(
        "Identifying referenced objects in repository '{}', processing {} commits per second, processing {} objects per second",
        referencedObjectsContext.persist().config().repositoryId(),
        commitRateLimiter,
        objRateLimiter);

    var persist = referencedObjectsContext.persist();
    var clock = persist.config().clock();
    var params = referencedObjectsContext.params();

    stats.started = clock.instant();
    try {

      checkState(
          repositoryLogic(persist).repositoryExists(),
          "The provided repository has not been initialized.");

      params.relatedObjects().repositoryRelatedObjects().forEach(this::pendingObj);

      for (String internalReferenceName : params.internalReferenceNames()) {
        var intRef = persist.fetchReference(internalReferenceName);
        checkState(intRef != null, "Internal reference %s not found!", internalReferenceName);
        walkReference(intRef);
      }

      var referenceLogic = referenceLogic(persist);
      referenceLogic.queryReferences(referencesQuery()).forEachRemaining(this::walkReference);

      while (!pendingCommits.isEmpty() || !pendingObjs.isEmpty()) {
        if (!pendingCommits.isEmpty()) {
          processPendingCommits();
        }
        if (!pendingObjs.isEmpty()) {
          processPendingObjs();
        }
      }
    } catch (MustRestartWithBiggerFilterException mustRestart) {
      LOGGER.warn(
          "Must restart identifying referenced objects for repository '{}', current parameters: expected object count: {}, FPP: {}, allowed FPP: {}",
          persist.config().repositoryId(),
          params.expectedObjCount(),
          params.falsePositiveProbability(),
          params.allowedFalsePositiveProbability());
      stats.mustRestart = true;
      stats.failure = mustRestart;
      throw mustRestart;
    } catch (RuntimeException e) {
      stats.failure = e;
      throw e;
    } finally {
      stats.ended = clock.instant();
      LOGGER.debug(
          "Finished identifying referenced objects in repository '{}', processed {} commits in {} references over {} objects",
          persist.config().repositoryId(),
          stats.numCommits,
          stats.numReferences,
          stats.numObjs);
    }

    return ImmutableResolveResult.of(stats.build(), purgeObjectsContext(referencedObjectsContext));
  }

  @Override
  public ResolveStats getStats() {
    return stats.build();
  }

  @VisibleForTesting
  void walkReference(Reference reference) {
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

    commitLogic(persist)
        .commitLog(commitLogQuery(reference.pointer()))
        .forEachRemaining(this::handleCommit);

    var extendedInfo = reference.extendedInfoObj();
    if (extendedInfo != null) {
      referencedObjectsContext.referencedObjects().markReferenced(extendedInfo);
    }
  }

  @VisibleForTesting
  void handleCommit(CommitObj commit) {
    if (referencedObjectsContext.visitedCommitFilter().visited(commit.id())) {
      // Prevent visiting the same commit more often than once
      return;
    }

    commitRateLimiter.acquire();

    stats.numCommits++;

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

    var indexesLogic = indexesLogic(referencedObjectsContext.persist());
    var index = indexesLogic.buildCompleteIndexOrEmpty(commit);
    for (StoreIndexElement<CommitOp> indexElement : index) {
      var content = indexElement.content();
      if (content.action().exists()) {
        var value = content.value();
        pendingObj(value);
      }
    }

    commit.secondaryParents().forEach(this::pendingCommit);
  }

  private void pendingCommit(ObjId commitObjId) {
    if (referencedObjectsContext.visitedCommitFilter().visited(commitObjId)) {
      // Prevent visiting the same commit more often than once
      return;
    }

    if (!pendingCommits.add(commitObjId)) {
      return;
    }

    stats.numPendingCommits++;

    if (pendingCommits.size() >= referencedObjectsContext.params().pendingObjsBatchSize()) {
      processPendingCommits();
    }
  }

  @VisibleForTesting
  void processPendingCommits() {
    stats.numPendingCommitBulkFetches++;

    var persist = referencedObjectsContext.persist();

    LOGGER.trace(
        "Fetching {} pending commits in repository '{}'",
        pendingCommits.size(),
        persist.config().repositoryId());

    var commits =
        persist.fetchTypedObjsIfExist(
            pendingCommits.toArray(ObjId[]::new), COMMIT, CommitObj.class);

    for (CommitObj commit : commits) {
      handleCommit(commit);
    }

    pendingCommits.clear();
  }

  private void pendingObj(ObjId objId) {
    if (recentObjIds.contains(objId)) {
      return;
    }

    if (!pendingObjs.add(objId)) {
      return;
    }

    stats.numPendingObjs++;

    if (pendingObjs.size() >= referencedObjectsContext.params().pendingObjsBatchSize()) {
      processPendingObjs();
    }
  }

  @VisibleForTesting
  void processPendingObjs() {
    stats.numPendingObjsBulkFetches++;

    var persist = referencedObjectsContext.persist();

    LOGGER.debug(
        "Fetching {} pending objects in repository '{}'",
        pendingObjs.size(),
        persist.config().repositoryId());

    var objs = persist.fetchObjsIfExist(pendingObjs.toArray(ObjId[]::new));

    for (Obj obj : objs) {
      if (obj != null) {
        handleObj(obj);
      }
    }

    pendingObjs.clear();
  }

  @VisibleForTesting
  void handleObj(Obj obj) {
    objRateLimiter.acquire();

    if (!recentObjIds.add(obj.id())) {
      // already handled
      return;
    }

    stats.numObjs++;

    var persist = referencedObjectsContext.persist();

    LOGGER.debug(
        "Handling obj {} of type {}/{} in repository '{}'",
        obj.id(),
        obj.type().name(),
        obj.type().shortName(),
        persist.config().repositoryId());

    referencedObjectsContext.referencedObjects().markReferenced(obj.id());

    var type = obj.type();

    if (VALUE.equals(type)) {
      var contentValueObj = (ContentValueObj) obj;
      var content =
          DefaultStoreWorker.instance()
              .valueFromStore(contentValueObj.payload(), contentValueObj.data());

      handleContent(content);
    }
  }

  @VisibleForTesting
  void handleContent(Content content) {
    stats.numContents++;

    referencedObjectsContext
        .params()
        .relatedObjects()
        .contentRelatedObjects(content)
        .forEach(this::pendingObj);
  }
}
