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

import static org.projectnessie.versioned.storage.cleanup.PurgeFilter.ReferencedObjectsPurgeFilter.referencedObjectsPurgeFilter;
import static org.projectnessie.versioned.storage.cleanup.ReferencedObjectsContext.objectsResolverContext;

import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;

/**
 * Primary point of entry to remove unreferenced objects from Nessie's backend database.
 *
 * <p>Simplified example code flow: <code><pre>
 * var params =
 *   CleanupParams.builder().build();
 * var cleanup =
 *   createCleanup(params);
 *
 * var referencedObjectsContext =
 *   cleanup.buildReferencedObjectsContext(persist,
 *   TimeUnit.MILLISECONDS.toMicros(
 *     Instant.now().minus(3, ChronoUnit.DAYS)
 *       .toEpochMilli()));
 * var referencedObjectsResolver =
 *   cleanup.createReferencedObjectsResolver(referencedObjectsContext);
 *
 * // Must handle MustRestartWithBiggerFilterException
 * var resolveResult =
 *   referencedObjectsResolver.resolve();
 *
 * var purgeObjects =
 *   cleanup.createPurgeObjects(resolveResult.purgeObjectsContext());
 * var purgeResult =
 *   purgeObjects.purge();
 * </pre></code>
 */
public class Cleanup {
  private final CleanupParams cleanupParams;

  private Cleanup(CleanupParams cleanupParams) {
    this.cleanupParams = cleanupParams;
  }

  public static Cleanup createCleanup(CleanupParams params) {
    return new Cleanup(params);
  }

  /**
   * Create the context holder used when identifying referenced objects and purging unreferenced
   * objects.
   *
   * <p>Choosing an appropriate value for {@code maxObjReferenced} is crucial. Technically, this
   * value must be at max the current timestamp - but logically {@code maxObjReferenced} should be
   * the timestamp of a few days ago to not delete unreferenced objects too early and give users a
   * chance to reset branches to another commit ID in case some table/view metadata is broken.
   *
   * <p>Uses an instance of {@link
   * org.projectnessie.versioned.storage.cleanup.PurgeFilter.ReferencedObjectsPurgeFilter} using a
   * bloom filter based {@link ReferencedObjectsFilter}, both configured using {@link
   * CleanupParams}'s attributes.
   *
   * @param persist the persistence/repository to run against
   * @param maxObjReferencedInMicrosSinceEpoch only {@link Obj}s with a {@link Obj#referenced()}
   *     older than {@code maxObjReferenced} will be deleted. Production workloads should set this
   *     to something like "now minus 7 days" to have the chance to reset branches, just in case.
   *     Technically, this value must not be greater than "now". "Now" should be inquired using
   *     {@code Persist.config().clock().instant()}.
   */
  public ReferencedObjectsContext buildReferencedObjectsContext(
      Persist persist, long maxObjReferencedInMicrosSinceEpoch) {
    var referencedObjects = new ReferencedObjectsFilterImpl(cleanupParams);
    var purgeFilter =
        referencedObjectsPurgeFilter(referencedObjects, maxObjReferencedInMicrosSinceEpoch);
    return objectsResolverContext(persist, cleanupParams, referencedObjects, purgeFilter);
  }

  /**
   * Creates a new objects-resolver instance to identify <em>referenced</em> objects, which must be
   * retained.
   *
   * @param objectsResolverContext context, preferably created using {@link
   *     #buildReferencedObjectsContext(Persist, long)}
   */
  public ReferencedObjectsResolver createReferencedObjectsResolver(
      ReferencedObjectsContext objectsResolverContext) {
    return new ReferencedObjectsResolverImpl(
        objectsResolverContext, cleanupParams.rateLimitFactory());
  }

  /**
   * Creates a new objects-purger instance to delete <em>unreferenced</em> objects.
   *
   * @param purgeObjectsContext return value of {@link ReferencedObjectsResolver#resolve()}.
   */
  public PurgeObjects createPurgeObjects(PurgeObjectsContext purgeObjectsContext) {
    return new PurgeObjectsImpl(purgeObjectsContext, cleanupParams.rateLimitFactory());
  }

  public CutHistoryParams buildCutHistoryParams(Persist persist, ObjId newRootCommitId) {
    return CutHistoryParams.cutHistoryParams(
        persist,
        newRootCommitId,
        cleanupParams.resolveCommitRatePerSecond(),
        cleanupParams.dryRun());
  }

  public CutHistory createCutHistory(CutHistoryParams context) {
    return new CutHistoryImpl(context, cleanupParams.rateLimitFactory());
  }
}
