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

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.projectnessie.versioned.storage.common.persist.CloseableIterator;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class PurgeObjectsImpl implements PurgeObjects {
  private static final Logger LOGGER = LoggerFactory.getLogger(PurgeObjectsImpl.class);

  private final PurgeObjectsContext purgeObjectsContext;
  private final PurgeStatsBuilder stats;
  private final AtomicBoolean used = new AtomicBoolean();

  public PurgeObjectsImpl(PurgeObjectsContext purgeObjectsContext) {
    this.purgeObjectsContext = purgeObjectsContext;
    this.stats = new PurgeStatsBuilder();
  }

  @Override
  public PurgeResult purge() {
    checkState(used.compareAndSet(false, true), "resolve() has already been called.");

    var scanRateLimiter = RateLimit.create(purgeObjectsContext.scanObjRatePerSecond());
    var purgeRateLimiter = RateLimit.create(purgeObjectsContext.deleteObjRatePerSecond());
    var purgeFilter = purgeObjectsContext.purgeFilter();
    var persist = purgeObjectsContext.persist();
    var clock = persist.config().clock();

    LOGGER.debug(
        "Purging unreferenced objects in repository '{}', scanning {} objects per second, deleting {} objects per second",
        persist.config().repositoryId(),
        scanRateLimiter,
        purgeRateLimiter);

    stats.started = clock.instant();

    try (CloseableIterator<Obj> iter = persist.scanAllObjects(Set.of())) {
      scanRateLimiter.acquire();

      while (iter.hasNext()) {
        stats.numScannedObjs++;
        var obj = iter.next();
        if (purgeFilter.mustKeep(obj)) {
          continue;
        }

        purgeRateLimiter.acquire();
        purgeObj(obj);
      }
    } catch (RuntimeException e) {
      stats.failure = e;
      throw e;
    } finally {
      stats.ended = clock.instant();
      LOGGER.debug(
          "Finished purging unreferenced objects in repository '{}', {} of {} objects deleted",
          persist.config().repositoryId(),
          stats.numScannedObjs,
          stats.numPurgedObjs);
    }

    return ImmutablePurgeResult.of(stats.build());
  }

  @Override
  public PurgeStats getStats() {
    return stats.build();
  }

  @Override
  public long estimatedHeapPressure() {
    return purgeObjectsContext.referencedObjects().estimatedHeapPressure();
  }

  private void purgeObj(Obj obj) {
    // TODO delete in parallel (multiple threads)
    stats.numPurgedObjs++;

    LOGGER.trace(
        "Deleting obj {} of type {}/{}", obj.id(), obj.type().name(), obj.type().shortName());

    if (!purgeObjectsContext.dryRun()) {
      purgeObjectsContext.persist().deleteWithReferenced(obj);
    }
  }
}
