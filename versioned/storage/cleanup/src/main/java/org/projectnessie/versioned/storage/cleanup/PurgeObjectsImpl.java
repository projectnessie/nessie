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

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntFunction;
import org.projectnessie.versioned.storage.common.persist.CloseableIterator;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class PurgeObjectsImpl implements PurgeObjects {
  private static final Logger LOGGER = LoggerFactory.getLogger(PurgeObjectsImpl.class);

  private final PurgeObjectsContext purgeObjectsContext;
  private final PurgeStatsBuilder stats;
  private final AtomicBoolean used = new AtomicBoolean();
  private final RateLimit scanRateLimiter;
  private final RateLimit purgeRateLimiter;

  public PurgeObjectsImpl(
      PurgeObjectsContext purgeObjectsContext, IntFunction<RateLimit> rateLimitIntFunction) {
    this.purgeObjectsContext = purgeObjectsContext;
    this.stats = new PurgeStatsBuilder();
    this.scanRateLimiter = rateLimitIntFunction.apply(purgeObjectsContext.scanObjRatePerSecond());
    this.purgeRateLimiter =
        rateLimitIntFunction.apply(purgeObjectsContext.deleteObjRatePerSecond());
  }

  @Override
  public PurgeResult purge() {
    checkState(used.compareAndSet(false, true), "purge() has already been called.");

    var purgeFilter = purgeObjectsContext.purgeFilter();
    var persist = purgeObjectsContext.persist();
    var clock = persist.config().clock();

    LOGGER.info(
        "Purging unreferenced objects in repository '{}', scanning {} objects per second, deleting {} objects per second, estimated context heap pressure: {}",
        persist.config().repositoryId(),
        scanRateLimiter,
        purgeRateLimiter,
        memSizeToStringMB(estimatedHeapPressure()));

    PurgeStats finalStats = null;
    try {
      stats.started = clock.instant();
      try (CloseableIterator<Obj> iter = persist.scanAllObjects(Set.of())) {
        while (iter.hasNext()) {
          scanRateLimiter.acquire();
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
      } finally {
        stats.ended = clock.instant();
        finalStats = stats.build();
      }

      LOGGER.info(
          "Successfully finished purging unreferenced objects after {} in repository '{}', purge stats: {}, estimated context heap pressure: {}",
          finalStats.duration(),
          persist.config().repositoryId(),
          finalStats,
          memSizeToStringMB(estimatedHeapPressure()));
    } catch (RuntimeException e) {
      if (finalStats != null) {
        LOGGER.warn(
            "Error while purging unreferenced objects after {} in repository '{}', purge stats: {}, estimated context heap pressure: {}",
            finalStats.duration(),
            persist.config().repositoryId(),
            finalStats,
            memSizeToStringMB(estimatedHeapPressure()),
            e);
      } else {
        LOGGER.warn(
            "Error while purging unreferenced objects in repository '{}'",
            persist.config().repositoryId(),
            stats.failure);
      }
      throw e;
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

    var persist = purgeObjectsContext.persist();

    var objType = obj.type();
    LOGGER.trace(
        "Deleting obj {} of type {}/{} in repository '{}'",
        obj.id(),
        objType.name(),
        objType.shortName(),
        persist.config().repositoryId());

    if (!purgeObjectsContext.dryRun()) {
      persist.deleteWithReferenced(obj);
    }
  }
}
