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
import static org.projectnessie.versioned.storage.cleanup.ImmutableCutHistoryScanResult.Builder;
import static org.projectnessie.versioned.storage.common.logic.Logics.commitLogic;
import static org.projectnessie.versioned.storage.common.objtypes.StandardObjType.COMMIT;
import static org.projectnessie.versioned.storage.common.persist.ObjId.EMPTY_OBJ_ID;

import java.util.Collections;
import java.util.List;
import java.util.function.IntFunction;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.persist.CloseableIterator;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;

public class CutHistoryImpl implements CutHistory {
  private final CutHistoryParams context;
  private final RateLimit scanRateLimiter;

  public CutHistoryImpl(CutHistoryParams context, IntFunction<RateLimit> rateLimitIntFunction) {
    this.context = context;
    this.scanRateLimiter = rateLimitIntFunction.apply(context.scanObjRatePerSecond());
  }

  @Override
  public CutHistoryScanResult identifyAffectedCommits() {
    Builder result = CutHistoryScanResult.builder();

    Persist persist = context.persist();
    ObjId cutPoint = context.cutPoint();
    result.cutPoint(cutPoint);

    long numObjects = 0;
    // Identify commits, whose "tails" overlap the cut point.
    // scanAllCommitLogEntries() returns all commits in no specific order, parents may be scanned
    // before or after their children.
    try (CloseableIterator<Obj> scan = persist.scanAllObjects(Collections.singleton(COMMIT))) {
      while (scan.hasNext()) {
        scanRateLimiter.acquire();

        CommitObj commit = (CommitObj) scan.next();
        numObjects++;

        var tail = commit.tail();
        var cutPointIdx = tail.indexOf(cutPoint);
        // Commits referencing the cut point as their last parent do not need to be rewritten
        if (cutPointIdx >= 0 && cutPointIdx < tail.size() - 1) {
          result.addAffectedCommitId(commit.id());
        }
      }

      result.numScannedObjs(numObjects);
      return result.build();
    }
  }

  @Override
  public CutHistoryResult cutHistory(CutHistoryScanResult scanResult) {
    var result = CutHistoryResult.builder().input(scanResult);
    var commitLogic = commitLogic(context.persist());
    var cutPoint = context.cutPoint();

    result.wasHistoryCut(false);
    var failed = false;
    for (ObjId id : scanResult.affectedCommitIds()) {
      try {
        var commitObj = commitLogic.fetchCommit(id);
        checkState(commitObj != null, "Commit not found: %s", id);

        if (commitObj.tail().size() <= 1) {
          continue;
        }

        var tail = commitObj.tail();
        var cutPointIdx = tail.indexOf(cutPoint);

        if (cutPointIdx < 0 || cutPointIdx == tail.size() - 1) {
          // may happen on re-invocation
          continue;
        }

        var updatedCommitObj =
            CommitObj.commitBuilder()
                .from(commitObj)
                .tail(commitObj.tail().subList(0, cutPointIdx + 1))
                .build();

        if (!context.dryRun()) {
          context.persist().upsertObj(updatedCommitObj);
          result.addRewrittenCommitId(id);
        }
      } catch (Exception e) {
        result.putFailure(id, e);
        failed = true;
      }
    }

    // Do not attempt cutting history if the affected commit tails could not be rewritten
    if (failed) {
      return result.build();
    }

    try {
      var commitObj = commitLogic.fetchCommit(cutPoint);
      checkState(commitObj != null, "Commit not found: %s", cutPoint);

      if (!EMPTY_OBJ_ID.equals(commitObj.directParent())
          || !commitObj.secondaryParents().isEmpty()) {
        var msg =
            String.format(
                "%s [updated to remove parents on %s]",
                commitObj.message(), context.persist().config().clock().instant());
        var updatedCommitObj =
            CommitObj.commitBuilder()
                .from(commitObj)
                .message(msg)
                .secondaryParents(List.of())
                .tail(List.of(EMPTY_OBJ_ID))
                .build();

        if (!context.dryRun()) {
          context.persist().upsertObj(updatedCommitObj);
          result.addRewrittenCommitId(cutPoint);
          result.wasHistoryCut(true);
        }
      }
    } catch (Exception e) {
      result.putFailure(cutPoint, e);
    }

    return result.build();
  }
}
