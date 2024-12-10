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

import static org.projectnessie.versioned.storage.cleanup.ImmutableCutHistoryScanResult.*;
import static org.projectnessie.versioned.storage.common.logic.Logics.commitLogic;
import static org.projectnessie.versioned.storage.common.objtypes.StandardObjType.COMMIT;

import java.util.Collections;
import java.util.List;
import java.util.function.IntFunction;
import org.projectnessie.versioned.storage.common.logic.CommitLogic;
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

    long numObjects = 0;
    // Identify commits, whose "tails" overlap the cut point.
    // scanAllCommitLogEntries() returns all commits in no specific order, parents may be scanned
    // before or after their children.
    try (CloseableIterator<Obj> scan = persist.scanAllObjects(Collections.singleton(COMMIT))) {
      while (scan.hasNext()) {
        scanRateLimiter.acquire();

        CommitObj commit = (CommitObj) scan.next();
        numObjects++;
        if (commit.tail().contains(cutPoint)) {
          result.addAffectedCommitId(commit.id());
        }
      }

      result.numScannedObjs(numObjects);
      return result.build();
    }
  }

  @Override
  public UpdateParentsResult rewriteParents(CutHistoryScanResult scanResult) {
    ImmutableUpdateParentsResult.Builder result = UpdateParentsResult.builder();
    CommitLogic commitLogic = commitLogic(context.persist());
    for (ObjId id : scanResult.affectedCommitIds()) {
      try {
        CommitObj commitObj = commitLogic.fetchCommit(id);
        if (commitObj == null) {
          result.putFailure(id, new IllegalAccessException("Commit not found: " + id));
          continue;
        }

        if (commitObj.tail().isEmpty()) {
          continue;
        }

        CommitObj updated =
            CommitObj.commitBuilder()
                .from(commitObj)
                .tail(commitObj.tail().subList(0, 1)) // one direct parent for simplicity
                .build();

        if (!context.dryRun()) {
          context.persist().upsertObj(updated);
        }

      } catch (Exception e) {
        result.putFailure(id, e);
      }
    }
    return result.build();
  }

  @Override
  public UpdateParentsResult cutHistory() {
    ObjId id = context.cutPoint();
    ImmutableUpdateParentsResult.Builder result = UpdateParentsResult.builder();
    CommitLogic commitLogic = commitLogic(context.persist());
    try {
      CommitObj commitObj = commitLogic.fetchCommit(id);
      if (commitObj == null) {
        result.putFailure(id, new IllegalAccessException("Commit not found: " + id));
      } else {
        CommitObj updated =
            CommitObj.commitBuilder()
                .from(commitObj)
                .secondaryParents(List.of())
                .tail(List.of())
                .build();

        if (!context.dryRun()) {
          context.persist().upsertObj(updated);
        }
      }
    } catch (Exception e) {
      result.putFailure(id, e);
    }
    return result.build();
  }
}
