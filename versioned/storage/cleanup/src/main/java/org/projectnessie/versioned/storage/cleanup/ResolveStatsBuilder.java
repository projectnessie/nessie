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

import java.time.Instant;
import java.util.Optional;

final class ResolveStatsBuilder {
  Instant started;
  Instant ended;

  boolean mustRestart;
  Exception failure;

  long numReferences;
  long numCommits;
  long numUniqueCommits;
  long numPendingCommits;
  long numPendingObjs;
  long numObjs;
  long numContents;
  long numPendingCommitBulkFetches;
  long numPendingObjsBulkFetches;

  ResolveStats build() {
    return ImmutableResolveStats.of(
        started,
        ended,
        mustRestart,
        Optional.ofNullable(failure),
        numReferences,
        numCommits,
        numUniqueCommits,
        numPendingCommits,
        numPendingObjs,
        numObjs,
        numContents,
        numPendingCommitBulkFetches,
        numPendingObjsBulkFetches);
  }
}
