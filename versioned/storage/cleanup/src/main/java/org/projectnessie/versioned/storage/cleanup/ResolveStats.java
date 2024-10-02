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
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
public interface ResolveStats {
  Instant started();

  Instant ended();

  boolean mustRestart();

  Optional<Exception> failure();

  /** Number of processed references, including Nessie internal references. */
  long numReferences();

  /** Number of processed commit objects, including Nessie internal commits. */
  long numCommits();

  /** Number of processed unique commit objects, including Nessie internal commits. */
  long numUniqueCommits();

  /** Number of commit objects that had been queued for batched commit object handling. */
  long numPendingCommits();

  /** Number of non-commit objects that had been queued for batched commit object handling. */
  long numPendingObjs();

  /** Number of non-commit objects. */
  long numObjs();

  /** Number of {@link org.projectnessie.model.Content} objects. */
  long numContents();

  /** Number of bulk commit object fetches. */
  long numPendingCommitBulkFetches();

  /** Number of bulk non-commit object fetches. */
  long numPendingObjsBulkFetches();
}
