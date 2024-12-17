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

import java.util.Set;
import org.projectnessie.nessie.immutables.NessieImmutable;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.persist.ObjId;

@NessieImmutable
public interface CutHistoryScanResult {

  /**
   * The ID of the commit whose parents (both direct parents and merge parents) are intended to be
   * removed.
   *
   * <p>The {@link #affectedCommitIds() "affected"} commits are those commits thar reference this
   * "cut point" in their direct parent {@link CommitObj#tail() "tails"}.
   */
  ObjId cutPoint();

  /**
   * This is the main output of the history scan operation preceding the actual history cutting. It
   * contains IDs of commits, whose {@link CommitObj#tail() parents} need to be rewritten to avoid
   * overlapping the history cut point.
   */
  Set<ObjId> affectedCommitIds();

  /** Number of objects handled while traversing the Nessie commit log. */
  long numScannedObjs();

  static ImmutableCutHistoryScanResult.Builder builder() {
    return ImmutableCutHistoryScanResult.builder();
  }
}
