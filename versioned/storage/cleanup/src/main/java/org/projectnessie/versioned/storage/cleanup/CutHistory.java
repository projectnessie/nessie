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

import org.projectnessie.versioned.storage.common.objtypes.CommitObj;

/**
 * Interface to cut the Nessie commit log at a certain point by remoting its parents. The cut point
 * is provided by {@link CutHistoryParams#cutPoint()}.
 *
 * <p>All references (branches and tags) that contain the cut point will be affected. References
 * that lose common ancestors with another branch will lose be ability to merge into that branch.
 *
 * <p>This operation is generally meaningful when followed by {@link PurgeObjects}, which will
 * removed pieces of the commit graph that are no longer reachable from reference HEADs.
 *
 * @see Cleanup#createCutHistory(CutHistoryParams)
 * @see Cleanup#createPurgeObjects(PurgeObjectsContext)
 */
public interface CutHistory {
  /**
   * Identifies commits, whose logical correctness would be affected by the {@link
   * #cutHistory(CutHistoryScanResult)} operation. These commits have lists {@link CommitObj#tail()
   * parent} that stretch beyond the cut point.
   *
   * <p>Note: this operation may be time-consuming.
   */
  CutHistoryScanResult identifyAffectedCommits();

  /**
   * Rewrites commits identifies by the {@link CutHistoryScanResult} parameter.
   *
   * <p>First, commits identified by {@link CutHistoryScanResult#affectedCommitIds()} are rewritten
   * to shorten their {@link CommitObj#tail() parent} lists so that they will not overlap the cut
   * point.
   *
   * <p>Second, the commit marked as the history {@link CutHistoryScanResult#affectedCommitIds() cut
   * point} is rewritten to remove all its parents (direct and merge parents). The rewritten commit
   * will effectively become a "root" commit. The message of the rewritten "cut point" commit is
   * updated to indicate that its parents got removed.
   *
   * <p>Note: this operation is idempotent.
   */
  CutHistoryResult cutHistory(CutHistoryScanResult scanResult);
}
