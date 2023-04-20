/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.versioned.storage.versionstore;

import java.util.List;
import java.util.Optional;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Commit;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.MergeResult;
import org.projectnessie.versioned.MetadataRewriter;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.storage.common.logic.CommitRetry.RetryException;

interface Transplant {
  MergeResult<Commit> transplant(
      Optional<?> retryState,
      BranchName sourceBranch,
      List<Hash> sequenceToTransplant,
      MetadataRewriter<CommitMeta> updateCommitMetadata,
      MergeBehaviors mergeBehaviors,
      boolean dryRun)
      throws ReferenceNotFoundException, RetryException, ReferenceConflictException;
}
