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
package org.projectnessie.nessie.combined;

import org.projectnessie.api.v2.TreeApi;
import org.projectnessie.api.v2.params.ImmutableTransplant;
import org.projectnessie.client.api.TransplantCommitsBuilder;
import org.projectnessie.client.builder.BaseTransplantCommitsBuilder;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.MergeResponse;
import org.projectnessie.model.Reference;

final class CombinedTransplantCommits extends BaseTransplantCommitsBuilder {
  private final TreeApi treeApi;

  public CombinedTransplantCommits(TreeApi treeApi) {
    this.treeApi = treeApi;
  }

  @Override
  public TransplantCommitsBuilder keepIndividualCommits(boolean keepIndividualCommits) {
    if (!keepIndividualCommits) {
      throw new IllegalArgumentException(
          "Individual commits are always kept during transplant operations.");
    }
    return this;
  }

  @Override
  public MergeResponse transplant() throws NessieNotFoundException, NessieConflictException {
    ImmutableTransplant.Builder transplant =
        ImmutableTransplant.builder()
            .message(message)
            .fromRefName(fromRefName)
            .hashesToTransplant(hashesToTransplant)
            .isDryRun(dryRun)
            .isReturnConflictAsResult(returnConflictAsResult)
            .isFetchAdditionalInfo(fetchAdditionalInfo);

    if (defaultMergeMode != null) {
      transplant.defaultKeyMergeMode(defaultMergeMode);
    }

    if (mergeModes != null) {
      transplant.keyMergeModes(mergeModes.values());
    }

    try {
      return treeApi.transplantCommitsIntoBranch(
          Reference.toPathString(branchName, hash), transplant.build());
    } catch (RuntimeException e) {
      throw CombinedClientImpl.maybeWrapException(e);
    }
  }
}
