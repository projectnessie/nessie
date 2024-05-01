/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie.client.rest.v1;

import org.projectnessie.api.v1.params.ImmutableMerge;
import org.projectnessie.client.api.MergeReferenceBuilder;
import org.projectnessie.client.builder.BaseMergeReferenceBuilder;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.MergeResponse;

final class HttpMergeReference extends BaseMergeReferenceBuilder {

  private final NessieApiClient client;

  HttpMergeReference(NessieApiClient client) {
    this.client = client;
  }

  @Override
  public MergeReferenceBuilder message(String message) {
    throw new UnsupportedOperationException("Merge message overrides are not supported in API v1.");
  }

  @Override
  public MergeReferenceBuilder commitMeta(CommitMeta commitMeta) {
    throw new UnsupportedOperationException(
        "Merge commit-meta overrides are not supported in API v1.");
  }

  @Override
  public MergeResponse merge() throws NessieNotFoundException, NessieConflictException {
    @SuppressWarnings("deprecation")
    ImmutableMerge.Builder merge =
        ImmutableMerge.builder()
            .fromHash(fromHash)
            .fromRefName(fromRefName)
            .isDryRun(dryRun)
            .isReturnConflictAsResult(returnConflictAsResult)
            .isFetchAdditionalInfo(fetchAdditionalInfo)
            .keepIndividualCommits(keepIndividualCommits);

    if (defaultMergeMode != null) {
      merge.defaultKeyMergeMode(defaultMergeMode);
    }

    if (mergeModes != null) {
      merge.keyMergeModes(mergeModes.values());
    }

    return client.getTreeApi().mergeRefIntoBranch(branchName, hash, merge.build());
  }
}
