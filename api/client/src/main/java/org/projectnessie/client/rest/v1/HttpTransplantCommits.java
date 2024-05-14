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

import org.projectnessie.api.v1.params.ImmutableTransplant;
import org.projectnessie.client.builder.BaseTransplantCommitsBuilder;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.MergeResponse;

final class HttpTransplantCommits extends BaseTransplantCommitsBuilder {

  private final NessieApiClient client;

  HttpTransplantCommits(NessieApiClient client) {
    this.client = client;
  }

  @Override
  public MergeResponse transplant() throws NessieNotFoundException, NessieConflictException {
    @SuppressWarnings("deprecation")
    ImmutableTransplant.Builder transplant =
        ImmutableTransplant.builder()
            .fromRefName(fromRefName)
            .hashesToTransplant(hashesToTransplant)
            .isDryRun(dryRun)
            .isReturnConflictAsResult(returnConflictAsResult)
            .isFetchAdditionalInfo(fetchAdditionalInfo)
            .keepIndividualCommits(keepIndividualCommits);

    if (defaultMergeMode != null) {
      transplant.defaultKeyMergeMode(defaultMergeMode);
    }

    if (mergeModes != null) {
      transplant.keyMergeModes(mergeModes.values());
    }

    return client
        .getTreeApi()
        .transplantCommitsIntoBranch(branchName, hash, message, transplant.build());
  }
}
