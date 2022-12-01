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
package org.projectnessie.client.http.v2api;

import org.projectnessie.api.v2.params.ImmutableMerge;
import org.projectnessie.client.api.MergeReferenceBuilder;
import org.projectnessie.client.builder.BaseMergeReferenceBuilder;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.MergeResponse;
import org.projectnessie.model.Reference;

final class HttpMergeReference extends BaseMergeReferenceBuilder {
  private final HttpClient client;

  public HttpMergeReference(HttpClient client) {
    this.client = client;
  }

  @Override
  public MergeReferenceBuilder keepIndividualCommits(boolean keepIndividualCommits) {
    if (keepIndividualCommits) {
      throw new IllegalArgumentException("Commits are always squashed during merge operations.");
    }
    return this;
  }

  @Override
  public MergeResponse merge() throws NessieNotFoundException, NessieConflictException {
    ImmutableMerge merge =
        ImmutableMerge.builder()
            .fromHash(fromHash)
            .fromRefName(fromRefName)
            .isDryRun(dryRun)
            .isFetchAdditionalInfo(fetchAdditionalInfo)
            .isReturnConflictAsResult(returnConflictAsResult)
            .build(); // TODO: message
    return client
        .newRequest()
        .path("trees/{ref}/history/merge")
        .resolveTemplate("ref", Reference.toPathString(branchName, hash))
        .unwrap(NessieNotFoundException.class, NessieConflictException.class)
        .post(merge)
        .readEntity(MergeResponse.class);
  }
}
