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
package org.projectnessie.client.rest.v2;

import java.util.Arrays;
import java.util.Optional;
import org.projectnessie.client.api.AssignBranchBuilder;
import org.projectnessie.client.api.AssignReferenceBuilder;
import org.projectnessie.client.api.AssignTagBuilder;
import org.projectnessie.client.api.CommitMultipleOperationsBuilder;
import org.projectnessie.client.api.CreateReferenceBuilder;
import org.projectnessie.client.api.DeleteBranchBuilder;
import org.projectnessie.client.api.DeleteReferenceBuilder;
import org.projectnessie.client.api.DeleteTagBuilder;
import org.projectnessie.client.api.GetAllReferencesBuilder;
import org.projectnessie.client.api.GetCommitLogBuilder;
import org.projectnessie.client.api.GetContentBuilder;
import org.projectnessie.client.api.GetDiffBuilder;
import org.projectnessie.client.api.GetEntriesBuilder;
import org.projectnessie.client.api.GetReferenceBuilder;
import org.projectnessie.client.api.GetRepositoryConfigBuilder;
import org.projectnessie.client.api.MergeReferenceBuilder;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.client.api.ReferenceHistoryBuilder;
import org.projectnessie.client.api.TransplantCommitsBuilder;
import org.projectnessie.client.api.UpdateRepositoryConfigBuilder;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.NessieConfiguration;
import org.projectnessie.model.Reference;
import org.projectnessie.model.SingleReferenceResponse;

public class HttpApiV2 implements NessieApiV2 {
  private final HttpClient client;

  public HttpApiV2(HttpClient client) {
    this.client = client;
  }

  private volatile boolean didGetConfig;
  private volatile boolean isNessieSpec220;

  boolean isNessieSpec220() {
    if (!didGetConfig) {
      getConfig();
    }
    return isNessieSpec220;
  }

  String toPathString(ContentKey key) {
    return isNessieSpec220() ? key.toPathStringEscaped() : key.toPathString();
  }

  @Override
  public void close() {
    this.client.close();
  }

  @Override
  public <C> Optional<C> unwrapClient(Class<C> clientType) {
    return clientType.isAssignableFrom(HttpClient.class)
        ? Optional.of(clientType.cast(client))
        : Optional.empty();
  }

  @Override
  public NessieConfiguration getConfig() {
    NessieConfiguration config =
        client.newRequest().path("config").get().readEntity(NessieConfiguration.class);
    didGetConfig = true;
    if (config.getSpecVersion() != null) {
      int[] parts =
          Arrays.stream(config.getSpecVersion().split("[.]")).mapToInt(Integer::parseInt).toArray();
      isNessieSpec220 = parts[0] > 2 || (parts[0] == 2 && parts[1] >= 2);
    }
    return config;
  }

  @Override
  public Branch getDefaultBranch() throws NessieNotFoundException {
    return (Branch)
        client
            .newRequest()
            .path("trees/-")
            .unwrap(NessieNotFoundException.class)
            .get()
            .readEntity(SingleReferenceResponse.class)
            .getReference();
  }

  @Override
  public GetContentBuilder getContent() {
    return new HttpGetContent(client, this);
  }

  @Override
  public GetAllReferencesBuilder getAllReferences() {
    return new HttpGetAllReferences(client);
  }

  @Override
  public CreateReferenceBuilder createReference() {
    return new HttpCreateReference(client);
  }

  @Override
  public GetReferenceBuilder getReference() {
    return new HttpGetReference(client);
  }

  @Override
  public ReferenceHistoryBuilder referenceHistory() {
    return new HttpReferenceHistory(client);
  }

  @Override
  public GetEntriesBuilder getEntries() {
    return new HttpGetEntries(client, this);
  }

  @Override
  public GetCommitLogBuilder getCommitLog() {
    return new HttpGetCommitLog(client);
  }

  @Override
  @Deprecated
  public AssignTagBuilder assignTag() {
    return new HttpAssignTag(client);
  }

  @Override
  @Deprecated
  public DeleteTagBuilder deleteTag() {
    return new HttpDeleteTag(client);
  }

  @Override
  @Deprecated
  public AssignBranchBuilder assignBranch() {
    return new HttpAssignBranch(client);
  }

  @Override
  @Deprecated
  public DeleteBranchBuilder deleteBranch() {
    return new HttpDeleteBranch(client);
  }

  @Override
  public AssignReferenceBuilder<Reference> assignReference() {
    return new HttpAssignReference(client);
  }

  @Override
  public DeleteReferenceBuilder<Reference> deleteReference() {
    return new HttpDeleteReference(client);
  }

  @Override
  public TransplantCommitsBuilder transplantCommitsIntoBranch() {
    return new HttpTransplantCommits(client);
  }

  @Override
  public MergeReferenceBuilder mergeRefIntoBranch() {
    return new HttpMergeReference(client);
  }

  @Override
  public CommitMultipleOperationsBuilder commitMultipleOperations() {
    return new HttpCommitMultipleOperations(client);
  }

  @Override
  public GetDiffBuilder getDiff() {
    return new HttpGetDiff(client, this);
  }

  @Override
  public GetRepositoryConfigBuilder getRepositoryConfig() {
    return new HttpGetRepositoryConfig(client);
  }

  @Override
  public UpdateRepositoryConfigBuilder updateRepositoryConfig() {
    return new HttpUpdateRepositoryConfig(client);
  }
}
