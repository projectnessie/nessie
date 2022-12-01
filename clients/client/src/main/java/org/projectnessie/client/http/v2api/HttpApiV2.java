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

import org.projectnessie.client.api.AssignBranchBuilder;
import org.projectnessie.client.api.AssignTagBuilder;
import org.projectnessie.client.api.CommitMultipleOperationsBuilder;
import org.projectnessie.client.api.CreateNamespaceBuilder;
import org.projectnessie.client.api.CreateReferenceBuilder;
import org.projectnessie.client.api.DeleteBranchBuilder;
import org.projectnessie.client.api.DeleteNamespaceBuilder;
import org.projectnessie.client.api.DeleteTagBuilder;
import org.projectnessie.client.api.GetAllReferencesBuilder;
import org.projectnessie.client.api.GetCommitLogBuilder;
import org.projectnessie.client.api.GetContentBuilder;
import org.projectnessie.client.api.GetDiffBuilder;
import org.projectnessie.client.api.GetEntriesBuilder;
import org.projectnessie.client.api.GetMultipleNamespacesBuilder;
import org.projectnessie.client.api.GetNamespaceBuilder;
import org.projectnessie.client.api.GetRefLogBuilder;
import org.projectnessie.client.api.GetReferenceBuilder;
import org.projectnessie.client.api.MergeReferenceBuilder;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.client.api.TransplantCommitsBuilder;
import org.projectnessie.client.api.UpdateNamespaceBuilder;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.client.util.v2api.ClientSideCreateNamespace;
import org.projectnessie.client.util.v2api.ClientSideDeleteNamespace;
import org.projectnessie.client.util.v2api.ClientSideGetMultipleNamespaces;
import org.projectnessie.client.util.v2api.ClientSideGetNamespace;
import org.projectnessie.client.util.v2api.ClientSideUpdateNamespace;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.NessieConfiguration;
import org.projectnessie.model.SingleReferenceResponse;

public class HttpApiV2 implements NessieApiV2 {
  private final HttpClient client;

  public HttpApiV2(HttpClient client) {
    this.client = client;
  }

  @Override
  public void close() {
    // nop
  }

  @Override
  public NessieConfiguration getConfig() {
    return client.newRequest().path("config").get().readEntity(NessieConfiguration.class);
  }

  @Override
  public Branch getDefaultBranch() throws NessieNotFoundException {
    return (Branch)
        client
            .newRequest()
            .path("trees/main") // TODO: use trees/-
            .unwrap(NessieNotFoundException.class)
            .get()
            .readEntity(SingleReferenceResponse.class)
            .getReference();
  }

  @Override
  public GetContentBuilder getContent() {
    return new HttpGetContent(client);
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
  public GetEntriesBuilder getEntries() {
    return new HttpGetEntries(client);
  }

  @Override
  public GetCommitLogBuilder getCommitLog() {
    return new HttpGetCommitLog(client);
  }

  @Override
  public AssignTagBuilder assignTag() {
    return new HttpAssignTag(client);
  }

  @Override
  public DeleteTagBuilder deleteTag() {
    return new HttpDeleteTag(client);
  }

  @Override
  public AssignBranchBuilder assignBranch() {
    return new HttpAssignBranch(client);
  }

  @Override
  public DeleteBranchBuilder deleteBranch() {
    return new HttpDeleteBranch(client);
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
    return new HttpGetDiff(client);
  }

  @Override
  public GetRefLogBuilder getRefLog() {
    throw new UnsupportedOperationException("Reflog is not supported in API v2");
  }

  @Override
  public GetNamespaceBuilder getNamespace() {
    return new ClientSideGetNamespace(this);
  }

  @Override
  public GetMultipleNamespacesBuilder getMultipleNamespaces() {
    return new ClientSideGetMultipleNamespaces(this);
  }

  @Override
  public CreateNamespaceBuilder createNamespace() {
    return new ClientSideCreateNamespace(this);
  }

  @Override
  public DeleteNamespaceBuilder deleteNamespace() {
    return new ClientSideDeleteNamespace(this);
  }

  @Override
  public UpdateNamespaceBuilder updateProperties() {
    return new ClientSideUpdateNamespace(this);
  }
}
