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
package org.projectnessie.client.http.v1api;

import org.projectnessie.client.api.AssignBranchBuilder;
import org.projectnessie.client.api.AssignTagBuilder;
import org.projectnessie.client.api.CommitMultipleOperationsBuilder;
import org.projectnessie.client.api.CreateReferenceBuilder;
import org.projectnessie.client.api.DeleteBranchBuilder;
import org.projectnessie.client.api.DeleteTagBuilder;
import org.projectnessie.client.api.GetAllReferencesBuilder;
import org.projectnessie.client.api.GetCommitLogBuilder;
import org.projectnessie.client.api.GetContentsBuilder;
import org.projectnessie.client.api.GetEntriesBuilder;
import org.projectnessie.client.api.GetReferenceBuilder;
import org.projectnessie.client.api.MergeReferenceBuilder;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.api.TransplantCommitsBuilder;
import org.projectnessie.client.http.NessieApiClient;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.NessieConfiguration;

public final class HttpApiV1 implements NessieApiV1 {

  private final NessieApiClient client;

  public HttpApiV1(NessieApiClient client) {
    this.client = client;
  }

  @Override
  public void close() {
    client.close();
  }

  @Override
  public NessieConfiguration getConfig() {
    return client.getConfigApi().getConfig();
  }

  @Override
  public Branch getDefaultBranch() throws NessieNotFoundException {
    return client.getTreeApi().getDefaultBranch();
  }

  @Override
  public GetContentsBuilder getContents() {
    return new HttpGetContents(client);
  }

  @Override
  public GetAllReferencesBuilder getAllReferences() {
    return new HttpGetAllReferences(client);
  }

  @Override
  public GetReferenceBuilder getReference() {
    return new HttpGetReference(client);
  }

  @Override
  public CreateReferenceBuilder createReference() {
    return new HttpCreateReference(client);
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
}
