/*
 * Copyright (C) 2023 Dremio
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

import org.projectnessie.api.v2.ConfigApi;
import org.projectnessie.api.v2.TreeApi;
import org.projectnessie.api.v2.params.GetReferenceParams;
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
import org.projectnessie.error.ErrorCode;
import org.projectnessie.error.ImmutableNessieError;
import org.projectnessie.error.NessieBadRequestException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.NessieConfiguration;
import org.projectnessie.model.Reference;

final class CombinedClientImpl implements NessieApiV2 {

  private final ConfigApi configApi;
  private final TreeApi treeApi;

  CombinedClientImpl(ConfigApi configApi, TreeApi treeApi) {
    this.configApi = configApi;
    this.treeApi = treeApi;
  }

  static RuntimeException maybeWrapException(RuntimeException e) {
    if (e instanceof IllegalArgumentException) {
      NessieBadRequestException ex =
          new NessieBadRequestException(
              ImmutableNessieError.builder()
                  .errorCode(ErrorCode.BAD_REQUEST)
                  .message(e.getMessage())
                  .status(400)
                  .reason("Bad request")
                  .build());
      ex.initCause(e);
      return ex;
    }
    return e;
  }

  @Override
  public void close() {}

  @Override
  public NessieConfiguration getConfig() {
    return configApi.getConfig();
  }

  @Override
  public Branch getDefaultBranch() throws NessieNotFoundException {
    return (Branch)
        treeApi.getReferenceByName(GetReferenceParams.builder().ref("-").build()).getReference();
  }

  @Override
  public GetContentBuilder getContent() {
    return new CombinedGetContent(treeApi);
  }

  @Override
  public GetAllReferencesBuilder getAllReferences() {
    return new CombinedGetAllReferences(treeApi);
  }

  @Override
  public CreateReferenceBuilder createReference() {
    return new CombinedCreateReference(treeApi);
  }

  @Override
  public GetReferenceBuilder getReference() {
    return new CombinedGetReference(treeApi);
  }

  @Override
  public ReferenceHistoryBuilder referenceHistory() {
    return new CombinedReferenceHistory(treeApi);
  }

  @Override
  public GetEntriesBuilder getEntries() {
    return new CombinedGetEntries(treeApi);
  }

  @Override
  public GetCommitLogBuilder getCommitLog() {
    return new CombinedGetCommitLog(treeApi);
  }

  @Override
  public TransplantCommitsBuilder transplantCommitsIntoBranch() {
    return new CombinedTransplantCommits(treeApi);
  }

  @Override
  public MergeReferenceBuilder mergeRefIntoBranch() {
    return new CombinedMergeReference(treeApi);
  }

  @Override
  public CommitMultipleOperationsBuilder commitMultipleOperations() {
    return new CombinedCommitMultipleOperations(treeApi);
  }

  @Override
  public GetDiffBuilder getDiff() {
    return new CombinedGetDiff(treeApi);
  }

  @Override
  public GetRepositoryConfigBuilder getRepositoryConfig() {
    return new CombinedGetRepositoryConfig(configApi);
  }

  @Override
  public UpdateRepositoryConfigBuilder updateRepositoryConfig() {
    return new CombinedUpdateRepositoryConfig(configApi);
  }

  @Override
  public DeleteReferenceBuilder<Reference> deleteReference() {
    return new CombinedDeleteReference(treeApi);
  }

  @Override
  public AssignReferenceBuilder<Reference> assignReference() {
    return new CombinedAssignReference(treeApi);
  }

  @Override
  @Deprecated
  public AssignTagBuilder assignTag() {
    return new CombinedAssignTag(treeApi);
  }

  @Override
  @Deprecated
  public AssignBranchBuilder assignBranch() {
    return new CombinedAssignBranch(treeApi);
  }

  @Override
  @Deprecated
  public DeleteTagBuilder deleteTag() {
    return new CombinedDeleteTag(treeApi);
  }

  @Override
  @Deprecated
  public DeleteBranchBuilder deleteBranch() {
    return new CombinedDeleteBranch(treeApi);
  }
}
