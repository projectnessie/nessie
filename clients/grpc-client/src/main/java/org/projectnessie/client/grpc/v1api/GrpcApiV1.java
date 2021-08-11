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
package org.projectnessie.client.grpc.v1api;

import static org.projectnessie.client.grpc.GrpcExceptionMapper.handleNessieNotFoundEx;
import static org.projectnessie.grpc.ProtoUtil.fromProto;

import io.grpc.ManagedChannel;
import org.projectnessie.api.grpc.ConfigServiceGrpc;
import org.projectnessie.api.grpc.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.projectnessie.api.grpc.ContentsServiceGrpc;
import org.projectnessie.api.grpc.ContentsServiceGrpc.ContentsServiceBlockingStub;
import org.projectnessie.api.grpc.Empty;
import org.projectnessie.api.grpc.TreeServiceGrpc;
import org.projectnessie.api.grpc.TreeServiceGrpc.TreeServiceBlockingStub;
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
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.NessieConfiguration;

public final class GrpcApiV1 implements NessieApiV1 {

  private final ManagedChannel channel;
  private final ConfigServiceBlockingStub configServiceBlockingStub;
  private final TreeServiceBlockingStub treeServiceBlockingStub;
  private final ContentsServiceBlockingStub contentsServiceBlockingStub;

  public GrpcApiV1(ManagedChannel channel) {
    this.channel = channel;
    this.configServiceBlockingStub = ConfigServiceGrpc.newBlockingStub(channel);
    this.contentsServiceBlockingStub = ContentsServiceGrpc.newBlockingStub(channel);
    this.treeServiceBlockingStub = TreeServiceGrpc.newBlockingStub(channel);
  }

  @Override
  public void close() {
    if (null != channel) channel.shutdown();
  }

  @Override
  public NessieConfiguration getConfig() {
    return fromProto(configServiceBlockingStub.getConfig(Empty.newBuilder().build()));
  }

  @Override
  public Branch getDefaultBranch() throws NessieNotFoundException {
    return handleNessieNotFoundEx(
        () ->
            fromProto(
                treeServiceBlockingStub.getDefaultBranch(Empty.newBuilder().build()).getBranch()));
  }

  @Override
  public GetContentsBuilder getContents() {
    return new GrpcGetContents(contentsServiceBlockingStub);
  }

  @Override
  public GetAllReferencesBuilder getAllReferences() {
    return new GrpcGetAllReferences(treeServiceBlockingStub);
  }

  @Override
  public GetReferenceBuilder getReference() {
    return new GrpcGetReference(treeServiceBlockingStub);
  }

  @Override
  public CreateReferenceBuilder createReference() {
    return new GrpcCreateReference(treeServiceBlockingStub);
  }

  @Override
  public GetEntriesBuilder getEntries() {
    return new GrpcGetEntries(treeServiceBlockingStub);
  }

  @Override
  public GetCommitLogBuilder getCommitLog() {
    return new GrpcGetCommitLog(treeServiceBlockingStub);
  }

  @Override
  public AssignTagBuilder assignTag() {
    return new GrpcAssignTag(treeServiceBlockingStub);
  }

  @Override
  public DeleteTagBuilder deleteTag() {
    return new GrpcDeleteTag(treeServiceBlockingStub);
  }

  @Override
  public AssignBranchBuilder assignBranch() {
    return new GrpcAssignBranch(treeServiceBlockingStub);
  }

  @Override
  public DeleteBranchBuilder deleteBranch() {
    return new GrpcDeleteBranch(treeServiceBlockingStub);
  }

  @Override
  public TransplantCommitsBuilder transplantCommitsIntoBranch() {
    return new GrpcTransplantCommits(treeServiceBlockingStub);
  }

  @Override
  public MergeReferenceBuilder mergeRefIntoBranch() {
    return new GrpcMergeReference(treeServiceBlockingStub);
  }

  @Override
  public CommitMultipleOperationsBuilder commitMultipleOperations() {
    return new GrpcCommitMultipleOperations(treeServiceBlockingStub);
  }
}
