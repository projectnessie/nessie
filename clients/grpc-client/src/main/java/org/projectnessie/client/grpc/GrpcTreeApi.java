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
package org.projectnessie.client.grpc;

import static org.projectnessie.client.grpc.GrpcExceptionMapper.handle;
import static org.projectnessie.client.grpc.GrpcExceptionMapper.handleNessieNotFoundEx;
import static org.projectnessie.grpc.ProtoUtil.fromProto;
import static org.projectnessie.grpc.ProtoUtil.refFromProto;
import static org.projectnessie.grpc.ProtoUtil.refToProto;
import static org.projectnessie.grpc.ProtoUtil.toProto;

import io.grpc.Channel;
import java.util.List;
import java.util.stream.Collectors;
import org.projectnessie.api.TreeApi;
import org.projectnessie.api.grpc.AssignReferenceRequest;
import org.projectnessie.api.grpc.CommitRequest;
import org.projectnessie.api.grpc.DeleteReferenceRequest;
import org.projectnessie.api.grpc.Empty;
import org.projectnessie.api.grpc.GetReferenceByNameRequest;
import org.projectnessie.api.grpc.MergeRequest;
import org.projectnessie.api.grpc.TransplantRequest;
import org.projectnessie.api.grpc.TransplantRequest.Builder;
import org.projectnessie.api.grpc.TreeServiceGrpc.TreeServiceBlockingStub;
import org.projectnessie.api.params.CommitLogParams;
import org.projectnessie.api.params.EntriesParams;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.grpc.ProtoUtil;
import org.projectnessie.model.Branch;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.Merge;
import org.projectnessie.model.Operations;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;
import org.projectnessie.model.Transplant;

/** The gRPC client-side implementation of the {@link TreeApi}. */
public class GrpcTreeApi implements TreeApi {

  private final TreeServiceBlockingStub stub;

  public GrpcTreeApi(Channel channel) {
    this.stub = org.projectnessie.api.grpc.TreeServiceGrpc.newBlockingStub(channel);
  }

  @Override
  public List<Reference> getAllReferences() {
    return stub.getAllReferences(Empty.newBuilder().build()).getReferenceList().stream()
        .map(ProtoUtil::refFromProto)
        .collect(Collectors.toList());
  }

  @Override
  public Branch getDefaultBranch() throws NessieNotFoundException {
    return handleNessieNotFoundEx(
        () -> fromProto(stub.getDefaultBranch(Empty.newBuilder().build()).getBranch()));
  }

  @Override
  public Reference createReference(Reference reference)
      throws NessieNotFoundException, NessieConflictException {
    return handle(() -> refFromProto(stub.createReference(refToProto(reference))));
  }

  @Override
  public Reference getReferenceByName(String refName) throws NessieNotFoundException {
    return handleNessieNotFoundEx(
        () ->
            refFromProto(
                stub.getReferenceByName(
                    GetReferenceByNameRequest.newBuilder().setNamedRef(refName).build())));
  }

  @Override
  public EntriesResponse getEntries(String refName, EntriesParams params)
      throws NessieNotFoundException {
    return handleNessieNotFoundEx(() -> fromProto(stub.getEntries(toProto(refName, params))));
  }

  @Override
  public LogResponse getCommitLog(String ref, CommitLogParams params)
      throws NessieNotFoundException {
    return handleNessieNotFoundEx(() -> fromProto(stub.getCommitLog(toProto(ref, params))));
  }

  @Override
  public void assignTag(String tagName, String oldHash, Tag tag)
      throws NessieNotFoundException, NessieConflictException {
    handle(
        () ->
            stub.assignTag(
                AssignReferenceRequest.newBuilder()
                    .setNamedRef(tagName)
                    .setOldHash(oldHash)
                    .setTag(
                        org.projectnessie.api.grpc.Tag.newBuilder()
                            .setName(tag.getName())
                            .setHash(tag.getHash())
                            .build())
                    .build()));
  }

  @Override
  public void deleteTag(String tagName, String hash)
      throws NessieConflictException, NessieNotFoundException {
    handle(
        () ->
            stub.deleteTag(
                DeleteReferenceRequest.newBuilder().setNamedRef(tagName).setHash(hash).build()));
  }

  @Override
  public void assignBranch(String branchName, String oldHash, Branch branch)
      throws NessieNotFoundException, NessieConflictException {
    handle(
        () ->
            stub.assignBranch(
                AssignReferenceRequest.newBuilder()
                    .setNamedRef(branchName)
                    .setOldHash(oldHash)
                    .setBranch(
                        org.projectnessie.api.grpc.Branch.newBuilder()
                            .setName(branch.getName())
                            .setHash(branch.getHash())
                            .build())
                    .build()));
  }

  @Override
  public void deleteBranch(String branchName, String hash)
      throws NessieConflictException, NessieNotFoundException {
    handle(
        () ->
            stub.deleteBranch(
                DeleteReferenceRequest.newBuilder().setNamedRef(branchName).setHash(hash).build()));
  }

  @Override
  public void transplantCommitsIntoBranch(
      String branchName, String hash, String message, Transplant transplant)
      throws NessieNotFoundException, NessieConflictException {
    handle(
        () -> {
          Builder builder = TransplantRequest.newBuilder().setBranchName(branchName).setHash(hash);
          if (null != message) builder.setMessage(message);
          if (null != transplant)
            transplant.getHashesToTransplant().forEach(builder::addHashesToTransplant);
          return stub.transplantCommitsIntoBranch(builder.build());
        });
  }

  @Override
  public void mergeRefIntoBranch(String branchName, String hash, Merge merge)
      throws NessieNotFoundException, NessieConflictException {
    handle(
        () -> {
          MergeRequest.Builder builder =
              MergeRequest.newBuilder().setToBranch(branchName).setExpectedHash(hash);
          if (null != merge) builder.setFromHash(merge.getFromHash());
          return stub.mergeRefIntoBranch(builder.build());
        });
  }

  @Override
  public Branch commitMultipleOperations(String branchName, String hash, Operations operations)
      throws NessieNotFoundException, NessieConflictException {
    return handle(
        () ->
            fromProto(
                stub.commitMultipleOperations(
                    CommitRequest.newBuilder()
                        .setBranch(branchName)
                        .setHash(hash)
                        .setCommitOperations(toProto(operations))
                        .build())));
  }
}
