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
package org.projectnessie.server.grpc;

import static org.projectnessie.grpc.ProtoUtil.fromProto;
import static org.projectnessie.grpc.ProtoUtil.refFromProto;
import static org.projectnessie.grpc.ProtoUtil.refToProto;
import static org.projectnessie.grpc.ProtoUtil.toProto;

import io.grpc.stub.StreamObserver;
import io.quarkus.grpc.GrpcService;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.projectnessie.api.grpc.AssignReferenceRequest;
import org.projectnessie.api.grpc.CommitLogRequest;
import org.projectnessie.api.grpc.CommitLogResponse;
import org.projectnessie.api.grpc.CommitRequest;
import org.projectnessie.api.grpc.DeleteReferenceRequest;
import org.projectnessie.api.grpc.Empty;
import org.projectnessie.api.grpc.EntriesRequest;
import org.projectnessie.api.grpc.EntriesResponse;
import org.projectnessie.api.grpc.GetAllReferencesResponse;
import org.projectnessie.api.grpc.GetReferenceByNameRequest;
import org.projectnessie.api.grpc.MergeRequest;
import org.projectnessie.api.grpc.Reference;
import org.projectnessie.api.grpc.TransplantRequest;
import org.projectnessie.api.grpc.TreeServiceGrpc;
import org.projectnessie.client.grpc.GrpcExceptionMapper;
import org.projectnessie.grpc.ProtoUtil;
import org.projectnessie.model.Branch;
import org.projectnessie.model.ImmutableMerge;
import org.projectnessie.model.ImmutableTransplant;
import org.projectnessie.model.Tag;
import org.projectnessie.services.rest.TreeResource;

/** The gRPC service implementation for the Tree-API. */
@GrpcService
public class TreeService extends TreeServiceGrpc.TreeServiceImplBase {

  private final TreeResource treeResource;

  @Inject
  public TreeService(TreeResource treeResource) {
    this.treeResource = treeResource;
  }

  @Override
  public void getAllReferences(Empty request, StreamObserver<GetAllReferencesResponse> observer) {
    try {
      observer.onNext(
          GetAllReferencesResponse.newBuilder()
              .addAllReference(
                  treeResource.getAllReferences().stream()
                      .map(ProtoUtil::refToProto)
                      .collect(Collectors.toList()))
              .build());
      observer.onCompleted();
    } catch (Exception e) {
      observer.onError(GrpcExceptionMapper.toProto(e));
    }
  }

  @Override
  public void getReferenceByName(
      GetReferenceByNameRequest request, StreamObserver<Reference> observer) {
    try {
      observer.onNext(refToProto(treeResource.getReferenceByName(request.getNamedRef())));
      observer.onCompleted();
    } catch (Exception e) {
      observer.onError(GrpcExceptionMapper.toProto(e));
    }
  }

  @Override
  public void createReference(Reference request, StreamObserver<Reference> observer) {
    try {
      observer.onNext(refToProto(treeResource.createReference(refFromProto(request))));
      observer.onCompleted();
    } catch (Exception e) {
      observer.onError(GrpcExceptionMapper.toProto(e));
    }
  }

  @Override
  public void getDefaultBranch(Empty request, StreamObserver<Reference> observer) {
    try {
      observer.onNext(refToProto(treeResource.getDefaultBranch()));
      observer.onCompleted();
    } catch (Exception e) {
      observer.onError(GrpcExceptionMapper.toProto(e));
    }
  }

  @Override
  public void assignTag(AssignReferenceRequest request, StreamObserver<Empty> observer) {
    try {
      treeResource.assignTag(
          request.getNamedRef(),
          request.getOldHash(),
          Tag.of(request.getTag().getName(), request.getTag().getHash()));
      observer.onNext(Empty.newBuilder().build());
      observer.onCompleted();
    } catch (Exception e) {
      observer.onError(GrpcExceptionMapper.toProto(e));
    }
  }

  @Override
  public void deleteTag(DeleteReferenceRequest request, StreamObserver<Empty> observer) {
    try {
      treeResource.deleteTag(request.getNamedRef(), request.getHash());
      observer.onNext(Empty.newBuilder().build());
      observer.onCompleted();
    } catch (Exception e) {
      observer.onError(GrpcExceptionMapper.toProto(e));
    }
  }

  @Override
  public void assignBranch(AssignReferenceRequest request, StreamObserver<Empty> observer) {
    try {
      treeResource.assignBranch(
          request.getNamedRef(),
          request.getOldHash(),
          Branch.of(request.getBranch().getName(), request.getBranch().getHash()));
      observer.onNext(Empty.newBuilder().build());
      observer.onCompleted();
    } catch (Exception e) {
      observer.onError(GrpcExceptionMapper.toProto(e));
    }
  }

  @Override
  public void deleteBranch(DeleteReferenceRequest request, StreamObserver<Empty> observer) {
    try {
      treeResource.deleteBranch(request.getNamedRef(), request.getHash());
      observer.onNext(Empty.newBuilder().build());
      observer.onCompleted();
    } catch (Exception e) {
      observer.onError(GrpcExceptionMapper.toProto(e));
    }
  }

  @Override
  public void getCommitLog(CommitLogRequest request, StreamObserver<CommitLogResponse> observer) {
    try {
      observer.onNext(
          toProto(treeResource.getCommitLog(request.getNamedRef(), fromProto(request))));
      observer.onCompleted();
    } catch (Exception e) {
      observer.onError(GrpcExceptionMapper.toProto(e));
    }
  }

  @Override
  public void getEntries(EntriesRequest request, StreamObserver<EntriesResponse> observer) {
    try {
      observer.onNext(toProto(treeResource.getEntries(request.getNamedRef(), fromProto(request))));
      observer.onCompleted();
    } catch (Exception e) {
      observer.onError(GrpcExceptionMapper.toProto(e));
    }
  }

  @Override
  public void transplantCommitsIntoBranch(
      TransplantRequest request, StreamObserver<Empty> observer) {
    try {
      treeResource.transplantCommitsIntoBranch(
          request.getBranchName(),
          request.getHash(),
          request.getMessage(),
          ImmutableTransplant.builder()
              .hashesToTransplant(request.getHashesToTransplantList())
              .build());
      observer.onNext(Empty.newBuilder().build());
      observer.onCompleted();
    } catch (Exception e) {
      observer.onError(GrpcExceptionMapper.toProto(e));
    }
  }

  @Override
  public void mergeRefIntoBranch(MergeRequest request, StreamObserver<Empty> observer) {
    try {
      treeResource.mergeRefIntoBranch(
          request.getToBranch(),
          request.getExpectedHash(),
          "".equals(request.getFromHash())
              ? null // there are some tests that check for nullability
              : ImmutableMerge.builder().fromHash(request.getFromHash()).build());
      observer.onNext(Empty.newBuilder().build());
      observer.onCompleted();
    } catch (Exception e) {
      observer.onError(GrpcExceptionMapper.toProto(e));
    }
  }

  @Override
  public void commitMultipleOperations(
      CommitRequest request, StreamObserver<org.projectnessie.api.grpc.Branch> observer) {
    try {
      observer.onNext(
          toProto(
              treeResource.commitMultipleOperations(
                  request.getBranch(),
                  request.getHash(),
                  fromProto(request.getCommitOperations()))));
      observer.onCompleted();
    } catch (Exception e) {
      observer.onError(GrpcExceptionMapper.toProto(e));
    }
  }
}
