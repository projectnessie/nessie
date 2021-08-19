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
import static org.projectnessie.grpc.ProtoUtil.toProto;

import io.grpc.stub.StreamObserver;
import io.quarkus.grpc.GrpcService;
import java.util.ArrayList;
import java.util.List;
import javax.inject.Inject;
import org.projectnessie.api.grpc.Contents;
import org.projectnessie.api.grpc.ContentsRequest;
import org.projectnessie.api.grpc.ContentsServiceGrpc;
import org.projectnessie.api.grpc.MultipleContentsRequest;
import org.projectnessie.api.grpc.MultipleContentsResponse;
import org.projectnessie.client.grpc.GrpcExceptionMapper;
import org.projectnessie.model.ContentsKey;
import org.projectnessie.model.MultiGetContentsRequest;
import org.projectnessie.model.MultiGetContentsResponse;
import org.projectnessie.services.rest.ContentsResource;

/** The gRPC service implementation for the Contents-API. */
@GrpcService
public class ContentsService extends ContentsServiceGrpc.ContentsServiceImplBase {
  private final ContentsResource contentsResource;

  @Inject
  public ContentsService(ContentsResource contentsResource) {
    this.contentsResource = contentsResource;
  }

  @Override
  public void getContents(ContentsRequest request, StreamObserver<Contents> observer) {
    try {
      org.projectnessie.model.Contents contents =
          contentsResource.getContents(
              fromProto(request.getContentsKey()),
              request.getRef(),
              getHashOnRefFromProtoRequest(request.getHashOnRef()));
      observer.onNext(toProto(contents));
      observer.onCompleted();
    } catch (Exception e) {
      observer.onError(GrpcExceptionMapper.toProto(e));
    }
  }

  @Override
  public void getMultipleContents(
      MultipleContentsRequest request, StreamObserver<MultipleContentsResponse> observer) {
    MultipleContentsResponse.Builder builder = MultipleContentsResponse.newBuilder();
    List<ContentsKey> requestedKeys = new ArrayList<>();
    request.getRequestedKeysList().forEach(k -> requestedKeys.add(fromProto(k)));
    try {
      MultiGetContentsResponse multipleContents =
          contentsResource.getMultipleContents(
              request.getRef(),
              getHashOnRefFromProtoRequest(request.getHashOnRef()),
              requestedKeys.isEmpty()
                  ? null // there are tests that check for nullability
                  : MultiGetContentsRequest.of(requestedKeys));
      multipleContents.getContents().forEach(c -> builder.addContentsWithKey(toProto(c)));
      observer.onNext(builder.build());
      observer.onCompleted();
    } catch (Exception e) {
      observer.onError(GrpcExceptionMapper.toProto(e));
    }
  }

  private String getHashOnRefFromProtoRequest(String hashOnRef) {
    return "".equals(hashOnRef) ? null : hashOnRef;
  }
}
