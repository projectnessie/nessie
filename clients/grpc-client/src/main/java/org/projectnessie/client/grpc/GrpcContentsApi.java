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

import static org.projectnessie.client.grpc.GrpcExceptionMapper.handleNessieNotFoundEx;
import static org.projectnessie.grpc.ProtoUtil.fromProto;
import static org.projectnessie.grpc.ProtoUtil.toProto;

import io.grpc.Channel;
import javax.annotation.Nullable;
import org.projectnessie.api.ContentsApi;
import org.projectnessie.api.grpc.ContentsServiceGrpc.ContentsServiceBlockingStub;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.ContentsKey;
import org.projectnessie.model.MultiGetContentsRequest;
import org.projectnessie.model.MultiGetContentsResponse;

/** The gRPC client-side implementation of the {@link ContentsApi}. */
public class GrpcContentsApi implements ContentsApi {

  private final ContentsServiceBlockingStub stub;

  public GrpcContentsApi(Channel channel) {
    this.stub = org.projectnessie.api.grpc.ContentsServiceGrpc.newBlockingStub(channel);
  }

  @Override
  public org.projectnessie.model.Contents getContents(
      ContentsKey key, String ref, @Nullable String hashOnRef) throws NessieNotFoundException {
    return handleNessieNotFoundEx(() -> fromProto(stub.getContents(toProto(key, ref, hashOnRef))));
  }

  @Override
  public MultiGetContentsResponse getMultipleContents(
      String ref, @Nullable String hashOnRef, MultiGetContentsRequest request)
      throws NessieNotFoundException {
    return handleNessieNotFoundEx(
        () -> fromProto(stub.getMultipleContents(toProto(ref, hashOnRef, request))));
  }
}
