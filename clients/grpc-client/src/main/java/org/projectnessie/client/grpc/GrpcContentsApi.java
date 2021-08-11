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

import io.grpc.Channel;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.projectnessie.api.ContentsApi;
import org.projectnessie.api.grpc.ContentsRequest;
import org.projectnessie.api.grpc.ContentsRequest.Builder;
import org.projectnessie.api.grpc.ContentsServiceGrpc.ContentsServiceBlockingStub;
import org.projectnessie.api.grpc.MultipleContentsRequest;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.grpc.ProtoUtil;
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
    return handleNessieNotFoundEx(
        () -> {
          Builder builder =
              ContentsRequest.newBuilder().setContentsKey(ProtoUtil.toProto(key)).setRef(ref);
          builder = null != hashOnRef ? builder.setHashOnRef(hashOnRef) : builder;
          return ProtoUtil.fromProto(stub.getContents(builder.build()));
        });
  }

  @Override
  public MultiGetContentsResponse getMultipleContents(
      String ref, @Nullable String hashOnRef, MultiGetContentsRequest request)
      throws NessieNotFoundException {
    return handleNessieNotFoundEx(
        () -> {
          final MultipleContentsRequest.Builder builder =
              MultipleContentsRequest.newBuilder().setRef(ref);
          if (null != hashOnRef) builder.setHashOnRef(hashOnRef);
          if (null != request)
            request.getRequestedKeys().forEach(k -> builder.addRequestedKeys(ProtoUtil.toProto(k)));

          return MultiGetContentsResponse.of(
              stub.getMultipleContents(builder.build()).getContentsWithKeyList().stream()
                  .map(ProtoUtil::fromProto)
                  .collect(Collectors.toList()));
        });
  }
}
