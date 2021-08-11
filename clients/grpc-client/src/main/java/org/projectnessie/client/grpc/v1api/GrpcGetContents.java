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
import static org.projectnessie.grpc.ProtoUtil.toProto;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.projectnessie.api.grpc.ContentsServiceGrpc.ContentsServiceBlockingStub;
import org.projectnessie.client.api.GetContentsBuilder;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Contents;
import org.projectnessie.model.ContentsKey;
import org.projectnessie.model.ImmutableMultiGetContentsRequest;
import org.projectnessie.model.MultiGetContentsResponse;
import org.projectnessie.model.MultiGetContentsResponse.ContentsWithKey;

final class GrpcGetContents implements GetContentsBuilder {
  private final ContentsServiceBlockingStub stub;
  private final ImmutableMultiGetContentsRequest.Builder request =
      ImmutableMultiGetContentsRequest.builder();
  private String refName;
  private String hashOnRef;

  public GrpcGetContents(ContentsServiceBlockingStub stub) {
    this.stub = stub;
  }

  @Override
  public GetContentsBuilder key(ContentsKey key) {
    request.addRequestedKeys(key);
    return this;
  }

  @Override
  public GetContentsBuilder keys(List<ContentsKey> keys) {
    request.addAllRequestedKeys(keys);
    return this;
  }

  @Override
  public GetContentsBuilder refName(String refName) {
    this.refName = refName;
    return this;
  }

  @Override
  public GetContentsBuilder hashOnRef(@Nullable String hashOnRef) {
    this.hashOnRef = hashOnRef;
    return this;
  }

  @Override
  public Map<ContentsKey, Contents> get() throws NessieNotFoundException {
    return handleNessieNotFoundEx(
        () -> {
          MultiGetContentsResponse response =
              fromProto(stub.getMultipleContents(toProto(refName, hashOnRef, request.build())));
          return response.getContents().stream()
              .collect(Collectors.toMap(ContentsWithKey::getKey, ContentsWithKey::getContents));
        });
  }
}
