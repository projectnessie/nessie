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
import static org.projectnessie.grpc.ProtoUtil.refFromProto;

import org.projectnessie.api.grpc.GetReferenceByNameRequest;
import org.projectnessie.api.grpc.TreeServiceGrpc.TreeServiceBlockingStub;
import org.projectnessie.client.api.GetReferenceBuilder;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Reference;

final class GrpcGetReference implements GetReferenceBuilder {
  private final TreeServiceBlockingStub stub;

  private String refName;

  public GrpcGetReference(TreeServiceBlockingStub stub) {
    this.stub = stub;
  }

  @Override
  public GetReferenceBuilder refName(String refName) {
    this.refName = refName;
    return this;
  }

  @Override
  public Reference get() throws NessieNotFoundException {
    return handleNessieNotFoundEx(
        () ->
            refFromProto(
                stub.getReferenceByName(
                    GetReferenceByNameRequest.newBuilder().setNamedRef(refName).build())));
  }
}
