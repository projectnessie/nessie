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

import static org.projectnessie.client.grpc.GrpcExceptionMapper.handle;
import static org.projectnessie.grpc.ProtoUtil.refFromProto;
import static org.projectnessie.grpc.ProtoUtil.refToProto;

import org.projectnessie.api.grpc.CreateReferenceRequest;
import org.projectnessie.api.grpc.TreeServiceGrpc.TreeServiceBlockingStub;
import org.projectnessie.client.api.CreateReferenceBuilder;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Reference;

final class GrpcCreateReference implements CreateReferenceBuilder {
  private final TreeServiceBlockingStub stub;
  private String sourceRefName;
  private Reference reference;

  public GrpcCreateReference(TreeServiceBlockingStub stub) {
    this.stub = stub;
  }

  @Override
  public CreateReferenceBuilder sourceRefName(String sourceRefName) {
    this.sourceRefName = sourceRefName;
    return this;
  }

  @Override
  public CreateReferenceBuilder reference(Reference reference) {
    this.reference = reference;
    return this;
  }

  @Override
  public Reference create() throws NessieNotFoundException, NessieConflictException {
    return handle(
        () -> {
          CreateReferenceRequest.Builder builder =
              CreateReferenceRequest.newBuilder().setReference(refToProto(reference));
          if (null != sourceRefName) builder.setSourceRefName(sourceRefName);
          return refFromProto(stub.createReference(builder.build()));
        });
  }
}
