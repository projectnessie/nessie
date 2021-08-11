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

import java.util.List;
import java.util.stream.Collectors;
import org.projectnessie.api.grpc.Empty;
import org.projectnessie.api.grpc.TreeServiceGrpc.TreeServiceBlockingStub;
import org.projectnessie.client.api.GetAllReferencesBuilder;
import org.projectnessie.grpc.ProtoUtil;
import org.projectnessie.model.Reference;

final class GrpcGetAllReferences implements GetAllReferencesBuilder {

  private final TreeServiceBlockingStub stub;

  public GrpcGetAllReferences(TreeServiceBlockingStub stub) {
    this.stub = stub;
  }

  @Override
  public List<Reference> get() {
    return stub.getAllReferences(Empty.newBuilder().build()).getReferenceList().stream()
        .map(ProtoUtil::refFromProto)
        .collect(Collectors.toList());
  }
}
