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

import org.projectnessie.api.grpc.AssignReferenceRequest;
import org.projectnessie.api.grpc.TreeServiceGrpc.TreeServiceBlockingStub;
import org.projectnessie.client.api.AssignTagBuilder;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Reference;

final class GrpcAssignTag implements AssignTagBuilder {
  private final TreeServiceBlockingStub stub;
  private Reference assignTo;
  private String tagName;
  private String hash;

  public GrpcAssignTag(TreeServiceBlockingStub stub) {
    this.stub = stub;
  }

  @Override
  public AssignTagBuilder assignTo(Reference assignTo) {
    this.assignTo = assignTo;
    return this;
  }

  @Override
  public AssignTagBuilder tagName(String branchName) {
    this.tagName = branchName;
    return this;
  }

  @Override
  public AssignTagBuilder hash(String hash) {
    this.hash = hash;
    return this;
  }

  @Override
  public void assign() throws NessieNotFoundException, NessieConflictException {
    handle(
        () ->
            stub.assignTag(
                AssignReferenceRequest.newBuilder()
                    .setNamedRef(tagName)
                    .setOldHash(hash)
                    .setTag(
                        org.projectnessie.api.grpc.Tag.newBuilder()
                            .setName(assignTo.getName())
                            .setHash(assignTo.getHash())
                            .build())
                    .build()));
  }
}
