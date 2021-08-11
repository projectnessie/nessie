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
import org.projectnessie.client.api.AssignBranchBuilder;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Reference;

final class GrpcAssignBranch implements AssignBranchBuilder {
  private final TreeServiceBlockingStub stub;
  private Reference assignTo;
  private String branchName;
  private String hash;

  public GrpcAssignBranch(TreeServiceBlockingStub stub) {
    this.stub = stub;
  }

  @Override
  public AssignBranchBuilder assignTo(Reference assignTo) {
    this.assignTo = assignTo;
    return this;
  }

  @Override
  public AssignBranchBuilder branchName(String branchName) {
    this.branchName = branchName;
    return this;
  }

  @Override
  public AssignBranchBuilder hash(String hash) {
    this.hash = hash;
    return this;
  }

  @Override
  public void assign() throws NessieNotFoundException, NessieConflictException {
    handle(
        () ->
            stub.assignBranch(
                AssignReferenceRequest.newBuilder()
                    .setNamedRef(branchName)
                    .setOldHash(hash)
                    .setBranch(
                        org.projectnessie.api.grpc.Branch.newBuilder()
                            .setName(assignTo.getName())
                            .setHash(assignTo.getHash())
                            .build())
                    .build()));
  }
}
