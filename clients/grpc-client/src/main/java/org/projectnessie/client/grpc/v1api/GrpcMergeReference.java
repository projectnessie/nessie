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

import org.projectnessie.api.grpc.MergeRequest;
import org.projectnessie.api.grpc.TreeServiceGrpc.TreeServiceBlockingStub;
import org.projectnessie.client.api.MergeReferenceBuilder;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;

final class GrpcMergeReference implements MergeReferenceBuilder {
  private final TreeServiceBlockingStub stub;
  private String branchName;
  private String hash;
  private String fromHash;
  private String fromRefName;

  public GrpcMergeReference(TreeServiceBlockingStub stub) {
    this.stub = stub;
  }

  @Override
  public MergeReferenceBuilder fromHash(String fromHash) {
    this.fromHash = fromHash;
    return this;
  }

  @Override
  public MergeReferenceBuilder fromRefName(String fromRefName) {
    this.fromRefName = fromRefName;
    return this;
  }

  @Override
  public MergeReferenceBuilder branchName(String branchName) {
    this.branchName = branchName;
    return this;
  }

  @Override
  public MergeReferenceBuilder hash(String hash) {
    this.hash = hash;
    return this;
  }

  @Override
  public void merge() throws NessieNotFoundException, NessieConflictException {
    handle(
        () -> {
          MergeRequest.Builder builder =
              MergeRequest.newBuilder().setToBranch(branchName).setExpectedHash(hash);
          if (null != fromHash) builder.setFromHash(fromHash);
          if (null != fromRefName) builder.setFromRefName(fromRefName);
          return stub.mergeRefIntoBranch(builder.build());
        });
  }
}
