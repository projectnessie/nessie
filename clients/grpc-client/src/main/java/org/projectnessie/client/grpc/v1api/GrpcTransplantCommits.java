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

import java.util.Collections;
import java.util.List;
import org.projectnessie.api.grpc.TransplantRequest;
import org.projectnessie.api.grpc.TransplantRequest.Builder;
import org.projectnessie.api.grpc.TreeServiceGrpc.TreeServiceBlockingStub;
import org.projectnessie.client.api.TransplantCommitsBuilder;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;

final class GrpcTransplantCommits implements TransplantCommitsBuilder {
  private final TreeServiceBlockingStub stub;
  private String branchName;
  private String hash;
  private String message;
  private String fromRefName;
  private List<String> hashesToTransplant = Collections.emptyList();

  public GrpcTransplantCommits(TreeServiceBlockingStub stub) {
    this.stub = stub;
  }

  @Override
  public TransplantCommitsBuilder branchName(String branchName) {
    this.branchName = branchName;
    return this;
  }

  @Override
  public TransplantCommitsBuilder hash(String hash) {
    this.hash = hash;
    return this;
  }

  @Override
  public TransplantCommitsBuilder message(String message) {
    this.message = message;
    return this;
  }

  @Override
  public TransplantCommitsBuilder fromRefName(String fromRefName) {
    this.fromRefName = fromRefName;
    return this;
  }

  @Override
  public TransplantCommitsBuilder hashesToTransplant(List<String> hashesToTransplant) {
    this.hashesToTransplant = hashesToTransplant;
    return this;
  }

  @Override
  public void transplant() throws NessieNotFoundException, NessieConflictException {
    handle(
        () -> {
          Builder builder = TransplantRequest.newBuilder().setBranchName(branchName).setHash(hash);
          if (null != message) builder.setMessage(message);
          if (null != fromRefName) builder.setFromRefName(fromRefName);
          hashesToTransplant.forEach(builder::addHashesToTransplant);
          return stub.transplantCommitsIntoBranch(builder.build());
        });
  }
}
