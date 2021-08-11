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

import javax.annotation.Nullable;
import org.projectnessie.api.grpc.TreeServiceGrpc.TreeServiceBlockingStub;
import org.projectnessie.api.params.CommitLogParams;
import org.projectnessie.client.api.GetCommitLogBuilder;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.LogResponse;

final class GrpcGetCommitLog implements GetCommitLogBuilder {
  private final TreeServiceBlockingStub stub;
  private final CommitLogParams.Builder params = CommitLogParams.builder();
  private String refName;

  public GrpcGetCommitLog(TreeServiceBlockingStub stub) {
    this.stub = stub;
  }

  @Override
  public GetCommitLogBuilder untilHash(@Nullable String untilHash) {
    params.startHash(untilHash);
    return this;
  }

  @Override
  public GetCommitLogBuilder refName(String refName) {
    this.refName = refName;
    return this;
  }

  @Override
  public GetCommitLogBuilder hashOnRef(@Nullable String hashOnRef) {
    params.endHash(hashOnRef);
    return this;
  }

  @Override
  public GetCommitLogBuilder maxRecords(int maxRecords) {
    params.maxRecords(maxRecords);
    return this;
  }

  @Override
  public GetCommitLogBuilder pageToken(String pageToken) {
    params.pageToken(pageToken);
    return this;
  }

  @Override
  public GetCommitLogBuilder queryExpression(String queryExpression) {
    params.expression(queryExpression);
    return this;
  }

  @Override
  public LogResponse get() throws NessieNotFoundException {
    return handleNessieNotFoundEx(
        () -> fromProto(stub.getCommitLog(toProto(refName, params.build()))));
  }
}
