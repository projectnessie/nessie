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
package org.projectnessie.client.http.v1api;

import org.projectnessie.api.params.CommitLogParams;
import org.projectnessie.client.api.GetCommitLogBuilder;
import org.projectnessie.client.http.NessieApiClient;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.LogResponse;

final class HttpGetCommitLog extends BaseHttpOnReferenceRequest<GetCommitLogBuilder>
    implements GetCommitLogBuilder {

  private final CommitLogParams.Builder params = CommitLogParams.builder();

  HttpGetCommitLog(NessieApiClient client) {
    super(client);
  }

  @Override
  public GetCommitLogBuilder untilHash(String untilHash) {
    params.startHash(untilHash);
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
    params.endHash(hashOnRef);
    return client.getTreeApi().getCommitLog(refName, params.build());
  }
}
