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
package org.projectnessie.client.rest.v1;

import org.projectnessie.api.v1.params.CommitLogParams;
import org.projectnessie.client.builder.BaseGetCommitLogBuilder;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.LogResponse;

final class HttpGetCommitLog extends BaseGetCommitLogBuilder<CommitLogParams> {

  private final NessieApiClient client;

  HttpGetCommitLog(NessieApiClient client) {
    super(CommitLogParams::forNextPage);
    this.client = client;
  }

  @Override
  protected CommitLogParams params() {
    return CommitLogParams.builder()
        .filter(filter)
        .maxRecords(maxRecords)
        .fetchOption(fetchOption)
        .startHash(untilHash)
        .endHash(hashOnRef)
        .build();
  }

  @Override
  protected LogResponse get(CommitLogParams p) throws NessieNotFoundException {
    return client.getTreeApi().getCommitLog(refName, p);
  }
}
