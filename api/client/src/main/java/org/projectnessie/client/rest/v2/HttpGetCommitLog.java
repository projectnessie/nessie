/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.client.rest.v2;

import org.projectnessie.api.v2.params.CommitLogParams;
import org.projectnessie.client.builder.BaseGetCommitLogBuilder;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.FetchOption;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.Reference;

final class HttpGetCommitLog extends BaseGetCommitLogBuilder<CommitLogParams> {

  private final HttpClient client;

  HttpGetCommitLog(HttpClient client) {
    super(CommitLogParams::forNextPage);
    this.client = client;
  }

  @Override
  protected CommitLogParams params() {
    return CommitLogParams.builder()
        .fetchOption(fetchOption)
        .startHash(untilHash)
        .maxRecords(maxRecords)
        .filter(filter)
        .build();
  }

  @Override
  protected LogResponse get(CommitLogParams p) throws NessieNotFoundException {
    return client
        .newRequest()
        .path("trees/{ref}/history")
        .resolveTemplate(
            "ref",
            Reference.toPathString(refName, hashOnRef)) // TODO: move refName, hashOnRef to params
        .queryParam("max-records", p.maxRecords())
        .queryParam("page-token", p.pageToken())
        .queryParam("filter", p.filter())
        .queryParam("limit-hash", p.startHash())
        .queryParam("fetch", FetchOption.getFetchOptionName(p.fetchOption()))
        .unwrap(NessieNotFoundException.class)
        .get()
        .readEntity(LogResponse.class);
  }
}
