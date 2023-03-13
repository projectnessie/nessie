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

import org.projectnessie.api.v2.params.ReferencesParams;
import org.projectnessie.client.builder.BaseGetAllReferencesBuilder;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.model.FetchOption;
import org.projectnessie.model.ReferencesResponse;

final class HttpGetAllReferences extends BaseGetAllReferencesBuilder<ReferencesParams> {

  private final HttpClient client;

  HttpGetAllReferences(HttpClient client) {
    super(ReferencesParams::forNextPage);
    this.client = client;
  }

  @Override
  protected ReferencesParams params() {
    return ReferencesParams.builder()
        .fetchOption(fetchOption)
        .filter(filter)
        .maxRecords(maxRecords)
        .build();
  }

  @Override
  protected ReferencesResponse get(ReferencesParams p) {
    return client
        .newRequest()
        .path("trees")
        .queryParam("fetch", FetchOption.getFetchOptionName(p.fetchOption()))
        .queryParam("max-records", p.maxRecords())
        .queryParam("page-token", p.pageToken())
        .queryParam("filter", p.filter())
        .get()
        .readEntity(ReferencesResponse.class);
  }
}
