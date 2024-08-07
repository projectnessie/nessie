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

import org.projectnessie.api.v2.params.DiffParams;
import org.projectnessie.client.builder.BaseGetDiffBuilder;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.client.http.HttpRequest;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.DiffResponse;
import org.projectnessie.model.Reference;

final class HttpGetDiff extends BaseGetDiffBuilder<DiffParams> {
  private final HttpClient client;
  private final HttpApiV2 api;

  HttpGetDiff(HttpClient client, HttpApiV2 api) {
    super(DiffParams::forNextPage);
    this.client = client;
    this.api = api;
  }

  @Override
  protected DiffParams params() {
    return DiffParams.builder()
        .fromRef(Reference.toPathString(fromRefName, fromHashOnRef))
        .toRef(Reference.toPathString(toRefName, toHashOnRef))
        .maxRecords(maxRecords)
        .minKey(minKey)
        .maxKey(maxKey)
        .prefixKey(prefixKey)
        .filter(filter)
        .requestedKeys(keys)
        .build();
  }

  @Override
  public DiffResponse get(DiffParams params) throws NessieNotFoundException {
    HttpRequest req =
        client
            .newRequest()
            .path("trees/{from}/diff/{to}")
            .resolveTemplate("from", params.getFromRef())
            .resolveTemplate("to", params.getToRef())
            .queryParam("max-records", params.maxRecords())
            .queryParam("page-token", params.pageToken())
            .queryParam("filter", params.getFilter());
    params.getRequestedKeys().forEach(k -> req.queryParam("key", api.toPathString(k)));
    ContentKey k = params.minKey();
    if (k != null) {
      req.queryParam("min-key", api.toPathString(k));
    }
    k = params.maxKey();
    if (k != null) {
      req.queryParam("max-key", api.toPathString(k));
    }
    k = params.prefixKey();
    if (k != null) {
      req.queryParam("prefix-key", api.toPathString(k));
    }
    return req.unwrap(NessieNotFoundException.class).get().readEntity(DiffResponse.class);
  }
}
