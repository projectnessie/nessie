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

import org.projectnessie.api.v2.params.EntriesParams;
import org.projectnessie.client.api.GetEntriesBuilder;
import org.projectnessie.client.builder.BaseGetEntriesBuilder;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.client.http.HttpRequest;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.Reference;

final class HttpGetEntries extends BaseGetEntriesBuilder<EntriesParams> {

  private final HttpClient client;
  private final HttpApiV2 api;

  HttpGetEntries(HttpClient client, HttpApiV2 api) {
    super(EntriesParams::forNextPage);
    this.client = client;
    this.api = api;
  }

  @Override
  @Deprecated
  public GetEntriesBuilder namespaceDepth(Integer namespaceDepth) {
    throw new UnsupportedOperationException("namespaceDepth is not supported for Nessie API v2");
  }

  @Override
  protected EntriesParams params() {
    return EntriesParams.builder() // TODO: namespace, derive prefix
        .filter(filter)
        .minKey(minKey)
        .maxKey(maxKey)
        .prefixKey(prefixKey)
        .requestedKeys(keys)
        .maxRecords(maxRecords)
        .withContent(withContent)
        .build();
  }

  @Override
  protected EntriesResponse get(EntriesParams p) throws NessieNotFoundException {
    HttpRequest req =
        client
            .newRequest()
            .path("trees/{ref}/entries")
            .resolveTemplate("ref", Reference.toPathString(refName, hashOnRef))
            .queryParam("filter", p.filter())
            .queryParam("content", p.withContent() ? "true" : null)
            .queryParam("page-token", p.pageToken())
            .queryParam("max-records", p.maxRecords());
    p.getRequestedKeys().forEach(k -> req.queryParam("key", api.toPathString(k)));
    ContentKey k = p.minKey();
    if (k != null) {
      req.queryParam("min-key", api.toPathString(k));
    }
    k = p.maxKey();
    if (k != null) {
      req.queryParam("max-key", api.toPathString(k));
    }
    k = p.prefixKey();
    if (k != null) {
      req.queryParam("prefix-key", api.toPathString(k));
    }
    return req.unwrap(NessieNotFoundException.class).get().readEntity(EntriesResponse.class);
  }
}
