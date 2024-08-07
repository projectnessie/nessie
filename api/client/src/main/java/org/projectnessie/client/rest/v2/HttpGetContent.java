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

import java.util.Map;
import org.projectnessie.client.builder.BaseGetContentBuilder;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.ContentResponse;
import org.projectnessie.model.GetMultipleContentsResponse;
import org.projectnessie.model.Reference;

final class HttpGetContent extends BaseGetContentBuilder {
  private final HttpClient client;
  private final HttpApiV2 api;

  HttpGetContent(HttpClient client, HttpApiV2 api) {
    this.client = client;
    this.api = api;
  }

  @Override
  public Map<ContentKey, Content> get() throws NessieNotFoundException {
    return getWithResponse().toContentsMap();
  }

  @Override
  public ContentResponse getSingle(ContentKey key) throws NessieNotFoundException {
    if (!request.build().getRequestedKeys().isEmpty()) {
      throw new IllegalStateException(
          "Must not use getSingle() with key() or keys(), pass the single key to getSingle()");
    }
    return client
        .newRequest()
        .path("trees/{ref}/contents/{key}")
        .resolveTemplate("ref", Reference.toPathString(refName, hashOnRef))
        .resolveTemplate("key", api.toPathString(key))
        .queryParam("for-write", forWrite ? "true" : null)
        .unwrap(NessieNotFoundException.class)
        .get()
        .readEntity(ContentResponse.class);
  }

  @Override
  public GetMultipleContentsResponse getWithResponse() throws NessieNotFoundException {
    return client
        .newRequest()
        .path("trees/{ref}/contents")
        .resolveTemplate("ref", Reference.toPathString(refName, hashOnRef))
        .queryParam("for-write", forWrite ? "true" : null)
        .unwrap(NessieNotFoundException.class)
        .post(request.build())
        .readEntity(GetMultipleContentsResponse.class);
  }
}
