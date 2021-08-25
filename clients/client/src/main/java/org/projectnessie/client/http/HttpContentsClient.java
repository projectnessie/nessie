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
package org.projectnessie.client.http;

import javax.validation.constraints.NotNull;
import org.projectnessie.api.http.HttpContentsApi;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Contents;
import org.projectnessie.model.ContentsKey;
import org.projectnessie.model.MultiGetContentsRequest;
import org.projectnessie.model.MultiGetContentsResponse;

class HttpContentsClient implements HttpContentsApi {

  private final HttpClient client;

  public HttpContentsClient(HttpClient client) {
    this.client = client;
  }

  @Override
  public Contents getContents(@NotNull ContentsKey key, String ref, String hashOnRef)
      throws NessieNotFoundException {
    return client
        .newRequest()
        .path("contents")
        .path(key.toPathString())
        .queryParam("ref", ref)
        .queryParam("hashOnRef", hashOnRef)
        .get()
        .readEntity(Contents.class);
  }

  @Override
  public MultiGetContentsResponse getMultipleContents(
      @NotNull String ref, String hashOnRef, @NotNull MultiGetContentsRequest request)
      throws NessieNotFoundException {
    return client
        .newRequest()
        .path("contents")
        .queryParam("ref", ref)
        .queryParam("hashOnRef", hashOnRef)
        .post(request)
        .readEntity(MultiGetContentsResponse.class);
  }
}
