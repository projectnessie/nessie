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

import javax.validation.constraints.NotNull;
import org.projectnessie.api.v1.http.HttpContentApi;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.GetMultipleContentsRequest;
import org.projectnessie.model.GetMultipleContentsResponse;

class RestV1ContentClient implements HttpContentApi {

  private final HttpClient client;

  RestV1ContentClient(HttpClient client) {
    this.client = client;
  }

  @Override
  public Content getContent(
      @NotNull @jakarta.validation.constraints.NotNull ContentKey key, String ref, String hashOnRef)
      throws NessieNotFoundException {
    return client
        .newRequest()
        .path("contents")
        .path(key.toPathString())
        .queryParam("ref", ref)
        .queryParam("hashOnRef", hashOnRef)
        .get()
        .readEntity(Content.class);
  }

  @Override
  public GetMultipleContentsResponse getMultipleContents(
      @NotNull @jakarta.validation.constraints.NotNull String ref,
      String hashOnRef,
      @NotNull @jakarta.validation.constraints.NotNull GetMultipleContentsRequest request)
      throws NessieNotFoundException {
    return client
        .newRequest()
        .path("contents")
        .queryParam("ref", ref)
        .queryParam("hashOnRef", hashOnRef)
        .post(request)
        .readEntity(GetMultipleContentsResponse.class);
  }
}
