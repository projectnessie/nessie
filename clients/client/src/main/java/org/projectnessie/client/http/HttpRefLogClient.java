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
import org.projectnessie.api.http.HttpRefLogApi;
import org.projectnessie.api.params.RefLogParams;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.RefLogResponse;

class HttpRefLogClient implements HttpRefLogApi {

  private final HttpClient client;

  public HttpRefLogClient(HttpClient client) {
    this.client = client;
  }

  @Override
  public RefLogResponse getRefLog(@NotNull RefLogParams params) throws NessieNotFoundException {
    HttpRequest builder = client.newRequest().path("reflogs");
    return builder
        .queryParam("maxRecords", params.maxRecords())
        .queryParam("pageToken", params.pageToken())
        .queryParam("startHash", params.startHash())
        .queryParam("endHash", params.endHash())
        .queryParam("filter", params.filter())
        .get()
        .readEntity(RefLogResponse.class);
  }
}
