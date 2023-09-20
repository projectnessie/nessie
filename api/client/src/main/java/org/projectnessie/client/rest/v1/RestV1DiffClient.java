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
import org.projectnessie.api.v1.http.HttpDiffApi;
import org.projectnessie.api.v1.params.DiffParams;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.DiffResponse;

class RestV1DiffClient implements HttpDiffApi {

  private final HttpClient client;

  RestV1DiffClient(HttpClient client) {
    this.client = client;
  }

  @Override
  public DiffResponse getDiff(@NotNull @jakarta.validation.constraints.NotNull DiffParams params)
      throws NessieNotFoundException {
    return client
        .newRequest()
        .path("diffs/{fromRef}{fromHashOnRef}...{toRef}{toHashOnRef}")
        .resolveTemplate("fromRef", params.getFromRef())
        .resolveTemplate("toRef", params.getToRef())
        .resolveTemplate(
            "fromHashOnRef",
            params.getFromHashOnRef() != null ? "*" + params.getFromHashOnRef() : "")
        .resolveTemplate(
            "toHashOnRef", params.getToHashOnRef() != null ? "*" + params.getToHashOnRef() : "")
        .get()
        .readEntity(DiffResponse.class);
  }
}
