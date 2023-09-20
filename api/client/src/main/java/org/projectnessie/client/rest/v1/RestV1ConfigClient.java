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

import org.projectnessie.api.v1.http.HttpConfigApi;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.model.NessieConfiguration;

class RestV1ConfigClient implements HttpConfigApi {
  private final HttpClient client;

  RestV1ConfigClient(HttpClient client) {
    this.client = client;
  }

  @Override
  public NessieConfiguration getConfig() {
    return client.newRequest().path("config").get().readEntity(NessieConfiguration.class);
  }
}
