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
package org.projectnessie.client.rest.v2;

import org.projectnessie.client.api.ReferenceHistoryBuilder;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.ReferenceHistoryResponse;

final class HttpReferenceHistory implements ReferenceHistoryBuilder {

  private final HttpClient client;

  HttpReferenceHistory(HttpClient client) {
    this.client = client;
  }

  private String refName;
  private Integer headCommitsToScan;

  @Override
  public ReferenceHistoryBuilder refName(String refName) {
    this.refName = refName;
    return this;
  }

  @Override
  public ReferenceHistoryBuilder headCommitsToScan(Integer headCommitsToScan) {
    this.headCommitsToScan = headCommitsToScan;
    return this;
  }

  @Override
  public ReferenceHistoryResponse get() throws NessieNotFoundException {
    return client
        .newRequest()
        .path("trees/{ref}/recent-changes")
        .queryParam("scan-commits", headCommitsToScan)
        .resolveTemplate("ref", refName)
        .unwrap(NessieNotFoundException.class)
        .get()
        .readEntity(ReferenceHistoryResponse.class);
  }
}
