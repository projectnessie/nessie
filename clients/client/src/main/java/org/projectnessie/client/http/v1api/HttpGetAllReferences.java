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
package org.projectnessie.client.http.v1api;

import java.util.stream.Stream;
import org.projectnessie.api.params.FetchOption;
import org.projectnessie.api.params.ReferencesParams;
import org.projectnessie.api.params.ReferencesParamsBuilder;
import org.projectnessie.client.StreamingUtil;
import org.projectnessie.client.api.GetAllReferencesBuilder;
import org.projectnessie.client.http.NessieApiClient;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Reference;
import org.projectnessie.model.ReferencesResponse;

final class HttpGetAllReferences extends BaseHttpRequest implements GetAllReferencesBuilder {

  private final ReferencesParamsBuilder params;

  HttpGetAllReferences(NessieApiClient client) {
    this(client, ReferencesParams.builder());
  }

  HttpGetAllReferences(NessieApiClient client, ReferencesParamsBuilder params) {
    super(client);
    this.params = params;
  }

  @Override
  public GetAllReferencesBuilder maxRecords(int maxRecords) {
    params.maxRecords(maxRecords);
    return this;
  }

  @Override
  public GetAllReferencesBuilder pageToken(String pageToken) {
    params.pageToken(pageToken);
    return this;
  }

  @Override
  public GetAllReferencesBuilder fetch(FetchOption fetchOption) {
    params.fetchOption(fetchOption);
    return this;
  }

  @Override
  public GetAllReferencesBuilder filter(String filter) {
    params.filter(filter);
    return this;
  }

  private ReferencesParams params() {
    return params.build();
  }

  @Override
  public ReferencesResponse get() {
    return get(params());
  }

  private ReferencesResponse get(ReferencesParams p) {
    return client.getTreeApi().getAllReferences(p);
  }

  @Override
  public Stream<Reference> stream() throws NessieNotFoundException {
    ReferencesParams p = params();
    return StreamingUtil.generateStream(
        ReferencesResponse::getReferences, pageToken -> get(p.forNextPage(pageToken)));
  }
}
