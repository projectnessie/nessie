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

import org.projectnessie.api.params.ReferencesParams;
import org.projectnessie.client.api.GetAllReferencesBuilder;
import org.projectnessie.client.http.NessieApiClient;
import org.projectnessie.model.ReferencesResponse;

final class HttpGetAllReferences extends BaseHttpRequest implements GetAllReferencesBuilder {

  private final ReferencesParams.Builder params = ReferencesParams.builder();

  HttpGetAllReferences(NessieApiClient client) {
    super(client);
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
  public GetAllReferencesBuilder fetchAdditionalInfo(boolean fetchAdditionalInfo) {
    params.fetchAdditionalInfo(fetchAdditionalInfo);
    return this;
  }

  @Override
  public GetAllReferencesBuilder queryExpression(String queryExpression) {
    params.expression(queryExpression);
    return this;
  }

  @Override
  public ReferencesResponse get() {
    return client.getTreeApi().getAllReferences(params.build());
  }
}
