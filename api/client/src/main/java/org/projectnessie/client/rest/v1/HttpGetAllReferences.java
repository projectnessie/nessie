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

import org.projectnessie.api.v1.params.ReferencesParams;
import org.projectnessie.client.builder.BaseGetAllReferencesBuilder;
import org.projectnessie.model.ReferencesResponse;

final class HttpGetAllReferences extends BaseGetAllReferencesBuilder<ReferencesParams> {

  private final NessieApiClient client;

  public HttpGetAllReferences(NessieApiClient client) {
    super(ReferencesParams::forNextPage);
    this.client = client;
  }

  @Override
  protected ReferencesParams params() {
    return ReferencesParams.builder()
        .maxRecords(maxRecords)
        .fetchOption(fetchOption)
        .filter(filter)
        .build();
  }

  @Override
  protected ReferencesResponse get(ReferencesParams p) {
    return client.getTreeApi().getAllReferences(p);
  }
}
