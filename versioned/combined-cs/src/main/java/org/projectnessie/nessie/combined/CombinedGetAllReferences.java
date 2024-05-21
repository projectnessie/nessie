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
package org.projectnessie.nessie.combined;

import org.projectnessie.api.v2.TreeApi;
import org.projectnessie.api.v2.params.ReferencesParams;
import org.projectnessie.client.builder.BaseGetAllReferencesBuilder;
import org.projectnessie.model.ReferencesResponse;

final class CombinedGetAllReferences extends BaseGetAllReferencesBuilder<ReferencesParams> {

  private final TreeApi treeApi;

  CombinedGetAllReferences(TreeApi treeApi) {
    super(ReferencesParams::forNextPage);
    this.treeApi = treeApi;
  }

  @Override
  protected ReferencesParams params() {
    return ReferencesParams.builder()
        .fetchOption(fetchOption)
        .filter(filter)
        .maxRecords(maxRecords)
        .build();
  }

  @Override
  protected ReferencesResponse get(ReferencesParams p) {
    try {
      return treeApi.getAllReferences(p);
    } catch (RuntimeException e) {
      throw CombinedClientImpl.maybeWrapException(e);
    }
  }
}
