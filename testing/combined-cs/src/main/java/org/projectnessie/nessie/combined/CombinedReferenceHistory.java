/*
 * Copyright (C) 2023 Dremio
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
import org.projectnessie.api.v2.params.ReferenceHistoryParams;
import org.projectnessie.api.v2.params.ReferenceHistoryParamsBuilder;
import org.projectnessie.client.api.ReferenceHistoryBuilder;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.ReferenceHistoryResponse;

final class CombinedReferenceHistory implements ReferenceHistoryBuilder {
  private final TreeApi treeApi;
  private final ReferenceHistoryParamsBuilder params = ReferenceHistoryParams.builder();

  CombinedReferenceHistory(TreeApi treeApi) {
    this.treeApi = treeApi;
  }

  @Override
  public ReferenceHistoryBuilder refName(String refName) {
    params.ref(refName);
    return this;
  }

  @Override
  public ReferenceHistoryBuilder headCommitsToScan(Integer headCommitsToScan) {
    params.headCommitsToScan(headCommitsToScan);
    return this;
  }

  @Override
  public ReferenceHistoryResponse get() throws NessieNotFoundException {
    return treeApi.getReferenceHistory(params.build());
  }
}
