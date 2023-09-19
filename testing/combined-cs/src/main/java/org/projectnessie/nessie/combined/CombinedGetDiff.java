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
import org.projectnessie.api.v2.params.DiffParams;
import org.projectnessie.client.builder.BaseGetDiffBuilder;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.DiffResponse;
import org.projectnessie.model.Reference;

final class CombinedGetDiff extends BaseGetDiffBuilder<DiffParams> {
  private final TreeApi treeApi;

  CombinedGetDiff(TreeApi treeApi) {
    super(DiffParams::forNextPage);
    this.treeApi = treeApi;
  }

  @Override
  protected DiffParams params() {
    return DiffParams.builder()
        .fromRef(Reference.toPathString(fromRefName, fromHashOnRef))
        .toRef(Reference.toPathString(toRefName, toHashOnRef))
        .maxRecords(maxRecords)
        .minKey(minKey)
        .maxKey(maxKey)
        .prefixKey(prefixKey)
        .filter(filter)
        .requestedKeys(keys)
        .build();
  }

  @Override
  public DiffResponse get(DiffParams params) throws NessieNotFoundException {
    try {
      return treeApi.getDiff(params);
    } catch (RuntimeException e) {
      throw CombinedClientImpl.maybeWrapException(e);
    }
  }
}
