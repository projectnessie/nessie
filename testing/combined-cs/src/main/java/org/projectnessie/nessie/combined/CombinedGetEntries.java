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
import org.projectnessie.api.v2.params.EntriesParams;
import org.projectnessie.client.api.GetEntriesBuilder;
import org.projectnessie.client.builder.BaseGetEntriesBuilder;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.Reference;

final class CombinedGetEntries extends BaseGetEntriesBuilder<EntriesParams> {

  private final TreeApi treeApi;

  CombinedGetEntries(TreeApi treeApi) {
    super(EntriesParams::forNextPage);
    this.treeApi = treeApi;
  }

  @Override
  @Deprecated
  public GetEntriesBuilder namespaceDepth(Integer namespaceDepth) {
    throw new UnsupportedOperationException("namespaceDepth is not supported for Nessie API v2");
  }

  @Override
  protected EntriesParams params() {
    return EntriesParams.builder() // TODO: namespace, derive prefix
        .filter(filter)
        .minKey(minKey)
        .maxKey(maxKey)
        .prefixKey(prefixKey)
        .requestedKeys(keys)
        .maxRecords(maxRecords)
        .withContent(withContent)
        .build();
  }

  @Override
  protected EntriesResponse get(EntriesParams p) throws NessieNotFoundException {
    try {
      return treeApi.getEntries(Reference.toPathString(refName, hashOnRef), p);
    } catch (RuntimeException e) {
      throw CombinedClientImpl.maybeWrapException(e);
    }
  }
}
