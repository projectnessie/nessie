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
package org.projectnessie.nessie.combined;

import org.projectnessie.api.v2.TreeApi;
import org.projectnessie.api.v2.params.GetReferenceParams;
import org.projectnessie.client.builder.BaseGetReferenceBuilder;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Reference;

final class CombinedGetReference extends BaseGetReferenceBuilder {

  private final TreeApi treeApi;

  CombinedGetReference(TreeApi treeApi) {
    this.treeApi = treeApi;
  }

  @Override
  public Reference get() throws NessieNotFoundException {
    try {
      return treeApi
          .getReferenceByName(
              GetReferenceParams.builder().ref(refName).fetchOption(fetchOption).build())
          .getReference();
    } catch (RuntimeException e) {
      throw CombinedClientImpl.maybeWrapException(e);
    }
  }
}
