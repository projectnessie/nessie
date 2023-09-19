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
import org.projectnessie.client.builder.BaseChangeReferenceBuilder;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Reference;

abstract class BaseCombinedDeleteReference<T extends Reference, B>
    extends BaseChangeReferenceBuilder<B> {

  private final TreeApi treeApi;

  BaseCombinedDeleteReference(TreeApi treeApi) {
    this.treeApi = treeApi;
  }

  public void delete() throws NessieConflictException, NessieNotFoundException {
    getAndDelete();
  }

  @SuppressWarnings("unchecked")
  public T getAndDelete() throws NessieNotFoundException, NessieConflictException {
    String type = super.type != null ? super.type.name() : null;
    try {
      return (T)
          treeApi
              .deleteReference(type, Reference.toPathString(refName, expectedHash))
              .getReference();
    } catch (RuntimeException e) {
      throw CombinedClientImpl.maybeWrapException(e);
    }
  }
}
