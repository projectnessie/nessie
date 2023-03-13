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

import org.projectnessie.api.v1.params.MultipleNamespacesParams;
import org.projectnessie.client.api.GetMultipleNamespacesBuilder;
import org.projectnessie.client.builder.BaseGetMultipleNamespacesBuilder;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.GetNamespacesResponse;

final class HttpGetMultipleNamespaces extends BaseGetMultipleNamespacesBuilder {

  private final NessieApiClient client;

  HttpGetMultipleNamespaces(NessieApiClient client) {
    this.client = client;
  }

  @Override
  public GetMultipleNamespacesBuilder onlyDirectChildren(boolean onlyDirectChildren) {
    if (onlyDirectChildren) {
      throw new IllegalArgumentException("'onlyDirectChildren' is not available with REST API v1");
    }
    return this;
  }

  @Override
  public GetNamespacesResponse get() throws NessieReferenceNotFoundException {
    MultipleNamespacesParams params =
        MultipleNamespacesParams.builder()
            .namespace(namespace)
            .refName(refName)
            .hashOnRef(hashOnRef)
            .build();
    return client.getNamespaceApi().getNamespaces(params);
  }
}
