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

import org.projectnessie.api.v1.params.NamespaceParams;
import org.projectnessie.api.v1.params.NamespaceParamsBuilder;
import org.projectnessie.client.api.GetNamespaceResult;
import org.projectnessie.client.builder.BaseGetNamespaceBuilder;
import org.projectnessie.error.NessieNamespaceNotFoundException;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.Namespace;

final class HttpGetNamespace extends BaseGetNamespaceBuilder {

  private final NessieApiClient client;

  HttpGetNamespace(NessieApiClient client) {
    this.client = client;
  }

  @Override
  public Namespace get() throws NessieNamespaceNotFoundException, NessieReferenceNotFoundException {
    NamespaceParamsBuilder builder =
        NamespaceParams.builder().namespace(namespace).refName(refName).hashOnRef(hashOnRef);
    return client.getNamespaceApi().getNamespace(builder.build());
  }

  @Override
  public GetNamespaceResult getWithResponse() {
    throw new UnsupportedOperationException(
        "Extended commit response data is not available in API v1");
  }
}
