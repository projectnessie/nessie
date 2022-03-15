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

import javax.annotation.Nullable;
import org.projectnessie.api.params.NamespacesParams;
import org.projectnessie.api.params.NamespacesParamsBuilder;
import org.projectnessie.client.api.GetNamespacesBuilder;
import org.projectnessie.client.http.NessieApiClient;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.GetNamespacesResponse;
import org.projectnessie.model.Namespace;

final class HttpGetNamespaces extends BaseHttpRequest implements GetNamespacesBuilder {

  private final NamespacesParamsBuilder builder = NamespacesParams.builder();

  HttpGetNamespaces(NessieApiClient client) {
    super(client);
  }

  /**
   * The namespace prefix to search for.
   *
   * @param namespace The namespace prefix to search for
   * @return this
   */
  @Override
  public GetNamespacesBuilder namespace(Namespace namespace) {
    builder.namespace(namespace);
    return this;
  }

  @Override
  public GetNamespacesBuilder refName(String refName) {
    builder.refName(refName);
    return this;
  }

  @Override
  public GetNamespacesBuilder hashOnRef(@Nullable String hashOnRef) {
    builder.hashOnRef(hashOnRef);
    return this;
  }

  @Override
  public GetNamespacesResponse get() throws NessieReferenceNotFoundException {
    NamespacesParams build = builder.build();
    return client.getNamespaceApi().getNamespaces(build);
  }
}
