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

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.projectnessie.api.params.NamespaceParams;
import org.projectnessie.api.params.NamespaceParamsBuilder;
import org.projectnessie.client.api.CreateNamespaceBuilder;
import org.projectnessie.client.http.NessieApiClient;
import org.projectnessie.error.NessieNamespaceAlreadyExistsException;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.ImmutableNamespace;
import org.projectnessie.model.Namespace;

final class HttpCreateNamespace extends BaseHttpRequest implements CreateNamespaceBuilder {

  private final NamespaceParamsBuilder builder = NamespaceParams.builder();
  private final Map<String, String> properties = new HashMap<>();

  HttpCreateNamespace(NessieApiClient client) {
    super(client);
  }

  @Override
  public CreateNamespaceBuilder namespace(Namespace namespace) {
    builder.namespace(namespace);
    return this;
  }

  @Override
  public CreateNamespaceBuilder refName(String refName) {
    builder.refName(refName);
    return this;
  }

  @Override
  public CreateNamespaceBuilder hashOnRef(@Nullable String hashOnRef) {
    builder.hashOnRef(hashOnRef);
    return this;
  }

  @Override
  public CreateNamespaceBuilder properties(Map<String, String> properties) {
    this.properties.putAll(properties);
    return this;
  }

  @Override
  public CreateNamespaceBuilder property(String key, String value) {
    this.properties.put(key, value);
    return this;
  }

  @Override
  public Namespace create()
      throws NessieNamespaceAlreadyExistsException, NessieReferenceNotFoundException {
    NamespaceParams params = builder.build();
    return client
        .getNamespaceApi()
        .createNamespace(
            params,
            ImmutableNamespace.builder()
                .from(params.getNamespace())
                .properties(properties)
                .build());
  }
}
