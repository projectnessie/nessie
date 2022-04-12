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

import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.projectnessie.api.params.ImmutableNamespaceUpdate;
import org.projectnessie.api.params.NamespaceParams;
import org.projectnessie.api.params.NamespaceParamsBuilder;
import org.projectnessie.client.api.UpdateNamespaceBuilder;
import org.projectnessie.client.http.NessieApiClient;
import org.projectnessie.error.NessieNamespaceNotFoundException;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.Namespace;

final class HttpUpdateNamespace extends BaseHttpRequest implements UpdateNamespaceBuilder {

  private final NamespaceParamsBuilder builder = NamespaceParams.builder();
  private final ImmutableNamespaceUpdate.Builder updateBuilder = ImmutableNamespaceUpdate.builder();

  HttpUpdateNamespace(NessieApiClient client) {
    super(client);
  }

  @Override
  public UpdateNamespaceBuilder namespace(Namespace namespace) {
    builder.namespace(namespace);
    return this;
  }

  @Override
  public UpdateNamespaceBuilder refName(String refName) {
    builder.refName(refName);
    return this;
  }

  @Override
  public UpdateNamespaceBuilder hashOnRef(@Nullable String hashOnRef) {
    builder.hashOnRef(hashOnRef);
    return this;
  }

  @Override
  public UpdateNamespaceBuilder removeProperties(Set<String> propertyRemovals) {
    updateBuilder.addAllPropertyRemovals(propertyRemovals);
    return this;
  }

  @Override
  public UpdateNamespaceBuilder updateProperty(String key, String value) {
    updateBuilder.putPropertyUpdates(key, value);
    return this;
  }

  @Override
  public UpdateNamespaceBuilder removeProperty(String key) {
    updateBuilder.addPropertyRemovals(key);
    return this;
  }

  @Override
  public UpdateNamespaceBuilder updateProperties(Map<String, String> propertyUpdates) {
    updateBuilder.putAllPropertyUpdates(propertyUpdates);
    return this;
  }

  @Override
  public void update() throws NessieNamespaceNotFoundException, NessieReferenceNotFoundException {
    client.getNamespaceApi().updateProperties(builder.build(), updateBuilder.build());
  }
}
