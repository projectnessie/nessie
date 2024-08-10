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
package org.projectnessie.client.builder;

import jakarta.annotation.Nullable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.projectnessie.client.api.UpdateNamespaceBuilder;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Namespace;

public abstract class BaseUpdateNamespaceBuilder implements UpdateNamespaceBuilder {

  protected final Map<String, String> propertyUpdates = new HashMap<>();
  protected final Set<String> propertyRemovals = new HashSet<>();
  protected Namespace namespace;
  protected String refName;
  protected String hashOnRef;
  protected CommitMeta commitMeta;

  @Override
  public UpdateNamespaceBuilder commitMeta(CommitMeta commitMeta) {
    this.commitMeta = commitMeta;
    return this;
  }

  @Override
  public UpdateNamespaceBuilder namespace(Namespace namespace) {
    this.namespace = namespace;
    return this;
  }

  @Override
  public UpdateNamespaceBuilder refName(String refName) {
    this.refName = refName;
    return this;
  }

  @Override
  public UpdateNamespaceBuilder hashOnRef(@Nullable String hashOnRef) {
    this.hashOnRef = hashOnRef;
    return this;
  }

  @Override
  public UpdateNamespaceBuilder removeProperty(String key) {
    this.propertyRemovals.add(key);
    return this;
  }

  @Override
  public UpdateNamespaceBuilder removeProperties(Set<String> propertyRemovals) {
    this.propertyRemovals.addAll(propertyRemovals);
    return this;
  }

  @Override
  public UpdateNamespaceBuilder updateProperty(String key, String value) {
    this.propertyUpdates.put(key, value);
    return this;
  }

  @Override
  public UpdateNamespaceBuilder updateProperties(Map<String, String> propertyUpdates) {
    this.propertyUpdates.putAll(propertyUpdates);
    return this;
  }
}
