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
import java.util.Map;
import org.projectnessie.client.api.CreateNamespaceBuilder;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Namespace;

public abstract class BaseCreateNamespaceBuilder implements CreateNamespaceBuilder {

  protected Namespace namespace;
  protected String refName;
  protected String hashOnRef;
  protected final Map<String, String> properties = new HashMap<>();
  protected CommitMeta commitMeta;

  @Override
  public CreateNamespaceBuilder commitMeta(CommitMeta commitMeta) {
    this.commitMeta = commitMeta;
    return this;
  }

  @Override
  public CreateNamespaceBuilder namespace(Namespace namespace) {
    this.namespace = namespace;
    return this;
  }

  @Override
  public CreateNamespaceBuilder refName(String refName) {
    this.refName = refName;
    return this;
  }

  @Override
  public CreateNamespaceBuilder hashOnRef(@Nullable String hashOnRef) {
    this.hashOnRef = hashOnRef;
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
}
