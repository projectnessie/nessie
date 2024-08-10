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
import org.projectnessie.client.api.GetMultipleNamespacesBuilder;
import org.projectnessie.model.Namespace;

public abstract class BaseGetMultipleNamespacesBuilder implements GetMultipleNamespacesBuilder {

  protected Namespace namespace;
  protected String refName;
  protected String hashOnRef;

  @Override
  public GetMultipleNamespacesBuilder namespace(Namespace namespace) {
    this.namespace = namespace;
    return this;
  }

  @Override
  public GetMultipleNamespacesBuilder refName(String refName) {
    this.refName = refName;
    return this;
  }

  @Override
  public GetMultipleNamespacesBuilder hashOnRef(@Nullable String hashOnRef) {
    this.hashOnRef = hashOnRef;
    return this;
  }
}
