/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.client.api;

import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;
import org.immutables.value.Value;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Reference;

@Value.Immutable
public interface GetNamespaceResult {
  @NotNull
  @Value.Parameter(order = 1)
  Namespace getNamespace();

  @Nullable
  @Value.Parameter(order = 2)
  Reference getEffectiveReference();

  static GetNamespaceResult of(Namespace namespace, Reference effectiveReference) {
    return ImmutableGetNamespaceResult.of(namespace, effectiveReference);
  }
}
