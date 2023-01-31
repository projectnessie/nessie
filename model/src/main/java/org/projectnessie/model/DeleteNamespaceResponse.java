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
package org.projectnessie.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableDeleteNamespaceResponse.class)
@JsonDeserialize(as = ImmutableDeleteNamespaceResponse.class)
public interface DeleteNamespaceResponse {
  @Value.Parameter(order = 1)
  Namespace getNamespace();

  @Value.Parameter(order = 2)
  Branch getEffectiveBranch();

  static DeleteNamespaceResponse of(Namespace namespace, Branch effectiveBranch) {
    return ImmutableDeleteNamespaceResponse.of(namespace, effectiveBranch);
  }
}
