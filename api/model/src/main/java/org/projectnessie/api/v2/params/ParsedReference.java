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
package org.projectnessie.api.v2.params;

import javax.annotation.Nullable;
import org.immutables.value.Value;
import org.projectnessie.model.Detached;
import org.projectnessie.model.Reference.ReferenceType;

/**
 * Represents the attributes that make a reference. All components are optional, except that at
 * least one of {@link #name()} or {@link #hashWithRelativeSpec()} must be supplied.
 */
@Value.Immutable
public interface ParsedReference {
  @Value.Parameter(order = 1)
  @Nullable
  @jakarta.annotation.Nullable
  String name();

  @Value.Parameter(order = 2)
  @Nullable
  @jakarta.annotation.Nullable
  String hashWithRelativeSpec();

  @Value.Parameter(order = 3)
  @Nullable
  @jakarta.annotation.Nullable
  ReferenceType type();

  @Value.Check
  default void check() {
    if (hashWithRelativeSpec() == null && name() == null) {
      throw new IllegalStateException(
          "Either name or commit ID with optional relative commit spec or both must be supplied");
    }
  }

  static ParsedReference parsedReference(
      @Nullable @jakarta.annotation.Nullable String name,
      @Nullable @jakarta.annotation.Nullable String hashWithRelativeSpec,
      @Nullable @jakarta.annotation.Nullable ReferenceType type) {
    if (hashWithRelativeSpec != null && name == null) {
      name = Detached.REF_NAME;
    }
    return ImmutableParsedReference.of(name, hashWithRelativeSpec, type);
  }
}
