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
import org.projectnessie.model.Branch;
import org.projectnessie.model.Detached;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Reference.ReferenceType;
import org.projectnessie.model.Tag;

/**
 * Represents the attributes that make a reference. All components are optional, except that at
 * least one of {@link #name()} or {@link #hash()} must be supplied.
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
  String hash();

  @Value.Parameter(order = 3)
  @Nullable
  @jakarta.annotation.Nullable
  ReferenceType type();

  @Value.Check
  default void check() {
    if (hash() == null && name() == null) {
      throw new IllegalStateException("Either name or hash or name and hash must be supplied");
    }
  }

  static ParsedReference parsedReference(
      @Nullable @jakarta.annotation.Nullable String name,
      @Nullable @jakarta.annotation.Nullable String hash,
      @Nullable @jakarta.annotation.Nullable ReferenceType type) {
    if (hash != null && name == null) {
      name = Detached.REF_NAME;
    }
    return ImmutableParsedReference.of(name, hash, type);
  }

  /**
   * Converts to a {@link Reference}, if one of the following conditions are met, all other
   * combinations throw an {@link IllegalArgumentException}.
   *
   * <ul>
   *   <li>Returns a {@link Detached}, if {@link #hash()} is present, {@link #name()} and {@link
   *       #type()} are {@code null}.
   *   <li>Returns a {@link Branch}, if {@link #name()} is present, {@link #type()} == {@link
   *       ReferenceType#BRANCH}.
   *   <li>Returns a {@link Tag}, if {@link #name()} is present, {@link #type()} == {@link
   *       ReferenceType#TAG}.
   * </ul>
   */
  @Value.NonAttribute
  default Reference toReference() {
    ReferenceType t = type();
    if (t == null) {
      if (Detached.REF_NAME.equals(name())) {
        return Detached.of(hash());
      }
      throw new IllegalArgumentException(
          "Cannot convert a name to a typed reference without a type");
    }

    switch (t) {
      case BRANCH:
        return Branch.of(name(), hash());
      case TAG:
        return Tag.of(name(), hash());
      default:
        throw new IllegalArgumentException("Unsupported reference type: " + type());
    }
  }
}
