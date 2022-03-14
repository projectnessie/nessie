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
package org.projectnessie.model;

import static org.projectnessie.model.Validation.validateReferenceName;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.io.Serializable;
import javax.annotation.Nullable;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.immutables.value.Value;

/** API representation of a Nessie Branch. This object is akin to a Ref in Git terminology. */
@Value.Immutable
@Schema(type = SchemaType.OBJECT, title = "Branch")
@JsonSerialize(as = ImmutableBranch.class)
@JsonDeserialize(as = ImmutableBranch.class)
@JsonTypeName("BRANCH")
public interface Branch extends Reference, Serializable {

  @Override
  @Nullable
  String getHash();

  /**
   * Validation rule using {@link org.projectnessie.model.Validation#validateReferenceName(String)}.
   */
  @Value.Check
  default void checkName() {
    validateReferenceName(getName());
  }

  @Override
  default ReferenceType getType() {
    return ReferenceType.BRANCH;
  }

  static ImmutableBranch.Builder builder() {
    return ImmutableBranch.builder();
  }

  static Branch of(String name, @Nullable String hash) {
    return builder().name(name).hash(hash).build();
  }
}
