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

import static org.projectnessie.model.Validation.validateHash;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import javax.validation.constraints.NotEmpty;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.immutables.value.Value;

/**
 * API representation of a detached Nessie commit, a commit without a named reference. The reserved
 * reference name {@value #REF_NAME} is used to indicate "datached" commit IDs. This object is akin
 * to a Ref in Git terminology.
 */
@Value.Immutable
@Schema(type = SchemaType.OBJECT, title = "Detached commit hash")
@JsonSerialize(as = ImmutableDetached.class)
@JsonDeserialize(as = ImmutableDetached.class)
@JsonTypeName("DETACHED")
public interface Detached extends Reference {

  String REF_NAME = "DETACHED";

  @Override
  @Value.Redacted
  @JsonIgnore
  default String getName() {
    return REF_NAME;
  }

  @Override
  @NotEmpty
  String getHash();

  /** Validation rule using {@link Validation#validateReferenceName(String)}. */
  @Value.Check
  default void checkHash() {
    validateHash(getHash());
  }

  @Override
  default ReferenceType getType() {
    throw new UnsupportedOperationException("Illegal use of detached reference");
  }

  static ImmutableDetached.Builder builder() {
    return ImmutableDetached.builder();
  }

  static Detached of(String hash) {
    return builder().hash(hash).build();
  }
}
