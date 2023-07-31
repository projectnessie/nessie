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

import static org.projectnessie.model.Validation.validateHashOrRelativeSpec;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import javax.annotation.Nullable;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.Pattern;
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
  @jakarta.validation.constraints.NotEmpty
  @Value.Parameter(order = 1)
  @Pattern(
      regexp = Validation.HASH_OR_RELATIVE_COMMIT_SPEC_REGEX,
      message = Validation.HASH_OR_RELATIVE_COMMIT_SPEC_MESSAGE)
  @jakarta.validation.constraints.Pattern(
      regexp = Validation.HASH_OR_RELATIVE_COMMIT_SPEC_REGEX,
      message = Validation.HASH_OR_RELATIVE_COMMIT_SPEC_MESSAGE)
  String getHash();

  @Nullable
  @jakarta.annotation.Nullable
  @Override
  @Value.Parameter(order = 2)
  ReferenceMetadata getMetadata();

  /** Validation rule using {@link Validation#validateHashOrRelativeSpec(String)}. */
  @Value.Check
  @Override
  default void checkHash() {
    // Note: a detached hash must have a start commit ID (absolute part); this is going to be
    // checked by the service layer.
    validateHashOrRelativeSpec(getHash());
  }

  @Override
  default ReferenceType getType() {
    throw new UnsupportedOperationException("Illegal use of detached reference");
  }

  static ImmutableDetached.Builder builder() {
    return ImmutableDetached.builder();
  }

  static Detached of(String hash) {
    return ImmutableDetached.of(hash, null);
  }
}
