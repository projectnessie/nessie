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

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.media.SchemaProperty;
import org.immutables.value.Value;

@Value.Immutable(prehash = true)
@Schema(
    type = SchemaType.OBJECT,
    title = "CreateReference",
    // Smallrye does neither support JsonFormat nor javax.validation.constraints.Pattern :(
    properties = {
      @SchemaProperty(name = "sourceRef", pattern = Validation.REF_NAME_REGEX),
      @SchemaProperty(name = "hash", pattern = Validation.HASH_REGEX)
    })
@JsonSerialize(as = ImmutableCreateReference.class)
@JsonDeserialize(as = ImmutableCreateReference.class)
public interface CreateReference extends Base {
  /** Name (and type) of the reference to create, and optional target hash. */
  @NotNull
  Reference getReference();

  /**
   * Name of the reference off which the reference shall be created. The hash, if given in {@link
   * #getReference()}, must exist on this source-ref.
   */
  @Nullable
  @JsonFormat(pattern = Validation.REF_NAME_REGEX)
  String getSourceRef();

  static CreateReference of(Reference reference, String sourceRef) {
    return ImmutableCreateReference.builder().reference(reference).sourceRef(sourceRef).build();
  }
}
