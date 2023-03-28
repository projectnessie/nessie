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
package org.projectnessie.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import javax.annotation.Nullable;
import javax.validation.constraints.NotEmpty;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableContentMetadata.class)
@JsonDeserialize(as = ImmutableContentMetadata.class)
public interface ContentMetadata {

  @NotEmpty
  @jakarta.validation.constraints.NotEmpty
  @Value.Parameter(order = 1)
  String getVariant();

  @Nullable
  @jakarta.annotation.Nullable
  @Schema(type = SchemaType.OBJECT)
  @Value.Parameter(order = 2)
  @JsonInclude(Include.NON_NULL)
  JsonNode getMetadata();

  static ContentMetadata of(String variant, JsonNode metadata) {
    return ImmutableContentMetadata.of(variant, metadata);
  }
}
