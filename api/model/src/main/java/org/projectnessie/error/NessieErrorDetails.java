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
package org.projectnessie.error;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.DiscriminatorMapping;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.immutables.value.Value;

@Schema(
    type = SchemaType.OBJECT,
    title = "NessieErrorDetails",
    oneOf = {ReferenceConflicts.class},
    discriminatorMapping = {
      @DiscriminatorMapping(value = ReferenceConflicts.TYPE, schema = ReferenceConflicts.class),
      @DiscriminatorMapping(
          value = ContentKeyErrorDetails.TYPE,
          schema = ContentKeyErrorDetails.class)
    },
    discriminatorProperty = "type")
@JsonTypeIdResolver(NessieErrorDetailsTypeIdResolver.class)
@JsonTypeInfo(use = JsonTypeInfo.Id.CUSTOM, property = "type", visible = true)
@JsonIgnoreProperties(ignoreUnknown = true)
public interface NessieErrorDetails {
  @Value.Redacted
  @JsonIgnore
  String getType();
}
