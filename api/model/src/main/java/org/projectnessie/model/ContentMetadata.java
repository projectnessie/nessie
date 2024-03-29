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

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver;
import javax.validation.constraints.NotEmpty;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.projectnessie.model.metadata.ContentMetadataVariantResolver;

@Schema(type = SchemaType.OBJECT, title = "ContentMetadata", discriminatorProperty = "variant")
@JsonTypeIdResolver(ContentMetadataVariantResolver.class)
@JsonTypeInfo(use = JsonTypeInfo.Id.CUSTOM, property = "variant", visible = true)
public interface ContentMetadata {

  @NotEmpty
  @jakarta.validation.constraints.NotEmpty
  String getVariant();
}
