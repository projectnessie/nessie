/*
 * Copyright (C) 2026 Dremio
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
package org.projectnessie.model.metadata;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.projectnessie.model.ContentMetadata;
import tools.jackson.databind.DatabindContext;
import tools.jackson.databind.JavaType;
import tools.jackson.databind.jsontype.impl.TypeIdResolverBase;

/** Dynamic {@link ContentMetadata} object (de)serialization for <em>Jackson 3</em>. */
public final class Jackson3ContentMetadataVariantResolver extends TypeIdResolverBase {

  private JavaType baseType;

  public Jackson3ContentMetadataVariantResolver() {}

  @Override
  public void init(JavaType bt) {
    baseType = bt;
  }

  @Override
  public String idFromValue(DatabindContext context, Object value) {
    return getId(value);
  }

  @Override
  public String idFromValueAndType(DatabindContext context, Object value, Class<?> suggestedType) {
    return getId(value);
  }

  @Override
  public JsonTypeInfo.Id getMechanism() {
    return JsonTypeInfo.Id.CUSTOM;
  }

  private String getId(Object value) {
    if (value instanceof ContentMetadata contentMetadata) {
      return contentMetadata.getVariant();
    }

    return null;
  }

  @Override
  public JavaType typeFromId(DatabindContext context, String id) {
    // Currently there are no specialized ContentMetadata implementations, so this one always
    // returns GenericContentMetadata.
    return context.constructSpecializedType(baseType, GenericContentMetadata.class);
  }
}
