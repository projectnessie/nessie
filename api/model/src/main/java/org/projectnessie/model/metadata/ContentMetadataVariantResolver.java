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
package org.projectnessie.model.metadata;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.DatabindContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.jsontype.impl.TypeIdResolverBase;
import org.projectnessie.model.ContentMetadata;

/** Dynamic {@link ContentMetadata} object (de)serialization for <em>Jackson</em>. */
public final class ContentMetadataVariantResolver extends TypeIdResolverBase {

  private JavaType baseType;

  public ContentMetadataVariantResolver() {}

  @Override
  public void init(JavaType bt) {
    baseType = bt;
  }

  @Override
  public String idFromValue(Object value) {
    return getId(value);
  }

  @Override
  public String idFromValueAndType(Object value, Class<?> suggestedType) {
    return getId(value);
  }

  @Override
  public JsonTypeInfo.Id getMechanism() {
    return JsonTypeInfo.Id.CUSTOM;
  }

  private String getId(Object value) {
    if (value instanceof ContentMetadata) {
      return ((ContentMetadata) value).getVariant();
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
