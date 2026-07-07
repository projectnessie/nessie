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
package org.projectnessie.catalog.model.schema.types;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.util.Locale;

/** Jackson 3 helper class to resolve a {@link NessieType} in a {@link NessieTypeSpec}. */
public class NessieTypeIdResolver3 extends tools.jackson.databind.jsontype.impl.TypeIdResolverBase {
  private tools.jackson.databind.JavaType baseType;

  public NessieTypeIdResolver3() {}

  @Override
  public void init(tools.jackson.databind.JavaType bt) {
    baseType = bt;
  }

  @Override
  public String idFromValue(tools.jackson.databind.DatabindContext context, Object value) {
    return getId(value);
  }

  @Override
  public String idFromValueAndType(
      tools.jackson.databind.DatabindContext context, Object value, Class<?> suggestedType) {
    return getId(value);
  }

  @Override
  public JsonTypeInfo.Id getMechanism() {
    return JsonTypeInfo.Id.CUSTOM;
  }

  private String getId(Object value) {
    if (value instanceof NessieTypeSpec nessieTypeSpec) {
      return nessieTypeSpec.type().lowerCaseName();
    }
    return null;
  }

  @Override
  public tools.jackson.databind.JavaType typeFromId(
      tools.jackson.databind.DatabindContext context, String id) {
    NessieType subType = NessieType.valueOf(id.toUpperCase(Locale.ROOT));
    Class<? extends NessieTypeSpec> asType = subType.typeSpec();
    if (baseType.getRawClass().isAssignableFrom(asType)) {
      return context.constructSpecializedType(baseType, asType);
    }

    @SuppressWarnings("unchecked")
    Class<? extends NessieTypeSpec> concrete =
        (Class<? extends NessieTypeSpec>) baseType.getRawClass();
    return context.constructSpecializedType(baseType, concrete);
  }
}
