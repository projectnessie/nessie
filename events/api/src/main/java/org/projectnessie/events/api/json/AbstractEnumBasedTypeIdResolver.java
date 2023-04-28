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
package org.projectnessie.events.api.json;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.DatabindContext;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.jsontype.impl.TypeIdResolverBase;
import java.io.IOException;

public abstract class AbstractEnumBasedTypeIdResolver extends TypeIdResolverBase {

  private JavaType targetType;

  @Override
  public void init(JavaType bt) {
    targetType = bt;
  }

  @Override
  public JsonTypeInfo.Id getMechanism() {
    return JsonTypeInfo.Id.CUSTOM;
  }

  @Override
  public String idFromValueAndType(Object value, Class<?> suggestedType) {
    return idFromValue(value);
  }

  @Override
  public JavaType typeFromId(DatabindContext context, String id) throws IOException {
    Class<?> subtype;
    try {
      subtype = subtypeFromId(id);
    } catch (Exception e) {
      // Type id is not a valid enum value; this may mean that this client is older than the
      // server, and the server has added a new event type. Try to convert to the custom subtype.
      subtype = customSubtype();
    }
    // Caller either requested the base type, or the requested subtype is compatible with the
    // actual subtype.
    if (targetType.getRawClass().isAssignableFrom(subtype)) {
      return context.constructSpecializedType(targetType, subtype);
    }
    // Caller explicitly asked to convert to the special custom subtype, which is always
    // possible.
    if (targetType.isTypeOrSubTypeOf(customSubtype())) {
      return context.constructSpecializedType(targetType, customSubtype());
    }
    // Caller asked for a specific subtype, but that subtype is not assignable to the actual
    // subtype.
    throw JsonMappingException.from(
        (DeserializationContext) context,
        String.format("Type id %s is not convertible to %s", id, targetType.getRawClass()));
  }

  protected abstract Class<?> subtypeFromId(String id);

  protected abstract Class<?> customSubtype();
}
