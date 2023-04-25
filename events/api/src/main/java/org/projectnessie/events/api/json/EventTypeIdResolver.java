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
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.jsontype.impl.TypeIdResolverBase;
import java.util.Locale;
import org.projectnessie.events.api.Event;
import org.projectnessie.events.api.EventType;

public final class EventTypeIdResolver extends TypeIdResolverBase {

  private JavaType baseType;

  @Override
  public void init(JavaType bt) {
    baseType = bt;
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
  public String idFromValue(Object value) {
    if (value instanceof Event) {
      return ((Event) value).getType().name();
    }
    return null;
  }

  @Override
  public JavaType typeFromId(DatabindContext context, String id) {
    Class<?> subtype;
    try {
      subtype = EventType.valueOf(id.toUpperCase(Locale.ROOT)).getSubtype();
    } catch (Exception e) {
      subtype = EventType.CUSTOM.getSubtype();
    }
    assert baseType.getRawClass().isAssignableFrom(subtype);
    return context.constructSpecializedType(baseType, subtype);
  }
}
