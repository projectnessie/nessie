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
package org.projectnessie.model.types;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.DatabindContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.jsontype.impl.TypeIdResolverBase;
import java.util.Locale;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Content;
import org.projectnessie.model.Detached;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Reference.ReferenceType;
import org.projectnessie.model.Tag;

/** Dynamic {@link Content} object (de)serialization for <em>Jackson</em>. */
public final class ReferenceTypeIdResolver extends TypeIdResolverBase {

  private JavaType baseType;

  public ReferenceTypeIdResolver() {}

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
    if (value instanceof Detached) {
      return "DETACHED";
    }
    if (value instanceof Reference) {
      return ((Reference) value).getType().name();
    }

    return null;
  }

  @Override
  public JavaType typeFromId(DatabindContext context, String id) {
    if (id == null) {
      return null;
    }
    id = id.toUpperCase(Locale.ROOT);
    if ("DETACHED".equals(id)) {
      return context.constructSpecializedType(baseType, Detached.class);
    }
    ReferenceType type = ReferenceType.parse(id);
    if (type == null) {
      throw new IllegalArgumentException("No type for reference");
    }
    switch (type) {
      case TAG:
        return context.constructSpecializedType(baseType, Tag.class);
      case BRANCH:
        return context.constructSpecializedType(baseType, Branch.class);
      default:
        throw new IllegalArgumentException();
    }
  }
}
