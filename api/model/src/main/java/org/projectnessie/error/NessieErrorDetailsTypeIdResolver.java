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

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.DatabindContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.jsontype.impl.TypeIdResolverBase;

public class NessieErrorDetailsTypeIdResolver extends TypeIdResolverBase {

  private JavaType baseType;

  public NessieErrorDetailsTypeIdResolver() {}

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
    if (value instanceof NessieErrorDetails) {
      return ((NessieErrorDetails) value).getType();
    }

    return null;
  }

  @Override
  public JavaType typeFromId(DatabindContext context, String id) {
    Class<? extends NessieErrorDetails> asType;
    switch (id) {
      case ReferenceConflicts.TYPE:
        asType = ReferenceConflicts.class;
        break;
      case ContentKeyErrorDetails.TYPE:
        asType = ContentKeyErrorDetails.class;
        break;
      default:
        asType = GenericErrorDetails.class;
        break;
    }
    if (baseType.getRawClass().isAssignableFrom(asType)) {
      return context.constructSpecializedType(baseType, asType);
    }

    // This is rather a "test-only" code path, but it might happen in real life as well, when
    // calling the ObjectMapper with a "too specific" type and not just NessieErrorDetails.class.
    // So we can get here for example, if the baseType (induced by the type passed to ObjectMapper),
    // is GenericErrorDetails.class, but the type is a "well known" type like
    // ReferenceConflicts.class.
    @SuppressWarnings("unchecked")
    Class<? extends NessieErrorDetails> concrete =
        (Class<? extends NessieErrorDetails>) baseType.getRawClass();
    return context.constructSpecializedType(baseType, concrete);
  }
}
