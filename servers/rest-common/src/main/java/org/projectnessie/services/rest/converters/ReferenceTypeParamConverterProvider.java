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
package org.projectnessie.services.rest.converters;

import jakarta.ws.rs.ext.ParamConverter;
import jakarta.ws.rs.ext.ParamConverterProvider;
import jakarta.ws.rs.ext.Provider;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.Locale;
import org.projectnessie.model.Reference.ReferenceType;

/**
 * Provider for {@link ReferenceTypeParamConverter}, to convert between lower-case representations
 * of {@link ReferenceType} in REST paths and upper-case in the Java enum..
 */
@Provider
public class ReferenceTypeParamConverterProvider implements ParamConverterProvider {

  private static final ReferenceTypeParamConverter REFERENCE_TYPE_PARAM_CONVERTER =
      new ReferenceTypeParamConverter();

  @Override
  public <T> ParamConverter<T> getConverter(
      Class<T> rawType, Type genericType, Annotation[] annotations) {
    if (rawType == ReferenceType.class) {
      @SuppressWarnings("unchecked")
      ParamConverter<T> r = (ParamConverter<T>) REFERENCE_TYPE_PARAM_CONVERTER;
      return r;
    }
    return null;
  }

  static final class ReferenceTypeParamConverter implements ParamConverter<ReferenceType> {

    @Override
    public ReferenceType fromString(String value) {
      return ReferenceType.valueOf(value.toUpperCase(Locale.ROOT));
    }

    @Override
    public String toString(ReferenceType value) {
      return value.name().toLowerCase(Locale.ROOT);
    }
  }
}
