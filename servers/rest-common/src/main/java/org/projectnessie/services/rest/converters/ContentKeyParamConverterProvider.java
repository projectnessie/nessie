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
import org.projectnessie.model.ContentKey;

/**
 * JAX-RS parameter converter provider to transform {@code String} into {@code ContentKey}, and
 * vice-versa.
 */
@Provider
public class ContentKeyParamConverterProvider implements ParamConverterProvider {

  private static final class ContentKeyParamConverter implements ParamConverter<ContentKey> {

    @Override
    public ContentKey fromString(String value) {
      if (value == null) {
        return null;
      }

      return ContentKey.fromPathString(value);
    }

    @Override
    public String toString(ContentKey value) {
      if (value == null) {
        return null;
      }
      return value.toPathString();
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> ParamConverter<T> getConverter(
      Class<T> rawType, Type genericType, Annotation[] annotations) {
    if (rawType.equals(ContentKey.class)) {
      return (ParamConverter<T>) new ContentKeyParamConverter();
    }
    return null;
  }
}
