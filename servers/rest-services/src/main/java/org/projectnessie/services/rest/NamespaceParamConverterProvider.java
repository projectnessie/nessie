/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie.services.rest;

import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import javax.ws.rs.ext.ParamConverter;
import javax.ws.rs.ext.ParamConverterProvider;
import javax.ws.rs.ext.Provider;
import org.projectnessie.model.Namespace;

/**
 * JAX-RS parameter converter provider to transform {@code String} into {@code Namespace}, and
 * vice-versa.
 */
@Provider
public class NamespaceParamConverterProvider implements ParamConverterProvider {

  private static final class NamespaceParamConverter implements ParamConverter<Namespace> {

    @Override
    public Namespace fromString(String value) {
      if (value == null) {
        return null;
      }

      return Namespace.fromPathString(value);
    }

    @Override
    public String toString(Namespace value) {
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
    if (rawType.equals(Namespace.class)) {
      return (ParamConverter<T>) new NamespaceParamConverter();
    }
    return null;
  }
}
