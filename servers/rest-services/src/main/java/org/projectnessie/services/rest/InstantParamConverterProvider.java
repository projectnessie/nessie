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
import java.time.Instant;
import java.time.format.DateTimeParseException;
import javax.ws.rs.ext.ParamConverter;
import javax.ws.rs.ext.ParamConverterProvider;
import javax.ws.rs.ext.Provider;

/**
 * JAX-RS parameter converter provider to transform {@code String} into {@code Instant}, and
 * vice-versa.
 */
@Provider
public class InstantParamConverterProvider implements ParamConverterProvider {

  private static final class InstantParamConverter implements ParamConverter<Instant> {

    @Override
    public Instant fromString(String instant) {
      if (null == instant) {
        return null;
      }
      try {
        return Instant.parse(instant);
      } catch (DateTimeParseException e) {
        throw new IllegalArgumentException(
            String.format("'%s' could not be parsed to an Instant in ISO-8601 format", instant), e);
      }
    }

    @Override
    public String toString(Instant instant) {
      if (instant == null) {
        return null;
      }
      return instant.toString();
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> ParamConverter<T> getConverter(
      Class<T> rawType, Type genericType, Annotation[] annotations) {
    if (rawType.equals(Instant.class)) {
      return (ParamConverter<T>) new InstantParamConverter();
    }
    return null;
  }
}
