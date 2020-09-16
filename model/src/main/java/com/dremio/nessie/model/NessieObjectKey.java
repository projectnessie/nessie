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
package com.dremio.nessie.model;

import java.io.UnsupportedEncodingException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.ws.rs.ext.ParamConverter;
import javax.ws.rs.ext.ParamConverterProvider;
import javax.ws.rs.ext.Provider;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

public class NessieObjectKey {

  private static final String SLASH = "/";

  private final ImmutableList<String> elements;

  @JsonCreator
  public NessieObjectKey(@JsonProperty("elements") List<String> elements) {
    this.elements = ImmutableList.copyOf(elements);
  }

  public List<String> getElements() {
    return elements;
  }


  private static class NessieObjectKeyConverter implements ParamConverter<NessieObjectKey> {

    @Override
    public NessieObjectKey fromString(String value) {
      if (value == null) {
        return null;
      }

      List<String> elements = StreamSupport.stream(Arrays.spliterator(value.split(SLASH)), false)
          .map(x -> {
            try {
              return URLDecoder.decode(x, StandardCharsets.UTF_8.toString());
            } catch (UnsupportedEncodingException e) {
              throw new RuntimeException(String.format("Unable to decode string %s", x), e);
            }
          }).collect(ImmutableList.toImmutableList());
      return new NessieObjectKey(elements);
    }

    @Override
    public String toString(NessieObjectKey value) {
      if (value == null) {
        return null;
      }

      return value.getElements().stream().map(x -> {
        try {
          return URLEncoder.encode(x, StandardCharsets.UTF_8.toString());
        } catch (UnsupportedEncodingException e) {
          throw new RuntimeException(String.format("Unable to decode string %s", x), e);
        }
      }).collect(Collectors.joining(SLASH));
    }

  }

  @Provider
  public static class NessieObjectKeyConverterProvider implements ParamConverterProvider {

    @SuppressWarnings("unchecked")
    @Override
    public <T> ParamConverter<T> getConverter(Class<T> rawType, Type genericType, Annotation[] annotations) {
      if (rawType.equals(NessieObjectKey.class)) {
        return (ParamConverter<T>) new NessieObjectKeyConverter();
      }
      return null;
    }

  }
}
