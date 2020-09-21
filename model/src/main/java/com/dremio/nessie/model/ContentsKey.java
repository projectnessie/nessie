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

import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.ws.rs.ext.ParamConverter;
import javax.ws.rs.ext.ParamConverterProvider;
import javax.ws.rs.ext.Provider;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Key for the contents of an object.
 *
 * <p>For URL encoding, embedded periods within a segment are replaced with zero byte values before passing in a url string.
 */
public class ContentsKey {
  private static final char ZERO_BYTE = '\u0000';
  private static final String ZERO_BYTE_STRING = Character.toString(ZERO_BYTE);

  private final List<String> elements;

  @JsonCreator
  public ContentsKey(@JsonProperty("elements") List<String> elements) {
    this.elements = Collections.unmodifiableList(new ArrayList<>(elements));
    validate();
  }

  // internal constructor for a list that doesn't need a defensive copy.
  private ContentsKey(@JsonProperty("elements") List<String> elements, boolean dummy) {
    this.elements = Collections.unmodifiableList(elements);
    validate();
  }

  public static ContentsKey of(String... elements) {
    return new ContentsKey(Arrays.asList(elements), true);
  }

  public static ContentsKey of(List<String> elements) {
    return new ContentsKey(elements);
  }

  public List<String> getElements() {
    return elements;
  }

  private void validate() {
    for (String e : elements) {
      if (e.contains(ZERO_BYTE_STRING)) {
        throw new IllegalArgumentException("A object key cannot contain a zero byte.");
      }
    }
  }

  private static class NessieObjectKeyConverter implements ParamConverter<ContentsKey> {

    @Override
    public ContentsKey fromString(String value) {
      if (value == null) {
        return null;
      }

      return fromEncoded(value);
    }

    @Override
    public String toString(ContentsKey value) {
      if (value == null) {
        return null;
      }
      return value.toPathString();
    }

  }

  @Provider
  public static class NessieObjectKeyConverterProvider implements ParamConverterProvider {

    @SuppressWarnings("unchecked")
    @Override
    public <T> ParamConverter<T> getConverter(Class<T> rawType, Type genericType, Annotation[] annotations) {
      if (rawType.equals(ContentsKey.class)) {
        return (ParamConverter<T>) new NessieObjectKeyConverter();
      }
      return null;
    }

  }

  /**
   * Convert from path encoded string to normal string.
   * @param encoded Path encoded string
   * @return Actual key.
   */
  public static ContentsKey fromEncoded(String encoded) {
    List<String> elements = StreamSupport.stream(Arrays.spliterator(encoded.split("\\.")), false)
        .map(x -> x.replace('\u0000', '.')).collect(Collectors.toList());
    return new ContentsKey(elements, true);
  }

  @Override
  public int hashCode() {
    return Objects.hash(elements);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof ContentsKey)) {
      return false;
    }
    ContentsKey other = (ContentsKey) obj;
    return Objects.equals(elements, other.elements);
  }

  @Override
  public String toString() {
    return elements.stream().collect(Collectors.joining("."));
  }

  /**
   * Convert this key to a url encoded path string.
   * @return String encoded for path use.
   */
  public String toPathString() {
    String pathString = getElements().stream().map(x -> x.replace('.', '\u0000')).collect(Collectors.joining("."));
    return pathString;
  }
}
