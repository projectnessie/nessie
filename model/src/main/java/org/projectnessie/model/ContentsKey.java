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
package org.projectnessie.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

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

  // internal constructor for a list that doesn't need a defensive copy.
  private ContentsKey(List<String> elements) {
    this.elements = Collections.unmodifiableList(elements);
    validate();
  }

  public static ContentsKey of(String... elements) {
    return new ContentsKey(Arrays.asList(elements));
  }

  @JsonCreator
  public static ContentsKey of(@JsonProperty("elements") List<String> elements) {
    return new ContentsKey(new ArrayList<>(elements));
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

  /**
   * Convert from path encoded string to normal string.
   * @param encoded Path encoded string
   * @return Actual key.
   */
  public static ContentsKey fromPathString(String encoded) {
    List<String> elements = Arrays.stream(encoded.split("\\."))
        .map(x -> x.replace('\u0000', '.')).collect(Collectors.toList());
    return new ContentsKey(elements);
  }

  /**
   * Convert this key to a url encoded path string.
   * @return String encoded for path use.
   */
  public String toPathString() {
    String pathString = getElements().stream().map(x -> x.replace('.', '\u0000')).collect(Collectors.joining("."));
    return pathString;
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
}
