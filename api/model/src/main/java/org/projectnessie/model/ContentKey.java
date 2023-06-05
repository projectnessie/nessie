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

import static java.lang.String.format;
import static org.projectnessie.model.Util.DOT_STRING;
import static org.projectnessie.model.Util.FIRST_ALLOWED_KEY_CHAR;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import org.immutables.value.Value;

/**
 * Key for the content of an object.
 *
 * <p>For URL encoding, embedded periods within a segment are replaced with zero byte values before
 * passing in a url string.
 */
@Value.Immutable
@JsonSerialize(as = ImmutableContentKey.class)
@JsonDeserialize(as = ImmutableContentKey.class)
public abstract class ContentKey implements Comparable<ContentKey> {

  /** Maximum number of characters in a key. Note: characters can take up to 3 bytes via UTF-8. */
  public static final int MAX_LENGTH = 500;

  /** Maximum number of elements. */
  public static final int MAX_ELEMENTS = 20;

  @NotNull
  @jakarta.validation.constraints.NotNull
  @Size
  @jakarta.validation.constraints.Size(min = 1)
  @Value.Parameter(order = 1)
  public abstract List<String> getElements();

  @JsonIgnore
  @Value.Redacted
  public String[] getElementsArray() {
    return getElements().toArray(new String[0]);
  }

  @JsonIgnore
  @Value.Redacted
  public int getElementCount() {
    return getElements().size();
  }

  /**
   * Returns the namespace that is always consisting of the first <b>N-1</b> elements from {@link
   * ContentKey#getElements()}.
   *
   * @return A {@link Namespace} instance that is always consisting of the first <b>N-1</b> elements
   *     from {@link ContentKey#getElements()}.
   */
  @JsonIgnore
  @Value.Redacted
  public Namespace getNamespace() {
    return Namespace.of(getElements().subList(0, getElementCount() - 1));
  }

  @JsonIgnore
  @Value.Redacted
  public String getName() {
    return getElements().get(getElementCount() - 1);
  }

  @JsonIgnore
  @Value.Redacted
  public ContentKey getParent() {
    List<String> elements = getElements();
    if (elements.size() <= 1) {
      throw new IllegalArgumentException("ContentKey has no parent");
    }
    return ContentKey.of(elements.subList(0, elements.size() - 1));
  }

  /**
   * Return the content key, truncated to the given maximum length if the number of elements is
   * greater than {@code targetMaxLength}, otherwise returns this content key.
   */
  public ContentKey truncateToLength(int targetMaxLength) {
    List<String> elements;
    elements = getElements();
    List<String> truncated = truncateToLengthElements(elements, targetMaxLength);
    if (truncated == elements) {
      return this;
    }
    return ContentKey.of(truncated);
  }

  private static List<String> truncateToLengthElements(List<String> elements, int targetMaxLength) {
    int len = elements.size();
    if (len <= targetMaxLength) {
      return elements;
    }
    return elements.subList(0, targetMaxLength);
  }

  public boolean startsWith(ContentKey other) {
    List<String> elements = getElements();
    List<String> otherElements = other.getElements();
    return startsWith(elements, otherElements);
  }

  public boolean startsWith(Namespace other) {
    List<String> elements = getElements();
    List<String> otherElements = other.getElements();
    return startsWith(elements, otherElements);
  }

  private static boolean startsWith(List<String> elements, List<String> otherElements) {
    int len = elements.size();
    int otherLen = otherElements.size();
    if (otherLen > len) {
      return false;
    }
    for (int i = 0; i < otherLen; i++) {
      if (!elements.get(i).equals(otherElements.get(i))) {
        return false;
      }
    }
    return true;
  }

  public static ContentKey of(Namespace namespace, String name) {
    ImmutableContentKey.Builder b = ImmutableContentKey.builder();
    if (namespace != null && !namespace.isEmpty()) {
      b.elements(namespace.getElements());
    }
    return b.addElements(name).build();
  }

  public static ContentKey of(String... elements) {
    Objects.requireNonNull(elements, "Elements array must not be null");
    return ImmutableContentKey.of(Arrays.asList(elements));
  }

  @JsonCreator
  public static ContentKey of(@JsonProperty("elements") List<String> elements) {
    Objects.requireNonNull(elements);
    return ImmutableContentKey.of(elements);
  }

  @Value.Check
  protected void validate() {
    List<String> elements = getElements();
    if (elements.size() > MAX_ELEMENTS) {
      throw new IllegalStateException(
          "Key too long, max allowed number of elements: " + MAX_ELEMENTS);
    }
    int sum = 0;
    for (String e : elements) {
      if (e == null) {
        throw new IllegalArgumentException(
            format("Content key '%s' must not contain a null element.", elements));
      }
      int l = e.length();
      if (l == 0) {
        throw new IllegalArgumentException(
            format("Content key '%s' must not contain an empty element.", elements));
      }
      for (int i = 0; i < l; i++) {
        char c = e.charAt(i);
        if (c < FIRST_ALLOWED_KEY_CHAR) {
          throw new IllegalArgumentException(
              format(
                  "Content key '%s' must not contain characters less than 0x%2h.",
                  elements, FIRST_ALLOWED_KEY_CHAR));
        }
      }
      sum += l;
    }
    if (sum > MAX_LENGTH) {
      throw new IllegalStateException("Key too long, max allowed length: " + MAX_LENGTH);
    }
  }

  /**
   * Convert from path encoded string to normal string.
   *
   * @param encoded Path encoded string
   * @return Actual key.
   */
  public static ContentKey fromPathString(String encoded) {
    return ContentKey.of(Util.fromPathString(encoded));
  }

  /**
   * Convert this key to a url encoded path string.
   *
   * @return String encoded for path use.
   */
  public String toPathString() {
    return Util.toPathString(getElements());
  }

  @Override
  public String toString() {
    return String.join(DOT_STRING, getElements());
  }

  @Override
  public int hashCode() {
    int h = 1;
    for (String element : getElements()) {
      h = 31 * h + element.hashCode();
    }
    return h;
  }

  @Override
  public final int compareTo(ContentKey that) {
    List<String> a = this.getElements();
    List<String> b = that.getElements();
    int max = Math.min(a.size(), b.size());
    for (int i = 0; i < max; i++) {
      int cmp = a.get(i).compareTo(b.get(i));
      if (cmp != 0) {
        return cmp;
      }
    }

    return a.size() - b.size();
  }
}
