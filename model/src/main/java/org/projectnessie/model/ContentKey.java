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

import static org.projectnessie.model.Util.DOT_STRING;
import static org.projectnessie.model.Util.GROUP_SEPARATOR_STRING;
import static org.projectnessie.model.Util.ZERO_BYTE_STRING;

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
    return Namespace.of(getElements().subList(0, getElements().size() - 1));
  }

  @JsonIgnore
  @Value.Redacted
  public String getName() {
    return getElements().get(getElements().size() - 1);
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
    for (String e : elements) {
      if (e == null) {
        throw new IllegalArgumentException(
            String.format("Content key '%s' must not contain a null element.", elements));
      }
      if (e.contains(ZERO_BYTE_STRING) || e.contains(GROUP_SEPARATOR_STRING)) {
        throw new IllegalArgumentException(
            String.format(
                "Content key '%s' must not contain a zero byte (\\u0000) / group separator (\\u001D).",
                elements));
      }
      if (e.isEmpty()) {
        throw new IllegalArgumentException(
            String.format("Content key '%s' must not contain an empty element.", elements));
      }
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

  @Value.Check
  protected void check() {
    if (getElements().stream().mapToInt(String::length).sum() > MAX_LENGTH) {
      throw new IllegalStateException("Key too long, max allowed length: " + MAX_LENGTH);
    }
    if (getElements().size() > MAX_ELEMENTS) {
      throw new IllegalStateException(
          "Key too long, max allowed number of elements: " + MAX_ELEMENTS);
    }
  }
}
