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
    Objects.requireNonNull(elements, "elements argument is null");
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
   * Parses the path encoded string to a {@link ContentKey} object, supports all Nessie Spec
   * versions.
   *
   * <p>The {@code encoded} parameter is split at dot ({@code .}) characters.
   *
   * <p>Legacy compatibility: a dot ({@code .}) character can be represented using ASCII 31 (0x1F)
   * or ASCII 0 (0x00).
   *
   * <p>Escaping, for compatibility w/ <a
   * href="https://jakarta.ee/specifications/servlet/6.0/jakarta-servlet-spec-6.0.html#uri-path-canonicalization">Jakarta
   * Servlet Specification 6, URI Path Canonicalization</a> and <a
   * href="https://datatracker.ietf.org/doc/html/rfc3986#section-3.3">RFC 3986, section 3.3</a>.
   * This is available with Nessie Spec version 2.2.0, as advertised via {@link
   * NessieConfiguration#getSpecVersion()}.
   *
   * <ul>
   *   <li>Fact: two consecutive dots ({@code ..}) would represent an <em>empty</em> namespace
   *       elements, which is illegal. It is correct to assume that this character sequence does not
   *       occur in encoded content keys.
   *   <li>The sequence {@code ..*} is the encoded representation for a single dot character {@code
   *       .}.
   *   <li>The sequence {@code ..^} is the encoded representation for a slash {@code /}, which is a
   *       URI path separator.
   *   <li>The sequence {@code ..=} is the encoded representation for a backslash {@code \}, which
   *       is a URI path separator.
   *   <li>The sequence {@code ..=} is the encoded representation for a hash {@code #}, which is a
   *       URI path separator.
   *   <li>The sequence {@code ..} followed by any character not mentioned in the sequences above,
   *       means that the first dot represents an element boundary, decoding should continue after
   *       the first {@code .} character.
   * </ul>
   *
   * <p>This function can decode the representations returned by {@link #toPathString()}, {@link
   * #toPathStringEscaped()} and {@link #toCanonicalString()}.
   *
   * @param encoded Path encoded string, as returned by {@link #toPathString()}, {@link
   *     #toPathStringEscaped()} and {@link #toCanonicalString()}.
   * @return Actual key.
   */
  public static ContentKey fromPathString(String encoded) {
    return ContentKey.of(Util.fromPathString(encoded));
  }

  /**
   * Convert these elements to a URI path/query string, which <em>may violate</em> <a
   * href="https://jakarta.ee/specifications/servlet/6.0/jakarta-servlet-spec-6.0#uri-path-canonicalization">Jakarta
   * Servlet Spec 6, chapter 3.5.2 URI Path Canonicalization</a>, because it uses so-called control
   * characters and ambiguous path elements.
   *
   * <p>Users must prefer {@link #toPathStringEscaped()}, if the service support Nessie spec version
   * 2.2.0 or newer.
   *
   * @return The URI path compatible representation of the given elements, possibly escaped. The
   *     returned value should be URL-encoded before added to a URI path or query.
   */
  public String toPathString() {
    return Util.toPathString(getElements());
  }

  /**
   * Convert these elements to a URI path/query string, which <em>may violate</em> <a
   * href="https://jakarta.ee/specifications/servlet/6.0/jakarta-servlet-spec-6.0#uri-path-canonicalization">Jakarta
   * Servlet Spec 6, chapter 3.5.2 URI Path Canonicalization</a>, because it uses so-called control
   * characters and ambiguous path elements.
   *
   * <p>Users must prefer {@link #toPathStringEscaped()}, if the service support Nessie spec version
   * 2.2.0 or newer.
   *
   * @return The URI path compatible representation of the given elements, possibly escaped. The
   *     returned value should be URL-encoded before added to a URI path or query.
   */
  public String toPathStringControlChars() {
    return Util.toPathString(getElements());
  }

  /**
   * Escapes content-key elements into a URI path/query compatible form that does not violate <a
   * href="https://jakarta.ee/specifications/servlet/6.0/jakarta-servlet-spec-6.0#uri-path-canonicalization">Jakarta
   * Servlet Spec 6, chapter 3.5.2 URI Path Canonicalization</a> by escaping the characters {@code
   * /}, {@code \}, {@code %}, and if escaping also the {@code .}.
   *
   * <p>Some examples:
   *
   * <ul>
   *   <li>{@code ["foo", "bar", "baz"]} returned as {@code "foo.bar.baz"} - no escaping needed.
   *   <li>{@code ["foo", ".bar", "baz"]} returned as {@code "foo..._bar.baz"} - escaping needed
   *       with the 2nd element. The first dot is the element separator. The 2nd dot is the first
   *       character of the second element, indicating that escaping starts at this element. {@code
   *       ._} is the escape sequence for the {@code .} character.
   *   <li>{@code ["foo.", ".bar", "a/\%aa"]} returned as {@code ".foo._.._bar.a.{.}.[aa"}
   * </ul>
   *
   * <p>The returned value is always processable by Nessie services announcing Nessie spec 2.2.0 or
   * newer.
   *
   * @return The URI path compatible representation of the given elements, possibly escaped. The
   *     returned value should be URL-encoded before added to a URI path.
   */
  public String toPathStringEscaped() {
    return Util.toPathStringEscaped(getElements());
  }

  /**
   * Escapes content-key elements into a canonical form that escapes {@code .} characters. The
   * returned format is similar to {@linkplain #toPathStringEscaped()}, but does not escape
   * problematic URI characters.
   *
   * <p>Some examples:
   *
   * <ul>
   *   <li>{@code ["foo", "bar", "baz"]} returned as {@code "foo.bar.baz"} - no escaping needed.
   *   <li>{@code ["foo", ".bar", "baz"]} returned as {@code "foo..._bar.baz"} - escaping needed
   *       with the 2nd element. The first dot is the element separator. The 2nd dot is the first
   *       character of the second element, indicating that escaping starts at this element. {@code
   *       ._} is the escape sequence for the {@code .} character.
   *   <li>{@code ["foo.", ".bar", "a/\%aa"]} returned as {@code ".foo._.._bar.a/\%aa"}
   *   <li>{@code ["foo", "bar", "a/\%aa"]} returned as {@code "foo.bar..a/\%aa"}
   * </ul>
   *
   * @return The canonical representation of the given elements, possibly escaped. The returned
   *     value should <em>not</em> be used in a URI path.
   */
  public String toCanonicalString() {
    return Util.toCanonicalString(getElements());
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
