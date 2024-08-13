/*
 * Copyright (C) 2024 Dremio
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
import static org.projectnessie.model.ContentKey.MAX_ELEMENTS;
import static org.projectnessie.model.ContentKey.MAX_LENGTH;
import static org.projectnessie.model.Util.FIRST_ALLOWED_KEY_CHAR;

import java.util.List;

public interface Elements {
  List<String> getElements();

  String[] getElementsArray();

  int getElementCount();

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
   * <p>This function can decode the representations returned by {@link #toPathString()}, {@link
   * #toPathStringEscaped()} and {@link #toCanonicalString()}. See those functions for a description
   * of the possible characters and escape mechanism(s).
   *
   * @param encoded Path encoded string, as returned by {@link #toPathString()}, {@link
   *     #toPathStringEscaped()} and {@link #toCanonicalString()}.
   * @return Actual key.
   */
  static List<String> elementsFromPathString(String encoded) {
    return Util.fromPathString(encoded);
  }

  /**
   * Convert these elements to a URI path/query string, which <em>may violate</em> <a
   * href="https://jakarta.ee/specifications/servlet/6.0/jakarta-servlet-spec-6.0#uri-path-canonicalization">Jakarta
   * Servlet Spec 6, chapter 3.5.2 URI Path Canonicalization</a>, because it uses so-called control
   * characters and ambiguous path elements.
   *
   * <p>Users must prefer {@link #toPathStringEscaped()}, if the service supports Nessie spec
   * version 2.2.0 or newer, except for content-key related values in CEL filters.
   *
   * <p>Elements are separated by {@code .} characters. {@code .} characters in elements are
   * replaced with the ASCII group separator (ASCII 29, {@code %1D}).
   *
   * @return The URI path compatible representation of the given elements, possibly escaped. The
   *     returned value should be URL-encoded before added to a URI path or query. The returned
   *     value can be parsed with {@link #elementsFromPathString(String)}.
   */
  default String toPathString() {
    return Util.toPathString(getElements());
  }

  /**
   * Escapes content-key elements into a URI path/query compatible form that does not violate <a
   * href="https://jakarta.ee/specifications/servlet/6.0/jakarta-servlet-spec-6.0#uri-path-canonicalization">Jakarta
   * Servlet Spec 6, chapter 3.5.2 URI Path Canonicalization</a> by escaping the problematic
   * characters.
   *
   * <p>The characters {@code .}, {@code /}, {@code \} and {@code %} trigger escaping (see {@link
   * #toCanonicalString()}). The escaped representation starts with a single {@code .}.
   *
   * <p>If no escaping is necessary, the returned string is the same as {@code String.join(".",
   * getElements())}, the elements are joined to a string using {@code .} as the delimiter. For
   * example {@code ["foo", "bar"]} yields {@code "foo.bar"}.
   *
   * <p>If escaping is necessary:
   *
   * <ul>
   *   <li>The returned string starts with a {@code .}
   *   <li>{@code .} is escaped as {@code *_}
   *   <li>{@code *} is escaped as {@code **}
   *   <li>{@code /} is escaped as <code>*{</code> (not for {@link #toCanonicalString()})
   *   <li>{@code \} is escaped as <code>*}</code> (not for {@link #toCanonicalString()})
   *   <li>{@code %} is escaped as {@code *[} (not for {@link #toCanonicalString()})
   * </ul>
   *
   * <p>Some examples:
   *
   * <ul>
   *   <li>{@code ["foo", "bar", "baz"]} returned as {@code "foo.bar.baz"} - no escaping needed.
   *   <li>{@code ["foo", ".bar", "baz"]} returned as {@code ".foo.*.bar.baz"} - escaping needed.
   *   <li>{@code ["foo.", ".bar", "a/\%aa"]} returned as <code>".foo*..*.bar.a*{*}*[aa"</code>.
   * </ul>
   *
   * <p>The returned value is always processable by Nessie services announcing Nessie spec 2.2.0 or
   * newer.
   *
   * @return The URI path compatible representation of the given elements, possibly escaped. The
   *     returned value should be URL-encoded before added to a URI path. The returned value can be
   *     parsed with {@link #elementsFromPathString(String)}.
   */
  default String toPathStringEscaped() {
    return Util.toPathStringEscaped(getElements());
  }

  /**
   * Escapes content-key elements into a canonical form that escapes {@code .} characters. The
   * returned format is similar to {@linkplain #toPathStringEscaped()}, but does not escape
   * problematic URI characters, only the {@code .} is escaped.
   *
   * @return The canonical representation of the given elements, possibly escaped. The returned
   *     value should <em>not</em> be used in a URI path. The returned value can be parsed with
   *     {@link #elementsFromPathString(String)}.
   */
  default String toCanonicalString() {
    return Util.toCanonicalString(getElements());
  }

  default void validate(String type) {
    List<String> elements = getElements();
    int elems = elements.size();
    if (elems > MAX_ELEMENTS) {
      throw new IllegalStateException(
          format("%s too long, max allowed number of elements: %s", type, MAX_ELEMENTS));
    }
    int sum = 0;
    for (int i = 0; i < elems; i++) {
      String e = elements.get(i);

      if (e == null) {
        throw new IllegalArgumentException(
            format("%s '%s' must not contain a null element", type, elements));
      }
      int l = e.length();
      if (l == 0) {
        throw new IllegalArgumentException(
            format("%s '%s' must not contain an empty element", type, elements));
      }
      for (int j = 0; j < l; j++) {
        char c = e.charAt(j);
        if (c < FIRST_ALLOWED_KEY_CHAR) {
          throw new IllegalArgumentException(
              format(
                  "%s '%s' must not contain characters less than 0x%2h",
                  type, elements, FIRST_ALLOWED_KEY_CHAR));
        } else if (c == 0x7f) {
          throw new IllegalArgumentException(
              format("%s '%s' must not contain the character 0x7F", type, elements));
        }
      }
      sum += l;
    }
    if (sum > MAX_LENGTH) {
      throw new IllegalStateException(
          format("%s too long, max allowed length: %d", type, MAX_LENGTH));
    }
  }
}
