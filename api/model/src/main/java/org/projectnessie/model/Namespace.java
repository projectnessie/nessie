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

import static org.projectnessie.model.Namespace.Empty.EMPTY_NAMESPACE;
import static org.projectnessie.model.Util.DOT_STRING;
import static org.projectnessie.model.Util.FIRST_ALLOWED_KEY_CHAR;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.validation.constraints.NotNull;
import org.immutables.value.Value;
import org.immutables.value.Value.Derived;

/**
 * For a given table name <b>a.b.c.tableName</b>, the {@link Namespace} would be the prefix
 * <b>a.b.c</b>, since the last element <b>tableName</b> always represents the name of the actual
 * table and is not included in the {@link Namespace} itself. Therefore, the {@link Namespace} is
 * always consisting of the first <b>N-1</b> elements.
 */
@Value.Immutable
@JsonSerialize(as = ImmutableNamespace.class)
@JsonDeserialize(as = ImmutableNamespace.class)
@JsonTypeName("NAMESPACE")
public abstract class Namespace extends Content {

  static final String ERROR_MSG_TEMPLATE =
      "'%s' is not a valid namespace identifier (should not end with '.')";

  /** This separate static class is needed to prevent class loader deadlocks. */
  public static final class Empty {
    private Empty() {}

    public static final Namespace EMPTY_NAMESPACE = builder().build();
  }

  /**
   * Refactor all code references to this constant to use {@link Empty#EMPTY_NAMESPACE}, there's a
   * non-zero risk of causing a class loader deadlock.
   */
  @Deprecated
  public static final Namespace EMPTY = builder().elements(Collections.emptyList()).build();

  public static ImmutableNamespace.Builder builder() {
    return ImmutableNamespace.builder();
  }

  @Override
  public Type getType() {
    return Type.NAMESPACE;
  }

  @NotNull
  @jakarta.validation.constraints.NotNull
  @Derived
  @JsonIgnore
  public String name() {
    return toPathStringControlChars();
  }

  @JsonIgnore
  @Value.Redacted
  public boolean isEmpty() {
    return getElements().isEmpty();
  }

  @NotNull
  @jakarta.validation.constraints.NotNull
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

  @JsonIgnore
  @Value.Redacted
  public Namespace getParent() {
    List<String> elements = getElements();
    if (elements.size() <= 1) {
      throw new IllegalArgumentException("Namespace has no parent");
    }
    return Namespace.of(elements.subList(0, elements.size() - 1));
  }

  @JsonIgnore
  @Value.Redacted
  public Namespace getParentOrEmpty() {
    List<String> elements = getElements();
    if (elements.size() <= 1) {
      return EMPTY_NAMESPACE;
    }
    return Namespace.of(elements.subList(0, elements.size() - 1));
  }

  @NotNull
  @jakarta.validation.constraints.NotNull
  @JsonInclude(Include.NON_EMPTY)
  public abstract Map<String, String> getProperties();

  @Override
  public abstract Namespace withId(String id);

  /**
   * Builds a {@link Namespace} using the elements of the given content key.
   *
   * @param contentKey key with the elements for the namespace
   * @return new namespace
   */
  public static Namespace of(ContentKey contentKey) {
    return Namespace.of(contentKey.getElementsArray());
  }

  /**
   * Builds a {@link Namespace} instance for the given elements.
   *
   * @param elements The elements to build the namespace from.
   * @return The constructed {@link Namespace} instance. If <b>elements</b> is empty, then {@link
   *     Namespace#name()} will be an empty string.
   */
  public static Namespace of(String... elements) {
    return Namespace.of(Collections.emptyMap(), elements);
  }

  /**
   * Builds a {@link Namespace} instance for the given elements and the given properties.
   *
   * @param elements The elements to build the namespace from.
   * @param properties The namespace properties.
   * @return The constructed {@link Namespace} instance. If <b>elements</b> is empty, then {@link
   *     Namespace#name()} will be an empty string.
   */
  public static Namespace of(Map<String, String> properties, String... elements) {
    Objects.requireNonNull(elements, "elements must be non-null");
    if (elements.length == 0 || (elements.length == 1 && "".equals(elements[0]))) {
      return EMPTY_NAMESPACE;
    }

    for (String e : elements) {
      if (e == null) {
        throw new IllegalArgumentException(
            String.format(
                "Namespace '%s' must not contain a null element.", Arrays.toString(elements)));
      }
      if (e.isEmpty()) {
        throw new IllegalArgumentException(
            String.format(
                "Namespace '%s' must not contain an empty element.", Arrays.toString(elements)));
      }
      if (e.chars().anyMatch(i -> i < FIRST_ALLOWED_KEY_CHAR)) {
        throw new IllegalArgumentException(
            String.format(
                "Namespace '%s' must not contain characters less than 0x%2h.",
                Arrays.toString(elements), FIRST_ALLOWED_KEY_CHAR));
      }
    }

    if (DOT_STRING.equals(elements[elements.length - 1])) {
      throw new IllegalArgumentException(
          String.format(ERROR_MSG_TEMPLATE, Arrays.toString(elements)));
    }

    return ImmutableNamespace.builder()
        .elements(Arrays.asList(elements))
        .properties(properties)
        .build();
  }

  /**
   * Builds a {@link Namespace} instance for the given elements.
   *
   * @param elements The elements to build the namespace from.
   * @return The constructed {@link Namespace} instance. If <b>elements</b> is empty, then {@link
   *     Namespace#name()} will be an empty string.
   */
  public static Namespace of(List<String> elements) {
    Objects.requireNonNull(elements, "elements must be non-null");
    return Namespace.of(elements.toArray(new String[0]));
  }

  /**
   * Builds a {@link Namespace} instance for the given elements and the given properties.
   *
   * @param elements The elements to build the namespace from.
   * @param properties The properties of the namespace.
   * @return The constructed {@link Namespace} instance. If <b>elements</b> is empty, then {@link
   *     Namespace#name()} will be an empty string.
   */
  public static Namespace of(List<String> elements, Map<String, String> properties) {
    Objects.requireNonNull(elements, "elements must be non-null");
    return Namespace.of(properties, elements.toArray(new String[0]));
  }

  /**
   * Builds a {@link Namespace} instance for the given elements split by the <b>.</b> (dot)
   * character, see {@linkplain Util#fromPathString(String) encoding specification}.
   *
   * @param identifier The identifier to build the namespace from, see {@linkplain
   *     Util#fromPathString(String) encoding specification}.
   * @return Splits the given <b>identifier</b> by <b>.</b> and returns a {@link Namespace}
   *     instance. If <b>identifier</b> is empty, then {@link Namespace#name()} will be an empty
   *     string.
   */
  public static Namespace parse(String identifier) {
    Objects.requireNonNull(identifier, "identifier must be non-null");
    if (identifier.isEmpty()) {
      return EMPTY_NAMESPACE;
    }
    if (identifier.endsWith(DOT_STRING)) {
      throw new IllegalArgumentException(String.format(ERROR_MSG_TEMPLATE, identifier));
    }
    return Namespace.of(Util.fromPathString(identifier));
  }

  /**
   * Checks whether the current {@link Namespace} is the same as or a sub-element of the given
   * {@link Namespace} instance by comparing each canonical element.
   *
   * @param parent {@link Namespace} instance to compare with.
   * @return <code>true</code> if the current {@link Namespace} is the same as or a sub-element of
   *     the given {@link Namespace} instance by comparing each canonical element, <code>false
   *     </code> otherwise.
   */
  public boolean isSameOrSubElementOf(Namespace parent) {
    Objects.requireNonNull(parent, "namespace must be non-null");
    if (getElementCount() < parent.getElementCount()) {
      return false;
    }
    for (int i = 0; i < parent.getElementCount(); i++) {
      // elements must match exactly
      if (!getElements().get(i).equals(parent.getElements().get(i))) {
        return false;
      }
    }
    return true;
  }

  /**
   * Parses the path encoded string to a {@link Namespace} object, supports all Nessie Spec
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
  public static Namespace fromPathString(String encoded) {
    return parse(encoded);
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
    return toPathStringControlChars();
  }

  public ContentKey toContentKey() {
    return ContentKey.of(getElements());
  }
}
