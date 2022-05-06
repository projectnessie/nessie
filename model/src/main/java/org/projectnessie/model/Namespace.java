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

import static org.projectnessie.model.UriUtil.DOT_STRING;
import static org.projectnessie.model.UriUtil.GROUP_SEPARATOR_STRING;
import static org.projectnessie.model.UriUtil.ZERO_BYTE_STRING;

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

  public static final Namespace EMPTY =
      ImmutableNamespace.builder().elements(Collections.emptyList()).build();

  @Override
  public Type getType() {
    return Type.NAMESPACE;
  }

  @NotNull
  @Derived
  @JsonIgnore
  public String name() {
    return toPathString();
  }

  @JsonIgnore
  @Value.Redacted
  public boolean isEmpty() {
    return name().isEmpty();
  }

  @NotNull
  public abstract List<String> getElements();

  @NotNull
  @JsonInclude(Include.NON_EMPTY)
  public abstract Map<String, String> getProperties();

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
      return EMPTY;
    }

    for (String e : elements) {
      if (e == null) {
        throw new IllegalArgumentException(
            String.format(
                "Namespace '%s' must not contain a null element.", Arrays.toString(elements)));
      }
      if (e.contains(ZERO_BYTE_STRING) || e.contains(GROUP_SEPARATOR_STRING)) {
        throw new IllegalArgumentException(
            String.format(
                "Namespace '%s' must not contain a zero byte (\\u0000) / group separator (\\u001D).",
                Arrays.toString(elements)));
      }
      if (e.isEmpty()) {
        throw new IllegalArgumentException(
            String.format(
                "Namespace '%s' must not contain an empty element.", Arrays.toString(elements)));
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
   * character.
   *
   * @param identifier The identifier to build the namespace from.
   * @return Splits the given <b>identifier</b> by <b>.</b> and returns a {@link Namespace}
   *     instance. If <b>identifier</b> is empty, then {@link Namespace#name()} will be an empty
   *     string.
   */
  public static Namespace parse(String identifier) {
    Objects.requireNonNull(identifier, "identifier must be non-null");
    if (identifier.isEmpty()) {
      return EMPTY;
    }
    if (identifier.endsWith(DOT_STRING)) {
      throw new IllegalArgumentException(String.format(ERROR_MSG_TEMPLATE, identifier));
    }
    return Namespace.of(UriUtil.fromPathString(identifier));
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
    if (getElements().size() < parent.getElements().size()) {
      return false;
    }
    for (int i = 0; i < parent.getElements().size(); i++) {
      // elements must match exactly
      if (!getElements().get(i).equals(parent.getElements().get(i))) {
        return false;
      }
    }
    return true;
  }

  /**
   * Convert from path encoded string to normal string.
   *
   * @param encoded Path encoded string
   * @return Actual key.
   */
  public static Namespace fromPathString(String encoded) {
    return parse(encoded);
  }

  /**
   * Convert this namespace to a URL encoded path string.
   *
   * @return String encoded for path use.
   */
  public String toPathString() {
    return UriUtil.toPathString(getElements());
  }

  @Override
  public String toString() {
    return name();
  }
}
