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
import static org.projectnessie.model.Namespace.Empty.EMPTY_NAMESPACE;

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
public abstract class Namespace extends Content implements Elements {

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
    return toPathString();
  }

  @JsonIgnore
  @Value.Redacted
  public boolean isEmpty() {
    return getElements().isEmpty();
  }

  @NotNull
  @jakarta.validation.constraints.NotNull
  @Override
  public abstract List<String> getElements();

  @JsonIgnore
  @Value.Redacted
  @Override
  public String[] getElementsArray() {
    return getElements().toArray(new String[0]);
  }

  @JsonIgnore
  @Value.Redacted
  @Override
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

    return ImmutableNamespace.builder()
        .elements(Arrays.asList(elements))
        .properties(properties)
        .build();
  }

  @Value.Check
  protected void validate() {
    Elements.super.validate("Namespace");

    int elementCount = getElementCount();
    List<String> elements = getElements();
    if (elementCount > 0 && ".".equals(elements.get(elementCount - 1))) {
      throw new IllegalArgumentException(
          format("Namespace '%s' must not contain a '.' element", elements));
    }
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
   * versions, see {@link Elements#elementsFromPathString(String)}.
   */
  public static Namespace fromPathString(String encoded) {
    return parse(encoded);
  }

  @Override
  @Value.NonAttribute
  @JsonIgnore
  public String toPathString() {
    return Elements.super.toPathString();
  }

  @Override
  @Value.NonAttribute
  @JsonIgnore
  public String toPathStringEscaped() {
    return Elements.super.toPathStringEscaped();
  }

  @Override
  @Value.NonAttribute
  @JsonIgnore
  public String toCanonicalString() {
    return Elements.super.toCanonicalString();
  }

  @Override
  public String toString() {
    return toPathString();
  }

  public ContentKey toContentKey() {
    return ContentKey.of(getElements());
  }
}
