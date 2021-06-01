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

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * For a given table name <b>a.b.c.tableName</b>, the {@link Namespace} would be the prefix <b>a.b.c</b>, since the last element
 * <b>tableName</b> always represents the name of the actual table and is not included in the {@link Namespace} itself. Therefore, the
 * {@link Namespace} is always consisting of the first <b>N-1</b> elements.
 */
@Value.Immutable(prehash = true)
@JsonSerialize(as = ImmutableNamespace.class)
@JsonDeserialize(as = ImmutableNamespace.class)
public abstract class Namespace {

  private static final String DOT = ".";
  static final String ERROR_MSG_TEMPLATE = "'%s' is not a valid namespace identifier (should not end with '.')";

  @JsonValue
  public abstract String name();

  public boolean isEmpty() {
    return "".equals(name());
  }

  /**
   * Builds a {@link Namespace} instance out of the first <b>N-1</b> elements.
   *
   * @param elements The elements to build the namespace from.
   * @return A {@link Namespace} instance, where the {@link Namespace#name()} represents the first <b>N-1</b> elements. If <b>elements</b>
   *     consists only of a single element, then {@link Namespace#name()} will be an empty string.
   */
  public static Namespace of(String... elements) {
    Objects.requireNonNull(elements, "elements must be non-null");
    if (elements.length == 0) {
      return ImmutableNamespace.builder().name("").build();
    }

    if (DOT.equals(elements[elements.length - 1])) {
      throw new IllegalArgumentException(String.format(ERROR_MSG_TEMPLATE, Arrays.toString(elements)));
    }

    StringJoiner joiner = new StringJoiner(DOT);
    for (int i = 0; i < elements.length - 1; i++) {
      joiner.add(elements[i]);
    }
    return ImmutableNamespace.builder().name(joiner.toString()).build();
  }

  /**
   * Builds a {@link Namespace} instance out of the first <b>N-1</b> elements.
   *
   * @param elements The elements to build the namespace from.
   * @return A {@link Namespace} instance, where the {@link Namespace#name()} represents the first <b>N-1</b> elements. If <b>elements</b>
   *     consists only of a single element, then {@link Namespace#name()} will be an empty string.
   */
  public static Namespace of(List<String> elements) {
    Objects.requireNonNull(elements, "elements must be non-null");
    return Namespace.of(elements.toArray(new String[0]));
  }

  /**
   * Builds a {@link Namespace} instance out of the first <b>N-1</b> elements when those are split by the <b>.</b> (dot) character.
   *
   * @param identifier The identifier to build the namespace from.
   * @return Splits the given <b>identifier</b> by <b>.</b> and returns a {@link Namespace} instance, where the {@link Namespace#name()}
   *     represents the first <b>N-1</b> elements. If <b>identifier</b> consists only of a single element, then {@link Namespace#name()}
   *     will be an empty string.
   */
  public static Namespace parse(String identifier) {
    Objects.requireNonNull(identifier, "identifier must be non-null");
    if (identifier.endsWith(DOT)) {
      throw new IllegalArgumentException(String.format(ERROR_MSG_TEMPLATE, identifier));
    }
    return Namespace.of(identifier.split("\\."));
  }
}
