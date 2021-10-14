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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

public class TestNamespace {

  @Test
  public void testNullAndEmpty() {
    assertThatThrownBy(() -> Namespace.of((String[]) null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("elements must be non-null");

    assertThatThrownBy(() -> Namespace.of((List<String>) null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("elements must be non-null");

    assertThatThrownBy(() -> Namespace.parse(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("identifier must be non-null");

    assertThat(Namespace.of().name()).isEmpty();
    assertThat(Namespace.parse("").name()).isEmpty();
    assertThat(Namespace.of(""))
        .extracting(Namespace::name, Namespace::isEmpty)
        .containsExactly("", true);
  }

  @Test
  public void testOneElement() {
    Namespace namespace = Namespace.of("foo");
    assertThat(namespace)
        .extracting(Namespace::name, Namespace::isEmpty)
        .containsExactly("foo", false);
  }

  @ParameterizedTest
  @MethodSource("elementsProvider")
  void testNamespaceFromElements(String[] elements, String expectedNamespace) {
    Namespace namespace = Namespace.of(elements);
    assertThat(namespace.name()).isEqualTo(expectedNamespace);
    assertThat(namespace.isEmpty()).isFalse();

    namespace = Namespace.of(Arrays.asList(elements));
    assertThat(namespace.name()).isEqualTo(expectedNamespace);
    assertThat(namespace.isEmpty()).isFalse();
  }

  @ParameterizedTest
  @MethodSource("identifierProvider")
  void testNamespaceParsing(String identifier, String expectedNamespace) {
    Namespace namespace = Namespace.parse(identifier);
    assertThat(namespace.name()).isEqualTo(expectedNamespace);
    assertThat(namespace.isEmpty()).isFalse();
  }

  @ParameterizedTest
  @MethodSource("invalidElementsProvider")
  void testInvalidElements(String[] elements) {
    assertThatThrownBy(() -> Namespace.of(elements))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(String.format(Namespace.ERROR_MSG_TEMPLATE, Arrays.toString(elements)));

    assertThatThrownBy(() -> Namespace.of(Arrays.asList(elements)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(String.format(Namespace.ERROR_MSG_TEMPLATE, Arrays.toString(elements)));
  }

  @ParameterizedTest
  @ValueSource(strings = {".", "a.", "a.b.c."})
  void testInvalidParsing(String identifier) {
    assertThatThrownBy(() -> Namespace.parse(identifier))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(String.format(Namespace.ERROR_MSG_TEMPLATE, identifier));
  }

  private static Stream<Arguments> elementsProvider() {
    return Stream.of(
        Arguments.of(new String[] {"a", "b"}, "a.b"),
        Arguments.of(new String[] {"a", "b", "c"}, "a.b.c"));
  }

  private static Stream<Arguments> identifierProvider() {
    return Stream.of(
        Arguments.of("a", "a"), Arguments.of("a.b", "a.b"), Arguments.of("a.b.c", "a.b.c"));
  }

  private static Stream<Arguments> invalidElementsProvider() {
    return Stream.of(
        Arguments.of(new String[] {"."}, "x"),
        Arguments.of(new String[] {"a", "."}, "x"),
        Arguments.of(new String[] {"a", "b", "c", "."}, "x"));
  }
}
