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

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

@Execution(ExecutionMode.CONCURRENT)
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

    assertThat(Namespace.of("")).isEqualTo(Namespace.EMPTY);
    assertThat(Namespace.of(Collections.emptyList())).isEqualTo(Namespace.EMPTY);
    assertThat(Namespace.of(singletonList(""))).isEqualTo(Namespace.EMPTY);

    assertThatThrownBy(() -> Namespace.of("", "something"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Namespace '[, something]' must not contain an empty element.");
    assertThatThrownBy(() -> Namespace.of("", "something", "x"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Namespace '[, something, x]' must not contain an empty element.");

    assertThatThrownBy(() -> Namespace.of("something", "", "x"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Namespace '[something, , x]' must not contain an empty element.");

    assertThatThrownBy(() -> Namespace.of("something", "x", ""))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Namespace '[something, x, ]' must not contain an empty element.");
  }

  @Test
  public void testOneElement() {
    Namespace namespace = Namespace.of("foo");
    assertThat(namespace)
        .extracting(Namespace::name, Namespace::isEmpty)
        .containsExactly("foo", false);
  }

  @Test
  public void testRoundTrip() {
    List<String> elements = asList("a", "b.c", "namespace");
    String pathString = UriUtil.toPathString(elements);
    String expectedPathString = "a.b\u001Dc.namespace";
    assertThat(pathString).isEqualTo(expectedPathString);
    Namespace namespace = Namespace.parse(pathString);
    assertThat(namespace.name()).isEqualTo(pathString);
    assertThat(namespace.getElements()).isEqualTo(elements);
    assertThat(namespace.toString()).isEqualTo(pathString);
    assertThat(namespace.toPathString()).isEqualTo(pathString);
    assertThat(namespace.name()).startsWith("a.b");
    assertThat(namespace.name()).startsWith("a.b\u001D");
    assertThat(namespace.name()).startsWith("a.b\u001Dc");
    assertThat(namespace.name()).startsWith("a.b\u001Dc.namespa");
    assertThat(namespace.name()).doesNotStartWith("a.b.c");
    assertThat(Namespace.parse("a.b.c").name()).doesNotStartWith("a.b\u001D");
  }

  @Test
  public void testIsSameOrSubElementOf() {
    Namespace namespace = Namespace.of(asList("a", "b.c", "namespace"));

    assertThatThrownBy(() -> Namespace.EMPTY.isSameOrSubElementOf(null))
        .hasMessage("namespace must be non-null");

    assertThat(Namespace.EMPTY.isSameOrSubElementOf(Namespace.EMPTY)).isTrue();
    assertThat(namespace.isSameOrSubElementOf(Namespace.EMPTY)).isTrue();
    assertThat(namespace.isSameOrSubElementOf(Namespace.of("a"))).isTrue();
    assertThat(namespace.isSameOrSubElementOf(Namespace.parse("a"))).isTrue();
    assertThat(namespace.isSameOrSubElementOf(Namespace.of("a", "b"))).isFalse();
    assertThat(namespace.isSameOrSubElementOf(Namespace.parse("a.b\u001Dc"))).isTrue();
    assertThat(namespace.isSameOrSubElementOf(Namespace.parse("a.b\u001Dc.namespa"))).isFalse();
    assertThat(namespace.isSameOrSubElementOf(Namespace.parse("a.b\u001Dc.namespace"))).isTrue();
    assertThat(namespace.isSameOrSubElementOf(Namespace.parse("a.b\u0000c.namespace"))).isTrue();

    assertThat(namespace.isSameOrSubElementOf(Namespace.of("a", "\u0012b"))).isFalse();
    assertThat(namespace.isSameOrSubElementOf(Namespace.of("x"))).isFalse();
    assertThat(namespace.isSameOrSubElementOf(Namespace.of("a", "b", "c"))).isFalse();

    assertThat(Namespace.parse("a.b.c").isSameOrSubElementOf(Namespace.parse("a.b\u001Dc")))
        .isFalse();
    assertThat(Namespace.EMPTY.isSameOrSubElementOf(Namespace.of("a"))).isFalse();
  }

  @Test
  public void testDifferentZeroByteRepresentations() {
    assertThat(Namespace.parse("a.b\u001Dc.d")).isEqualTo(Namespace.parse("a.b\u0000c.d"));
    assertThat(
            Namespace.parse("a.b\u001Dc.d").isSameOrSubElementOf(Namespace.parse("a.b\u0000c.d")))
        .isTrue();

    assertThat(Namespace.parse("a.b\u001Dc.d").isSameOrSubElementOf(Namespace.parse("a.b\u0000c")))
        .isTrue();
    assertThat(Namespace.parse("a.b\u0000c.d").isSameOrSubElementOf(Namespace.parse("a.b\u001Dc")))
        .isTrue();

    // even though we treat the zero byte + the group separator equally, we can't do comparisons
    // based on strings only
    assertThat(Namespace.of(asList("a", "b.c", "namespace")).name()).doesNotStartWith("a.b\u0000c");
  }

  @Test
  public void testNamespaceWithProperties() {
    Map<String, String> properties = new HashMap<>();
    properties.put("location", "/tmp");
    properties.put("x", "y");
    Namespace namespace = Namespace.of(properties, "a", "b.c", "d");
    assertThat(namespace.getProperties()).isEqualTo(properties);
  }

  @ParameterizedTest
  @MethodSource("elementsProvider")
  void testNamespaceFromElements(String[] elements, String expectedNamespace) {
    Namespace namespace = Namespace.of(elements);
    assertThat(namespace.name()).isEqualTo(expectedNamespace);
    assertThat(namespace.isEmpty()).isFalse();
    assertThat(namespace.getElements()).containsExactly(elements);

    namespace = Namespace.of(Arrays.asList(elements));
    assertThat(namespace.name()).isEqualTo(expectedNamespace);
    assertThat(namespace.isEmpty()).isFalse();
    assertThat(namespace.getElements()).isEqualTo(Arrays.asList(elements));
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

  @ParameterizedTest
  @ValueSource(
      strings = {"\u0000", "a.\u0000", "a.b.c.\u0000", "\u001D", "a.\u001D", "a.b.c.\u001D"})
  void testZeroByteUsage(String identifier) {
    assertThatThrownBy(() -> Namespace.of(identifier))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            String.format(
                "Namespace '%s' must not contain a zero byte (\\u0000) / group separator (\\u001D).",
                singletonList(identifier)));
  }

  @ParameterizedTest
  @MethodSource("invalidElementsWithNullsProvider")
  void testNullsInElements(String[] elements) {
    assertThatThrownBy(() -> Namespace.of(elements))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            String.format(
                "Namespace '%s' must not contain a null element.", Arrays.toString(elements)));
  }

  private static Stream<Arguments> elementsProvider() {
    return Stream.of(
        Arguments.of(new String[] {"a", "b"}, "a.b"),
        Arguments.of(new String[] {"a", "b", "c"}, "a.b.c"),
        Arguments.of(new String[] {"a", "b.c", "d"}, "a.b\u001Dc.d"),
        Arguments.of(new String[] {"a", "b_c", "d.e"}, "a.b_c.d\u001De"),
        Arguments.of(new String[] {"a.c", "b.d"}, "a\u001Dc.b\u001Dd"));
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

  private static Stream<Arguments> invalidElementsWithNullsProvider() {
    return Stream.of(
        Arguments.of(new String[] {null}, "x"),
        Arguments.of(new String[] {"a", ".", null}, "x"),
        Arguments.of(new String[] {"a", "b", "c", ".", null}, "x"));
  }

  @ParameterizedTest
  @MethodSource("namespaceOfAndParseCases")
  void namespaceOfAndParse(NamespaceOfParse testCase) {
    assertThat(testCase.namespace)
        .extracting(
            Namespace::getElements, Namespace::name, Namespace::toString, Namespace::toPathString)
        .containsExactly(testCase.elements, testCase.name, testCase.name, testCase.name);
  }

  static class NamespaceOfParse {
    final Namespace namespace;
    final List<String> elements;
    final String name;

    NamespaceOfParse(Namespace namespace, List<String> elements, String name) {
      this.namespace = namespace;
      this.elements = elements;
      this.name = name;
    }

    @Override
    public String toString() {
      return new StringJoiner(", ", NamespaceOfParse.class.getSimpleName() + "[", "]")
          .add("namespace=" + namespace)
          .add("elements=" + elements)
          .add("name='" + name + "'")
          .toString();
    }
  }

  static List<NamespaceOfParse> namespaceOfAndParseCases() {
    return asList(
        new NamespaceOfParse(
            Namespace.fromPathString(UriUtil.toPathString(Arrays.asList("a", "b.c", "namespace"))),
            Arrays.asList("a", "b.c", "namespace"),
            UriUtil.toPathString(Arrays.asList("a", "b.c", "namespace"))),
        new NamespaceOfParse(
            Namespace.fromPathString(
                UriUtil.toPathString(Arrays.asList("a", "b.c", "d.e.f.namespace"))),
            Arrays.asList("a", "b.c", "d.e.f.namespace"),
            UriUtil.toPathString(Arrays.asList("a", "b.c", "d.e.f.namespace"))),
        new NamespaceOfParse(
            Namespace.fromPathString("a.namespace"), asList("a", "namespace"), "a.namespace"),
        new NamespaceOfParse(
            Namespace.of("a", "namespace"), asList("a", "namespace"), "a.namespace"),
        new NamespaceOfParse(
            Namespace.of(asList("a", "namespace")), asList("a", "namespace"), "a.namespace"),
        new NamespaceOfParse(
            Namespace.fromPathString("a.b.namespace"),
            asList("a", "b", "namespace"),
            "a.b.namespace"),
        new NamespaceOfParse(
            Namespace.of("a", "b", "namespace"), asList("a", "b", "namespace"), "a.b.namespace"),
        new NamespaceOfParse(
            Namespace.of(asList("a", "b", "namespace")),
            asList("a", "b", "namespace"),
            "a.b.namespace"),
        new NamespaceOfParse(Namespace.EMPTY, Collections.emptyList(), ""),
        new NamespaceOfParse(
            Namespace.of(singletonList("namespace")), singletonList("namespace"), "namespace"),
        new NamespaceOfParse(
            Namespace.fromPathString("namespace"), singletonList("namespace"), "namespace"));
  }
}
