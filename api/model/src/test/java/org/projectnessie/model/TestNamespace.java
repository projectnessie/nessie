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
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.projectnessie.model.Namespace.Empty.EMPTY_NAMESPACE;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(SoftAssertionsExtension.class)
public class TestNamespace {
  @InjectSoftAssertions protected SoftAssertions soft;

  @Test
  public void testNullAndEmpty() {
    soft.assertThatThrownBy(() -> Namespace.of((String[]) null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("elements must be non-null");

    soft.assertThatThrownBy(() -> Namespace.of((List<String>) null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("elements must be non-null");

    soft.assertThatThrownBy(() -> Namespace.parse(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("identifier must be non-null");

    soft.assertThat(Namespace.of().name()).isEmpty();
    soft.assertThat(Namespace.parse("").name()).isEmpty();
    soft.assertThat(Namespace.of(""))
        .extracting(Namespace::name, Namespace::isEmpty)
        .containsExactly("", true);

    soft.assertThat(Namespace.of("")).isEqualTo(EMPTY_NAMESPACE);
    soft.assertThat(Namespace.of(Collections.emptyList())).isEqualTo(EMPTY_NAMESPACE);
    soft.assertThat(Namespace.of(singletonList(""))).isEqualTo(EMPTY_NAMESPACE);

    soft.assertThatThrownBy(() -> Namespace.of("", "something"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Namespace '[, something]' must not contain an empty element");
    soft.assertThatThrownBy(() -> Namespace.of("", "something", "x"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Namespace '[, something, x]' must not contain an empty element");

    soft.assertThatThrownBy(() -> Namespace.of("something", "", "x"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Namespace '[something, , x]' must not contain an empty element");

    soft.assertThatThrownBy(() -> Namespace.of("something", "x", ""))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Namespace '[something, x, ]' must not contain an empty element");
  }

  @Test
  public void emptyElement() {
    Namespace namespace = Namespace.of("");
    soft.assertThat(namespace).isSameAs(EMPTY_NAMESPACE);
  }

  @Test
  public void testOneElement() {
    Namespace namespace = Namespace.of("foo");
    soft.assertThat(namespace)
        .extracting(Namespace::name, Namespace::isEmpty)
        .containsExactly("foo", false);
  }

  @Test
  public void testRoundTrip() {
    List<String> elements = asList("a", "b.c", "namespace");
    String pathString = Util.toPathString(elements);
    String expectedPathString = "a.b\u001Dc.namespace";
    soft.assertThat(pathString).isEqualTo(expectedPathString);
    Namespace namespace = Namespace.parse(pathString);
    soft.assertThat(namespace.name()).isEqualTo(pathString);
    soft.assertThat(namespace.getElements()).isEqualTo(elements);
    soft.assertThat(namespace.toString()).isEqualTo(pathString);
    soft.assertThat(namespace.toPathString()).isEqualTo(pathString);
    soft.assertThat(namespace.name()).startsWith("a.b");
    soft.assertThat(namespace.name()).startsWith("a.b\u001D");
    soft.assertThat(namespace.name()).startsWith("a.b\u001Dc");
    soft.assertThat(namespace.name()).startsWith("a.b\u001Dc.namespa");
    soft.assertThat(namespace.name()).doesNotStartWith("a.b.c");
    soft.assertThat(Namespace.parse("a.b.c").name()).doesNotStartWith("a.b\u001D");
  }

  @Test
  public void testIsSameOrSubElementOf() {
    Namespace namespace = Namespace.of(asList("a", "b.c", "namespace"));

    soft.assertThatThrownBy(() -> EMPTY_NAMESPACE.isSameOrSubElementOf(null))
        .hasMessage("namespace must be non-null");

    soft.assertThat(EMPTY_NAMESPACE.isSameOrSubElementOf(EMPTY_NAMESPACE)).isTrue();
    soft.assertThat(namespace.isSameOrSubElementOf(EMPTY_NAMESPACE)).isTrue();
    soft.assertThat(namespace.isSameOrSubElementOf(Namespace.of("a"))).isTrue();
    soft.assertThat(namespace.isSameOrSubElementOf(Namespace.parse("a"))).isTrue();
    soft.assertThat(namespace.isSameOrSubElementOf(Namespace.of("a", "b"))).isFalse();
    soft.assertThat(namespace.isSameOrSubElementOf(Namespace.parse("a.b\u001Dc"))).isTrue();
    soft.assertThat(namespace.isSameOrSubElementOf(Namespace.parse("a.b\u001Dc.namespa")))
        .isFalse();
    soft.assertThat(namespace.isSameOrSubElementOf(Namespace.parse("a.b\u001Dc.namespace")))
        .isTrue();
    soft.assertThat(namespace.isSameOrSubElementOf(Namespace.parse("a.b\u0000c.namespace")))
        .isTrue();

    soft.assertThat(namespace.isSameOrSubElementOf(Namespace.of("x"))).isFalse();
    soft.assertThat(namespace.isSameOrSubElementOf(Namespace.of("a", "b", "c"))).isFalse();

    soft.assertThat(Namespace.parse("a.b.c").isSameOrSubElementOf(Namespace.parse("a.b\u001Dc")))
        .isFalse();
    soft.assertThat(EMPTY_NAMESPACE.isSameOrSubElementOf(Namespace.of("a"))).isFalse();
  }

  @Test
  public void testNamespaceWithProperties() {
    Map<String, String> properties = new HashMap<>();
    properties.put("location", "/tmp");
    properties.put("x", "y");
    Namespace namespace = Namespace.of(properties, "a", "b.c", "d");
    soft.assertThat(namespace.getProperties()).isEqualTo(properties);
  }

  @Test
  void noParent() {
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> Namespace.of("x").getParent())
        .withMessage("Namespace has no parent");
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> Namespace.of().getParent())
        .withMessage("Namespace has no parent");
  }

  @ParameterizedTest
  @MethodSource
  void invalidElements(List<String> elements, String message) {
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> Namespace.of(elements))
        .withMessageStartingWith("Namespace")
        .withMessageEndingWith(message);
  }

  static Stream<Arguments> invalidElements() {
    return Stream.of(
        arguments(List.of("."), "must not contain a '.' element"),
        arguments(List.of("abc", "."), "must not contain a '.' element"),
        arguments(List.of("", "abc"), "must not contain an empty element"),
        arguments(List.of("abc", ""), "must not contain an empty element"),
        arguments(List.of("abc", "", "def"), "must not contain an empty element"));
  }
}
