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
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.INTEGER;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

@ExtendWith(SoftAssertionsExtension.class)
class TestContentKey {
  @InjectSoftAssertions protected SoftAssertions soft;

  static final String STRING_100 =
      "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890";

  @ParameterizedTest
  @MethodSource("keyLengthGood")
  void keyLengthGood(List<String> elements) {
    ContentKey.of(elements);
  }

  static Stream<Arguments> keyLengthGood() {
    return Stream.of(
        arguments(emptyList()),
        arguments(singletonList("a")),
        arguments(singletonList(STRING_100)),
        arguments(singletonList(STRING_100 + STRING_100 + STRING_100 + STRING_100 + STRING_100)),
        arguments(asList(STRING_100, STRING_100)),
        arguments(asList(STRING_100, STRING_100, STRING_100)),
        arguments(asList(STRING_100, STRING_100, STRING_100, STRING_100)),
        arguments(asList(STRING_100, STRING_100, STRING_100, STRING_100, STRING_100)));
  }

  @ParameterizedTest
  @MethodSource("keyTooLong")
  void keyTooLong(List<String> elements) {
    assertThatThrownBy(() -> ContentKey.of(elements))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Content key too long, max allowed length: " + ContentKey.MAX_LENGTH);
  }

  static Stream<Arguments> keyTooLong() {
    return Stream.of(
        arguments(
            singletonList(STRING_100 + STRING_100 + STRING_100 + STRING_100 + STRING_100 + "x")),
        arguments(
            singletonList(
                STRING_100 + STRING_100 + STRING_100 + STRING_100 + STRING_100 + STRING_100)),
        arguments(asList(STRING_100, STRING_100, STRING_100, STRING_100, STRING_100, "x")),
        arguments(asList("x", STRING_100, STRING_100, STRING_100, STRING_100, STRING_100)));
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 2, 5, 15, 19, 20})
  void keyElementsGood(int elements) {
    ContentKey.of(IntStream.range(0, elements).mapToObj(i -> "foo").collect(Collectors.toList()));
  }

  @ParameterizedTest
  @ValueSource(ints = {21, 55, 100})
  void keyTooManyElements(int elements) {
    assertThatThrownBy(
            () ->
                ContentKey.of(
                    IntStream.range(0, elements).mapToObj(i -> "foo").collect(Collectors.toList())))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Content key too long, max allowed number of elements: " + ContentKey.MAX_ELEMENTS);
  }

  @ParameterizedTest
  @MethodSource("compare")
  void compare(ContentKey a, ContentKey b, int expectedCompare) {
    assertThat(a)
        .describedAs("Compare of %s to %s expect %d", a, b, expectedCompare)
        .extracting(k -> Integer.signum(k.compareTo(b)))
        .asInstanceOf(INTEGER)
        .isEqualTo(expectedCompare);
  }

  static Stream<Arguments> compare() {
    return Stream.of(
        arguments(ContentKey.of("a"), ContentKey.of("a"), 0),
        arguments(ContentKey.of("a"), ContentKey.of("a", "a"), -1),
        arguments(ContentKey.of("a", "a"), ContentKey.of("a"), 1),
        arguments(ContentKey.of("a"), ContentKey.of("abcdef"), -1),
        arguments(ContentKey.of("abcdef"), ContentKey.of(), 1),
        arguments(ContentKey.of("abcdef"), ContentKey.of("0123", "123", "123"), 1),
        arguments(ContentKey.of("abcdef", "abc", "abc"), ContentKey.of("0123"), 1),
        arguments(ContentKey.of("0"), ContentKey.of("0123", "123", "123"), -1),
        arguments(ContentKey.of("abcdef", "abc", "abc"), ContentKey.of(), 1),
        arguments(ContentKey.of("key.0"), ContentKey.of("key.1"), -1),
        arguments(ContentKey.of("key.1"), ContentKey.of("key.0"), 1),
        arguments(ContentKey.of("key.42"), ContentKey.of("key.42"), 0),
        arguments(ContentKey.of("key", "0"), ContentKey.of("key", "1"), -1),
        arguments(ContentKey.of("key", "1"), ContentKey.of("key", "0"), 1),
        arguments(ContentKey.of("key", "42"), ContentKey.of("key", "42"), 0));
  }

  @Test
  void namespaceNotIncluded() throws IOException {
    ObjectMapper mapper = new ObjectMapper();

    Namespace namespace = Namespace.parse("a.b.c");
    soft.assertThat(namespace.name()).isEqualTo("a.b.c");
    soft.assertThat(namespace.getElements()).containsExactly("a", "b", "c");

    ContentKey key = ContentKey.of("a", "b", "c", "tableName");
    soft.assertThat(key.getNamespace()).isEqualTo(namespace);
    String serializedKey = mapper.writeValueAsString(key);
    soft.assertThat(serializedKey).contains("elements").doesNotContain("namespace");

    ContentKey deserialized = mapper.readValue(serializedKey, ContentKey.class);
    soft.assertThat(deserialized).isEqualTo(key);
    soft.assertThat(deserialized.getNamespace()).isEqualTo(namespace);
  }

  @Test
  public void construction() {
    String[] elements = {"a", "b", "c", "d"};
    ContentKey key = ContentKey.of(elements);
    soft.assertThat(key.getElements()).containsExactlyElementsOf(asList(elements));
    soft.assertThat(key.toPathString()).isEqualTo(String.join(".", elements));
    soft.assertThat(ContentKey.fromPathString("a.b.c.d")).isEqualTo(key);
  }

  static Stream<Arguments> invalidChars() {
    return IntStream.range(0, 0x20)
        .mapToObj(asciiCode -> arguments("invalid-char-" + asciiCode + ((char) asciiCode)));
  }

  @ParameterizedTest
  @MethodSource("invalidChars")
  void blockSpecialBytesUsage(String s) {
    assertThrows(IllegalArgumentException.class, () -> ContentKey.of(s));
  }

  @Test
  public void testDifferentZeroByteRepresentations() {
    assertThat(ContentKey.fromPathString("a.b\u001Dc.d"))
        .isEqualTo(ContentKey.fromPathString("a.b\u0000c.d"));
  }

  @Test
  void npe() {
    soft.assertThatThrownBy(() -> ContentKey.of((String[]) null))
        .isInstanceOf(NullPointerException.class);
    soft.assertThatThrownBy(() -> ContentKey.of(null, null))
        .isInstanceOf(NullPointerException.class);
    soft.assertThatThrownBy(() -> ContentKey.of("a", null))
        .isInstanceOf(NullPointerException.class);
    soft.assertThatThrownBy(() -> ContentKey.of((List<String>) null))
        .isInstanceOf(NullPointerException.class);
    soft.assertThatThrownBy(() -> ContentKey.of(singletonList(null)))
        .isInstanceOf(NullPointerException.class);
    soft.assertThatThrownBy(() -> ContentKey.of(asList("a", null)))
        .isInstanceOf(NullPointerException.class);
    soft.assertThatThrownBy(() -> ContentKey.of(asList(null, "a")))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void empty() {
    soft.assertThatThrownBy(() -> ContentKey.of(null, ""))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Content key '[]' must not contain an empty element");
    soft.assertThatThrownBy(() -> ContentKey.of("a", ""))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Content key '[a, ]' must not contain an empty element");
    soft.assertThatThrownBy(() -> ContentKey.of(singletonList("")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Content key '[]' must not contain an empty element");
    soft.assertThatThrownBy(() -> ContentKey.of(asList("a", "")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Content key '[a, ]' must not contain an empty element");
    soft.assertThatThrownBy(() -> ContentKey.of("", "something"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Content key '[, something]' must not contain an empty element");
    soft.assertThatThrownBy(() -> ContentKey.of("", "something", "x"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Content key '[, something, x]' must not contain an empty element");
  }

  @Test
  void toParentWithNoParent() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> ContentKey.of("x").getParent())
        .withMessage("ContentKey has no parent");
  }

  static Stream<Arguments> truncateToLength() {
    return Stream.of(
        arguments(ContentKey.of("x"), 1, ContentKey.of("x")),
        arguments(ContentKey.of("x"), 2, ContentKey.of("x")),
        arguments(ContentKey.of("x"), 0, ContentKey.of()),
        arguments(ContentKey.of("x", "y", "z"), 1, ContentKey.of("x")),
        arguments(ContentKey.of("x", "y", "z"), 2, ContentKey.of("x", "y")),
        arguments(ContentKey.of("x", "y", "z"), 3, ContentKey.of("x", "y", "z")),
        arguments(ContentKey.of("x", "y", "z"), 4, ContentKey.of("x", "y", "z")),
        arguments(ContentKey.of("x", "y", "z"), 0, ContentKey.of()));
  }

  @ParameterizedTest
  @MethodSource
  void truncateToLength(ContentKey key, int len, ContentKey expected) {
    assertThat(key.truncateToLength(len)).isEqualTo(expected);
  }

  static Stream<Arguments> startsWith() {
    return Stream.of(
        arguments(ContentKey.of("x"), ContentKey.of("x", "y"), false),
        arguments(ContentKey.of("x"), ContentKey.of("x"), true),
        arguments(ContentKey.of("x"), ContentKey.of("y"), false),
        arguments(ContentKey.of("x"), ContentKey.of(), true),
        arguments(ContentKey.of("x", "y", "z"), ContentKey.of("x"), true),
        arguments(ContentKey.of("x", "y", "z"), ContentKey.of("x", "y"), true),
        arguments(ContentKey.of("x", "y", "z"), ContentKey.of("x", "y", "z"), true),
        arguments(ContentKey.of("x", "y", "z"), ContentKey.of(), true));
  }

  @ParameterizedTest
  @MethodSource
  void startsWith(ContentKey key, ContentKey other, boolean expect) {
    soft.assertThat(key.startsWith(other)).isEqualTo(expect);
    soft.assertThat(key.startsWith(Namespace.of(other))).isEqualTo(expect);
  }

  @Test
  void noParent() {
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> ContentKey.of("x").getParent())
        .withMessage("ContentKey has no parent");
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> ContentKey.of().getParent())
        .withMessage("ContentKey has no parent");
  }

  @ParameterizedTest
  @MethodSource
  void invalidElements(List<String> elements, String message) {
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> ContentKey.of(elements))
        .withMessageStartingWith("Content key")
        .withMessageEndingWith(message);
  }

  static Stream<Arguments> invalidElements() {
    return Stream.of(
        arguments(List.of(""), "must not contain an empty element"),
        arguments(List.of("", "abc"), "must not contain an empty element"),
        arguments(List.of("abc", ""), "must not contain an empty element"),
        arguments(List.of("abc", "", "def"), "must not contain an empty element"));
  }
}
