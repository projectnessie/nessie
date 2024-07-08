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
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.projectnessie.model.Namespace.Empty.EMPTY_NAMESPACE;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class TestContentKey {

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
        .hasMessage("Key too long, max allowed length: " + ContentKey.MAX_LENGTH);
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
        .hasMessage("Key too long, max allowed number of elements: " + ContentKey.MAX_ELEMENTS);
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

  @ParameterizedTest
  @MethodSource("contentKeyOfAndParseCases")
  void contentKeyOfAndParse(ContentKeyOfParse testCase) {
    assertThat(testCase.key)
        .extracting(
            ContentKey::getElements,
            ContentKey::getName,
            ContentKey::getNamespace,
            ContentKey::toString,
            ContentKey::toPathString)
        .containsExactly(
            testCase.elements,
            testCase.name,
            testCase.namespace,
            testCase.string,
            testCase.pathString);
  }

  static class ContentKeyOfParse {
    final ContentKey key;
    final List<String> elements;
    final String name;
    final Namespace namespace;
    final String string;
    final String pathString;

    ContentKeyOfParse(
        ContentKey key,
        List<String> elements,
        String name,
        Namespace namespace,
        String string,
        String pathString) {
      this.key = key;
      this.elements = elements;
      this.name = name;
      this.namespace = namespace;
      this.string = string;
      this.pathString = pathString;
    }

    @Override
    public String toString() {
      return "key="
          + key
          + ", elements="
          + elements
          + ", namespace="
          + namespace
          + ", string='"
          + string
          + '\''
          + ", pathString='"
          + pathString
          + '\'';
    }
  }

  static List<ContentKeyOfParse> contentKeyOfAndParseCases() {
    return asList(
        new ContentKeyOfParse(
            ContentKey.fromPathString("a.table"),
            asList("a", "table"),
            "table",
            Namespace.of("a"),
            "a.table",
            "a.table"),
        new ContentKeyOfParse(
            ContentKey.of("a", "table"),
            asList("a", "table"),
            "table",
            Namespace.of("a"),
            "a.table",
            "a.table"),
        new ContentKeyOfParse(
            ContentKey.of(asList("a", "table")),
            asList("a", "table"),
            "table",
            Namespace.of("a"),
            "a.table",
            "a.table"),
        new ContentKeyOfParse(
            ContentKey.of(Namespace.of("a"), "table"),
            asList("a", "table"),
            "table",
            Namespace.of("a"),
            "a.table",
            "a.table"),
        new ContentKeyOfParse(
            ContentKey.of(Namespace.of(singletonList("a")), "table"),
            asList("a", "table"),
            "table",
            Namespace.of("a"),
            "a.table",
            "a.table"),
        new ContentKeyOfParse(
            ContentKey.of(Namespace.parse("a"), "table"),
            asList("a", "table"),
            "table",
            Namespace.of("a"),
            "a.table",
            "a.table"),
        //
        new ContentKeyOfParse(
            ContentKey.fromPathString("a.b.table"),
            asList("a", "b", "table"),
            "table",
            Namespace.of("a", "b"),
            "a.b.table",
            "a.b.table"),
        new ContentKeyOfParse(
            ContentKey.of("a", "b", "table"),
            asList("a", "b", "table"),
            "table",
            Namespace.of("a", "b"),
            "a.b.table",
            "a.b.table"),
        new ContentKeyOfParse(
            ContentKey.of(asList("a", "b", "table")),
            asList("a", "b", "table"),
            "table",
            Namespace.of("a", "b"),
            "a.b.table",
            "a.b.table"),
        new ContentKeyOfParse(
            ContentKey.of(Namespace.of("a", "b"), "table"),
            asList("a", "b", "table"),
            "table",
            Namespace.of("a", "b"),
            "a.b.table",
            "a.b.table"),
        new ContentKeyOfParse(
            ContentKey.of(Namespace.of(asList("a", "b")), "table"),
            asList("a", "b", "table"),
            "table",
            Namespace.of("a", "b"),
            "a.b.table",
            "a.b.table"),
        new ContentKeyOfParse(
            ContentKey.of(Namespace.parse("a.b"), "table"),
            asList("a", "b", "table"),
            "table",
            Namespace.of("a", "b"),
            "a.b.table",
            "a.b.table"),
        //
        new ContentKeyOfParse(
            ContentKey.of(EMPTY_NAMESPACE, "table"),
            singletonList("table"),
            "table",
            Namespace.parse(""),
            "table",
            "table"),
        new ContentKeyOfParse(
            ContentKey.of("table"),
            singletonList("table"),
            "table",
            Namespace.parse(""),
            "table",
            "table"),
        new ContentKeyOfParse(
            ContentKey.of(singletonList("table")),
            singletonList("table"),
            "table",
            Namespace.parse(""),
            "table",
            "table"),
        new ContentKeyOfParse(
            ContentKey.fromPathString("table"),
            singletonList("table"),
            "table",
            Namespace.parse(""),
            "table",
            "table"));
  }

  @Test
  void namespaceNotIncluded() throws IOException {
    ObjectMapper mapper = new ObjectMapper();

    Namespace namespace = Namespace.parse("a.b.c");
    assertThat(namespace.name()).isEqualTo("a.b.c");
    assertThat(namespace.getElements()).containsExactly("a", "b", "c");

    ContentKey key = ContentKey.of("a", "b", "c", "tableName");
    assertThat(key.getNamespace()).isEqualTo(namespace);
    String serializedKey = mapper.writeValueAsString(key);
    assertThat(serializedKey).contains("elements").doesNotContain("namespace");

    ContentKey deserialized = mapper.readValue(serializedKey, ContentKey.class);
    assertThat(deserialized).isEqualTo(key);
    assertThat(deserialized.getNamespace()).isEqualTo(namespace);
  }

  @Test
  public void validation() {
    assertThatThrownBy(() -> ContentKey.of("a", "b", "\u0000", "c", "d"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Content key '[a, b, \u0000, c, d]' must not contain characters less than 0x20.");

    assertThatThrownBy(() -> ContentKey.of("a", "b", "\u001D", "c", "d"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Content key '[a, b, \u001D, c, d]' must not contain characters less than 0x20.");
  }

  @Test
  public void construction() {
    String[] elements = {"a", "b", "c", "d"};
    ContentKey key = ContentKey.of(elements);
    assertThat(key.getElements()).containsExactlyElementsOf(asList(elements));
    assertThat(key.toPathString()).isEqualTo(String.join(".", elements));
    assertThat(ContentKey.fromPathString("a.b.c.d")).isEqualTo(key);
  }

  @Test
  void singleByte() {
    assertRoundTrip("a.b", "c.d");
  }

  @Test
  void strangeCharacters() {
    assertRoundTrip("/%", "#&&");
  }

  @Test
  void doubleByte() {
    assertRoundTrip("/%国", "国.国");
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
    assertAll(
        () ->
            assertThatThrownBy(() -> ContentKey.of((String[]) null))
                .isInstanceOf(NullPointerException.class),
        () ->
            assertThatThrownBy(() -> ContentKey.of(null, null))
                .isInstanceOf(NullPointerException.class),
        () ->
            assertThatThrownBy(() -> ContentKey.of("a", null))
                .isInstanceOf(NullPointerException.class),
        () ->
            assertThatThrownBy(() -> ContentKey.of((List<String>) null))
                .isInstanceOf(NullPointerException.class),
        () ->
            assertThatThrownBy(() -> ContentKey.of(singletonList(null)))
                .isInstanceOf(NullPointerException.class),
        () ->
            assertThatThrownBy(() -> ContentKey.of(asList("a", null)))
                .isInstanceOf(NullPointerException.class),
        () ->
            assertThatThrownBy(() -> ContentKey.of(asList(null, "a")))
                .isInstanceOf(NullPointerException.class));
  }

  @Test
  void empty() {
    assertAll(
        () ->
            assertThatThrownBy(() -> ContentKey.of(null, ""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Content key '[]' must not contain an empty element."),
        () ->
            assertThatThrownBy(() -> ContentKey.of("a", ""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Content key '[a, ]' must not contain an empty element."),
        () ->
            assertThatThrownBy(() -> ContentKey.of(singletonList("")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Content key '[]' must not contain an empty element."),
        () ->
            assertThatThrownBy(() -> ContentKey.of(asList("a", "")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Content key '[a, ]' must not contain an empty element."),
        () ->
            assertThatThrownBy(() -> ContentKey.of("", "something"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Content key '[, something]' must not contain an empty element."),
        () ->
            assertThatThrownBy(() -> ContentKey.of("", "something", "x"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Content key '[, something, x]' must not contain an empty element."));
  }

  private void assertRoundTrip(String... elements) {
    ContentKey k = ContentKey.of(elements);
    ContentKey k2 = ContentKey.fromPathString(k.toPathString());
    assertEquals(k, k2);
  }

  @Test
  void toParentWithNoParent() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> ContentKey.of("x").getParent())
        .withMessage("ContentKey has no parent");
  }

  static Stream<Arguments> getParent() {
    return Stream.of(
        arguments(ContentKey.of("a", "b", "c", "d"), ContentKey.of("a", "b", "c")),
        arguments(ContentKey.of("a", "b", "c"), ContentKey.of("a", "b")),
        arguments(ContentKey.of("a", "b"), ContentKey.of("a")));
  }

  @ParameterizedTest
  @MethodSource
  void getParent(ContentKey in, ContentKey parent) {
    assertThat(in).extracting(ContentKey::getParent).isEqualTo(parent);
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
    assertThat(key.startsWith(other)).isEqualTo(expect);
    assertThat(key.startsWith(Namespace.of(other))).isEqualTo(expect);
  }
}
