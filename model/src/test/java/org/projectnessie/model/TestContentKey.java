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
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

@Execution(ExecutionMode.CONCURRENT)
class TestContentKey {

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
            ContentKey.of(Namespace.EMPTY, "table"),
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
            "Content key '[a, b, \u0000, c, d]' must not contain a zero byte (\\u0000) / group separator (\\u001D).");

    assertThatThrownBy(() -> ContentKey.of("a", "b", "\u001D", "c", "d"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Content key '[a, b, \u001D, c, d]' must not contain a zero byte (\\u0000) / group separator (\\u001D).");
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

  @ParameterizedTest
  @ValueSource(strings = {"\u0000", "\u001D"})
  void blockZeroByteUsage(String s) {
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
}
