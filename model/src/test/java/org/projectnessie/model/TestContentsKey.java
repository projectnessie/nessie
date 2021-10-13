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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class TestContentsKey {

  @ParameterizedTest
  @MethodSource("contentsKeyOfAndParseCases")
  void contentsKeyOfAndParse(ContentsKeyOfParse testCase) {
    assertThat(testCase.key)
        .extracting(
            ContentsKey::getElements,
            ContentsKey::getName,
            ContentsKey::getNamespace,
            ContentsKey::toString,
            ContentsKey::toPathString)
        .containsExactly(
            testCase.elements,
            testCase.name,
            testCase.namespace,
            testCase.string,
            testCase.pathString);
  }

  static class ContentsKeyOfParse {
    final ContentsKey key;
    final List<String> elements;
    final String name;
    final Namespace namespace;
    final String string;
    final String pathString;

    ContentsKeyOfParse(
        ContentsKey key,
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

  static List<ContentsKeyOfParse> contentsKeyOfAndParseCases() {
    return asList(
        new ContentsKeyOfParse(
            ContentsKey.fromPathString("a.table"),
            asList("a", "table"),
            "table",
            Namespace.of("a"),
            "a.table",
            "a.table"),
        new ContentsKeyOfParse(
            ContentsKey.of("a", "table"),
            asList("a", "table"),
            "table",
            Namespace.of("a"),
            "a.table",
            "a.table"),
        new ContentsKeyOfParse(
            ContentsKey.of(asList("a", "table")),
            asList("a", "table"),
            "table",
            Namespace.of("a"),
            "a.table",
            "a.table"),
        new ContentsKeyOfParse(
            ContentsKey.of(Namespace.of("a"), "table"),
            asList("a", "table"),
            "table",
            Namespace.of("a"),
            "a.table",
            "a.table"),
        new ContentsKeyOfParse(
            ContentsKey.of(Namespace.of(singletonList("a")), "table"),
            asList("a", "table"),
            "table",
            Namespace.of("a"),
            "a.table",
            "a.table"),
        new ContentsKeyOfParse(
            ContentsKey.of(Namespace.parse("a"), "table"),
            asList("a", "table"),
            "table",
            Namespace.of("a"),
            "a.table",
            "a.table"),
        //
        new ContentsKeyOfParse(
            ContentsKey.fromPathString("a.b.table"),
            asList("a", "b", "table"),
            "table",
            Namespace.of("a", "b"),
            "a.b.table",
            "a.b.table"),
        new ContentsKeyOfParse(
            ContentsKey.of("a", "b", "table"),
            asList("a", "b", "table"),
            "table",
            Namespace.of("a", "b"),
            "a.b.table",
            "a.b.table"),
        new ContentsKeyOfParse(
            ContentsKey.of(asList("a", "b", "table")),
            asList("a", "b", "table"),
            "table",
            Namespace.of("a", "b"),
            "a.b.table",
            "a.b.table"),
        new ContentsKeyOfParse(
            ContentsKey.of(Namespace.of("a", "b"), "table"),
            asList("a", "b", "table"),
            "table",
            Namespace.of("a", "b"),
            "a.b.table",
            "a.b.table"),
        new ContentsKeyOfParse(
            ContentsKey.of(Namespace.of(asList("a", "b")), "table"),
            asList("a", "b", "table"),
            "table",
            Namespace.of("a", "b"),
            "a.b.table",
            "a.b.table"),
        new ContentsKeyOfParse(
            ContentsKey.of(Namespace.parse("a.b"), "table"),
            asList("a", "b", "table"),
            "table",
            Namespace.of("a", "b"),
            "a.b.table",
            "a.b.table"),
        //
        new ContentsKeyOfParse(
            ContentsKey.of(Namespace.EMPTY, "table"),
            singletonList("table"),
            "table",
            Namespace.parse(""),
            "table",
            "table"),
        new ContentsKeyOfParse(
            ContentsKey.of("table"),
            singletonList("table"),
            "table",
            Namespace.parse(""),
            "table",
            "table"),
        new ContentsKeyOfParse(
            ContentsKey.of(singletonList("table")),
            singletonList("table"),
            "table",
            Namespace.parse(""),
            "table",
            "table"),
        new ContentsKeyOfParse(
            ContentsKey.fromPathString("table"),
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

    ContentsKey key = ContentsKey.of("a", "b", "c", "tableName");
    assertThat(key.getNamespace()).isEqualTo(namespace);
    String serializedKey = mapper.writeValueAsString(key);
    assertThat(serializedKey).contains("elements").doesNotContain("namespace");

    ContentsKey deserialized = mapper.readValue(serializedKey, ContentsKey.class);
    assertThat(deserialized).isEqualTo(key);
    assertThat(deserialized.getNamespace()).isEqualTo(namespace);
  }

  @Test
  public void validation() {
    assertThatThrownBy(() -> ContentsKey.of("a", "b", "\u0000", "c", "d"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("An object key must not contain a zero byte.");
  }

  @Test
  public void construction() {
    String[] elements = {"a", "b", "c", "d"};
    ContentsKey key = ContentsKey.of(elements);
    assertThat(key.getElements()).containsExactlyElementsOf(asList(elements));
    assertThat(key.toPathString()).isEqualTo(String.join(".", elements));
    assertThat(ContentsKey.fromPathString("a.b.c.d")).isEqualTo(key);
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

  @Test
  void blockZeroByteUsage() {
    assertThrows(IllegalArgumentException.class, () -> ContentsKey.of("\u0000"));
  }

  @Test
  void npe() {
    assertAll(
        () ->
            assertThatThrownBy(() -> ContentsKey.of((String[]) null))
                .isInstanceOf(NullPointerException.class),
        () ->
            assertThatThrownBy(() -> ContentsKey.of(null, null))
                .isInstanceOf(NullPointerException.class),
        () ->
            assertThatThrownBy(() -> ContentsKey.of("a", null))
                .isInstanceOf(NullPointerException.class),
        () ->
            assertThatThrownBy(() -> ContentsKey.of((List<String>) null))
                .isInstanceOf(NullPointerException.class),
        () ->
            assertThatThrownBy(() -> ContentsKey.of(singletonList(null)))
                .isInstanceOf(NullPointerException.class),
        () ->
            assertThatThrownBy(() -> ContentsKey.of(asList("a", null)))
                .isInstanceOf(NullPointerException.class),
        () ->
            assertThatThrownBy(() -> ContentsKey.of(asList(null, "a")))
                .isInstanceOf(NullPointerException.class));
  }

  @Test
  void empty() {
    assertAll(
        () ->
            assertThatThrownBy(() -> ContentsKey.of(null, ""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("An object key must not contain an empty name (last element)."),
        () ->
            assertThatThrownBy(() -> ContentsKey.of("a", ""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("An object key must not contain an empty name (last element)."),
        () ->
            assertThatThrownBy(() -> ContentsKey.of(singletonList("")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("An object key must not contain an empty name (last element)."),
        () ->
            assertThatThrownBy(() -> ContentsKey.of(asList("a", "")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("An object key must not contain an empty name (last element)."));
  }

  private void assertRoundTrip(String... elements) {
    ContentsKey k = ContentsKey.of(elements);
    ContentsKey k2 = ContentsKey.fromPathString(k.toPathString());
    assertEquals(k, k2);
  }
}
