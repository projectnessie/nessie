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

import java.util.Arrays;
import java.util.List;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

public class TestTableReference {

  static List<Object[]> fromContentsKeyTestCases() {
    return Arrays.asList(
        new Object[] {"simple_name", "simple_name", null, null, null},
        new Object[] {"`simple_name@ref`", "simple_name", "ref", null, null},
        new Object[] {
          "`simple_name@ref#12345678abcdef12345678abcdef`",
          "simple_name",
          "ref",
          "12345678abcdef12345678abcdef",
          null
        },
        new Object[] {
          "`simple_name#12345678abcdef12345678abcdef`",
          "simple_name",
          null,
          "12345678abcdef12345678abcdef",
          null
        },
        new Object[] {"`simple_name#2020-12-24`", "simple_name", null, null, "2020-12-24"},
        new Object[] {"`simple_name@ref#2020-12-24`", "simple_name", "ref", null, "2020-12-24"});
  }

  @ParameterizedTest
  @MethodSource("fromContentsKeyTestCases")
  public void fromContentsKey(
      String name,
      String expectedName,
      String expectedReference,
      String expectedHash,
      String expectedTimestamp) {
    TableReference tr = TableReference.parse(name);
    assertThat(tr)
        .extracting(
            TableReference::getName,
            TableReference::hasReference,
            TableReference::getReference,
            TableReference::hasHash,
            TableReference::getHash,
            TableReference::hasTimestamp,
            TableReference::getTimestamp,
            TableReference::toString)
        .containsExactly(
            expectedName,
            expectedReference != null,
            expectedReference,
            expectedHash != null,
            expectedHash,
            expectedTimestamp != null,
            expectedTimestamp,
            name.replaceAll("'", "`"));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "foo@bar@boo",
        "foo#baz#baa",
        "foo@#baa",
        "foo@#",
        "foo@bar#",
        "@bar#baz",
        "@#baz",
        "@#baz",
        "#baz",
        "#",
        "@",
        ""
      })
  void illegalSyntax(String tableReference) {
    Assertions.assertThatThrownBy(() -> TableReference.parse(tableReference))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(String.format(TableReference.ILLEGAL_TABLE_REFERENCE_MESSAGE, tableReference));
  }

  @Test
  public void strangeCharacters() {
    String branch = "bar";
    String path = "/%";
    TableReference tr = TableReference.parse(path);
    assertThat(path).isEqualTo(tr.getName());
    assertThat(tr.getReference()).isNull();
    assertThat(tr.getTimestamp()).isNull();
    tr = TableReference.parse(path + "@" + branch);
    assertThat(path).isEqualTo(tr.getName());
    assertThat(branch).isEqualTo(tr.getReference());
    assertThat(tr.getTimestamp()).isNull();
    path = "&&";
    tr = TableReference.parse(path);
    assertThat(path).isEqualTo(tr.getName());
    assertThat(tr.getReference()).isNull();
    assertThat(tr.getTimestamp()).isNull();
    tr = TableReference.parse(path + "@" + branch);
    assertThat(path).isEqualTo(tr.getName());
    assertThat(branch).isEqualTo(tr.getReference());
    assertThat(tr.getTimestamp()).isNull();
  }

  @Test
  public void doubleByte() {
    String branch = "bar";
    String path = "/%国";
    TableReference tr = TableReference.parse(path);
    assertThat(path).isEqualTo(tr.getName());
    assertThat(tr.getReference()).isNull();
    assertThat(tr.getTimestamp()).isNull();
    tr = TableReference.parse(path + "@" + branch);
    assertThat(path).isEqualTo(tr.getName());
    assertThat(branch).isEqualTo(tr.getReference());
    assertThat(tr.getTimestamp()).isNull();
    path = "国.国";
    tr = TableReference.parse(path);
    assertThat(path).isEqualTo(tr.toString());
    assertThat(tr.getReference()).isNull();
    assertThat(tr.getTimestamp()).isNull();
    tr = TableReference.parse(path + "@" + branch);
    assertThat("`" + path + "@" + branch + "`").isEqualTo(tr.toString());
    assertThat(branch).isEqualTo(tr.getReference());
    assertThat(tr.getTimestamp()).isNull();
  }

  @Test
  public void whitespace() {
    String branch = "bar";
    String path = "foo ";
    TableReference tr = TableReference.parse(path);
    assertThat(path).isEqualTo(tr.getName());
    assertThat(tr.getReference()).isNull();
    assertThat(tr.getTimestamp()).isNull();
    tr = TableReference.parse(path + "@" + branch);
    assertThat(path).isEqualTo(tr.getName());
    assertThat(branch).isEqualTo(tr.getReference());
    assertThat(tr.getTimestamp()).isNull();
  }
}
