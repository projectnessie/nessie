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
package org.projectnessie.client;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.model.ContentsKey;
import org.projectnessie.model.Namespace;

public class TestTableReference {

  @Test
  public void noMarkings() {
    String path = "foo";
    TableReference tr = TableReference.parse(path);
    assertThat(path).isEqualTo(tr.contentsKey().toPathString());
    assertThat(tr.reference()).isNull();
    assertThat(tr.hash()).isNull();
    assertThat(tr.timestamp()).isNull();
  }

  @Test
  public void branchOnly() {
    String path = "foo@bar";
    TableReference pti = TableReference.parse(path);
    assertThat("foo").isEqualTo(pti.contentsKey().getName());
    assertThat("bar").isEqualTo(pti.reference());
    assertThat(pti.timestamp()).isNull();
  }

  static List<Object[]> fromContentsKeyTestCases() {
    return Arrays.asList(
        new Object[] {
          Namespace.of("ns1", "ns2"),
          "simple_name",
          ContentsKey.of(Namespace.of("ns1", "ns2"), "simple_name"),
          null,
          null,
          null
        },
        new Object[] {
          Namespace.of("ns1", "ns2"),
          "`simple_name@ref`",
          ContentsKey.of(Namespace.of("ns1", "ns2"), "simple_name"),
          "ref",
          null,
          null
        },
        new Object[] {
          Namespace.of("ns1", "ns2"),
          "'simple_name@ref'",
          ContentsKey.of(Namespace.of("ns1", "ns2"), "simple_name"),
          "ref",
          null,
          null
        },
        new Object[] {
          Namespace.of("ns1", "ns2"),
          "`simple_name@ref#12345678abcdef12345678abcdef`",
          ContentsKey.of(Namespace.of("ns1", "ns2"), "simple_name"),
          "ref",
          "12345678abcdef12345678abcdef",
          null
        },
        new Object[] {
          Namespace.of("ns1", "ns2"),
          "`simple_name#12345678abcdef12345678abcdef`",
          ContentsKey.of(Namespace.of("ns1", "ns2"), "simple_name"),
          null,
          "12345678abcdef12345678abcdef",
          null
        });
  }

  @ParameterizedTest
  @MethodSource("fromContentsKeyTestCases")
  public void fromContentsKey(
      Namespace namespace,
      String name,
      ContentsKey expectedContentsKey,
      String expectedReference,
      String expectedHash,
      String expectedTimestamp) {
    TableReference tr = TableReference.parse(namespace, name);
    assertThat(tr)
        .extracting(
            TableReference::contentsKey,
            TableReference::reference,
            TableReference::hash,
            TableReference::timestamp)
        .containsExactly(expectedContentsKey, expectedReference, expectedHash, expectedTimestamp);
  }

  @Test
  public void branchAndTimestamp() {
    String path = "foo@bar#baz";
    Assertions.assertThatThrownBy(() -> TableReference.parse(path))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(TableReference.ILLEGAL_HASH_MESSAGE);
  }

  @Test
  public void twoBranches() {
    String path = "foo@bar@boo";
    Assertions.assertThatThrownBy(() -> TableReference.parse(path))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(TableReference.ILLEGAL_PATH_MESSAGE);
  }

  @Test
  public void twoTimestamps() {
    String path = "foo#baz#baa";
    Assertions.assertThatThrownBy(() -> TableReference.parse(path))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(TableReference.ILLEGAL_PATH_MESSAGE);
  }

  @Test
  public void strangeCharacters() {
    String branch = "bar";
    String path = "/%";
    TableReference pti = TableReference.parse(path);
    assertThat(path).isEqualTo(pti.contentsKey().getName());
    assertThat(pti.reference()).isNull();
    assertThat(pti.timestamp()).isNull();
    pti = TableReference.parse(path + "@" + branch);
    assertThat(path).isEqualTo(pti.contentsKey().getName());
    assertThat(branch).isEqualTo(pti.reference());
    assertThat(pti.timestamp()).isNull();
    path = "&&";
    pti = TableReference.parse(path);
    assertThat(path).isEqualTo(pti.contentsKey().getName());
    assertThat(pti.reference()).isNull();
    assertThat(pti.timestamp()).isNull();
    pti = TableReference.parse(path + "@" + branch);
    assertThat(path).isEqualTo(pti.contentsKey().getName());
    assertThat(branch).isEqualTo(pti.reference());
    assertThat(pti.timestamp()).isNull();
  }

  @Test
  public void doubleByte() {
    String branch = "bar";
    String path = "/%国";
    TableReference pti = TableReference.parse(path);
    assertThat(path).isEqualTo(pti.contentsKey().getName());
    assertThat(pti.reference()).isNull();
    assertThat(pti.timestamp()).isNull();
    pti = TableReference.parse(path + "@" + branch);
    assertThat(path).isEqualTo(pti.contentsKey().getName());
    assertThat(branch).isEqualTo(pti.reference());
    assertThat(pti.timestamp()).isNull();
    path = "国.国";
    pti = TableReference.parse(path);
    assertThat(path).isEqualTo(pti.contentsKey().toString());
    assertThat(pti.reference()).isNull();
    assertThat(pti.timestamp()).isNull();
    pti = TableReference.parse(path + "@" + branch);
    assertThat(path).isEqualTo(pti.contentsKey().toString());
    assertThat(branch).isEqualTo(pti.reference());
    assertThat(pti.timestamp()).isNull();
  }

  @Test
  public void whitespace() {
    String branch = "bar";
    String path = "foo ";
    TableReference pti = TableReference.parse(path);
    assertThat(path).isEqualTo(pti.contentsKey().getName());
    assertThat(pti.reference()).isNull();
    assertThat(pti.timestamp()).isNull();
    pti = TableReference.parse(path + "@" + branch);
    assertThat(path).isEqualTo(pti.contentsKey().getName());
    assertThat(branch).isEqualTo(pti.reference());
    assertThat(pti.timestamp()).isNull();
  }
}
