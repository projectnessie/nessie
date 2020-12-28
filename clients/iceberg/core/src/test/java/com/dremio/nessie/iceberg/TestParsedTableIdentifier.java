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
package com.dremio.nessie.iceberg;

import static com.dremio.nessie.iceberg.NessieCatalog.CONF_NESSIE_REF;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestParsedTableIdentifier {

  @Test
  void noMarkings() {
    String path = "foo";
    ParsedTableIdentifier pti =
        ParsedTableIdentifier.getParsedTableIdentifier(path, new HashMap<>());
    Assertions.assertEquals(path, pti.getTableIdentifier().name());
    Assertions.assertNull(pti.getReference());
    Assertions.assertNull(pti.getTimestamp());
  }

  @Test
  void branchOnly() {
    String path = "foo@bar";
    ParsedTableIdentifier pti =
        ParsedTableIdentifier.getParsedTableIdentifier(path, new HashMap<>());
    Assertions.assertEquals("foo", pti.getTableIdentifier().name());
    Assertions.assertEquals("bar", pti.getReference());
    Assertions.assertNull(pti.getTimestamp());
  }

  @Test
  void timestampOnly() {
    String path = "foo#baz";
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> ParsedTableIdentifier.getParsedTableIdentifier(path, new HashMap<>()));
  }

  @Test
  void branchAndTimestamp() {
    String path = "foo@bar#baz";
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> ParsedTableIdentifier.getParsedTableIdentifier(path, new HashMap<>()));
  }

  @Test
  void twoBranches() {
    String path = "foo@bar@boo";
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> ParsedTableIdentifier.getParsedTableIdentifier(path, new HashMap<>()));
  }

  @Test
  void twoTimestamps() {
    String path = "foo#baz#baa";
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> ParsedTableIdentifier.getParsedTableIdentifier(path, new HashMap<>()));
  }

  @Test
  void branchOnlyInProps() {
    String path = "foo";
    Map<String, String> map = new HashMap<>();
    map.put(CONF_NESSIE_REF, "bar");
    ParsedTableIdentifier pti = ParsedTableIdentifier.getParsedTableIdentifier(path, map);
    Assertions.assertEquals("foo", pti.getTableIdentifier().name());
    Assertions.assertEquals("bar", pti.getReference());
    Assertions.assertNull(pti.getTimestamp());
  }

  @Test
  void strangeCharacters() {
    String branch = "bar";
    String path = "/%";
    ParsedTableIdentifier pti =
        ParsedTableIdentifier.getParsedTableIdentifier(path, new HashMap<>());
    Assertions.assertEquals(path, pti.getTableIdentifier().name());
    Assertions.assertNull(pti.getReference());
    Assertions.assertNull(pti.getTimestamp());
    pti = ParsedTableIdentifier.getParsedTableIdentifier(path + "@" + branch, new HashMap<>());
    Assertions.assertEquals(path, pti.getTableIdentifier().name());
    Assertions.assertEquals(branch, pti.getReference());
    Assertions.assertNull(pti.getTimestamp());
    path = "&&";
    pti = ParsedTableIdentifier.getParsedTableIdentifier(path, new HashMap<>());
    Assertions.assertEquals(path, pti.getTableIdentifier().name());
    Assertions.assertNull(pti.getReference());
    Assertions.assertNull(pti.getTimestamp());
    pti = ParsedTableIdentifier.getParsedTableIdentifier(path + "@" + branch, new HashMap<>());
    Assertions.assertEquals(path, pti.getTableIdentifier().name());
    Assertions.assertEquals(branch, pti.getReference());
    Assertions.assertNull(pti.getTimestamp());
  }

  @Test
  void doubleByte() {
    String branch = "bar";
    String path = "/%国";
    ParsedTableIdentifier pti =
        ParsedTableIdentifier.getParsedTableIdentifier(path, new HashMap<>());
    Assertions.assertEquals(path, pti.getTableIdentifier().name());
    Assertions.assertNull(pti.getReference());
    Assertions.assertNull(pti.getTimestamp());
    pti = ParsedTableIdentifier.getParsedTableIdentifier(path + "@" + branch, new HashMap<>());
    Assertions.assertEquals(path, pti.getTableIdentifier().name());
    Assertions.assertEquals(branch, pti.getReference());
    Assertions.assertNull(pti.getTimestamp());
    path = "国.国";
    pti = ParsedTableIdentifier.getParsedTableIdentifier(path, new HashMap<>());
    Assertions.assertEquals(path, pti.getTableIdentifier().toString());
    Assertions.assertNull(pti.getReference());
    Assertions.assertNull(pti.getTimestamp());
    pti = ParsedTableIdentifier.getParsedTableIdentifier(path + "@" + branch, new HashMap<>());
    Assertions.assertEquals(path, pti.getTableIdentifier().toString());
    Assertions.assertEquals(branch, pti.getReference());
    Assertions.assertNull(pti.getTimestamp());
  }

  @Test
  void whitespace() {
    String branch = "bar ";
    String path = "foo ";
    ParsedTableIdentifier pti =
        ParsedTableIdentifier.getParsedTableIdentifier(path, new HashMap<>());
    Assertions.assertEquals(path, pti.getTableIdentifier().name());
    Assertions.assertNull(pti.getReference());
    Assertions.assertNull(pti.getTimestamp());
    pti = ParsedTableIdentifier.getParsedTableIdentifier(path + "@" + branch, new HashMap<>());
    Assertions.assertEquals(path, pti.getTableIdentifier().name());
    Assertions.assertEquals(branch, pti.getReference());
    Assertions.assertNull(pti.getTimestamp());
  }
}
