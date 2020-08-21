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
package com.dremio.nessie.versioned.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class TestIdMap {

  private final Id id1 = Id.generateRandom();

  @Test
  void ensureImmutable() {
    IdMap map1 = new IdMap(1);
    IdMap map2 = map1.setId(0, id1);
    assertNotEquals(map1, map2);
    assertTrue(map1.getId(0).isEmpty());
    assertFalse(map2.getId(0).isEmpty());
    assertEquals(id1, map2.getId(0));
  }

  @Test
  void ensureOutOfRangeFails() {
    IdMap map1 = new IdMap(1);
    assertThrows(IndexOutOfBoundsException.class, () -> map1.getId(1));
    assertThrows(IndexOutOfBoundsException.class, () -> map1.getId(-1));
  }

  @Test
  void trivialEquals() {
    IdMap map1 = new IdMap(1);
    assertEquals(map1, map1);
    assertNotEquals(map1, id1);
  }

  @Test
  void iter() {
    IdMap map1 = new IdMap(1);
    map1 = map1.setId(0, id1);
    assertEquals(id1, map1.iterator().next());
  }

  @Test
  void checkDirty() {
    IdMap map = new IdMap(10);
    assertEquals(0, map.getChanges().size());

    // noop operation.
    map = map.setId(0, Id.EMPTY);
    assertEquals(0, map.getChanges().size());

    // real operation.
    map = map.setId(5, id1);
    assertEquals(1, map.getChanges().size());
    assertEquals(5, map.getChanges().get(0).getPosition());

    // rollback operation and confirm no changes.
    map = map.setId(5, Id.EMPTY);
    assertEquals(0, map.getChanges().size());

  }

  @Test
  void ensureRoundTrip() {

    IdMap map1 = new IdMap(15);
    for (int i = 0; i < map1.size(); i++) {
      map1 = map1.setId(i, Id.generateRandom());
    }

    IdMap map2 = IdMap.fromAttributeValue(map1.toAttributeValue(), 15);

    assertEquals(map1, map2);
    assertEquals(map1.hashCode(), map2.hashCode());
    assertNotEquals(map1.getChanges().size(), map2.getChanges().size());
  }

  @Test
  void failOnWrongDeserialization() {
    IdMap map1 = new IdMap(15);
    assertThrows(IllegalArgumentException.class, () -> IdMap.fromAttributeValue(map1.toAttributeValue(), 14));
  }



}
