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
package com.dremio.nessie.versioned.store.rocksdb;

import java.util.Map;
import java.util.Random;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.dremio.nessie.versioned.impl.SampleEntities;
import com.dremio.nessie.versioned.store.Entity;
import com.esotericsoftware.kryo.io.Input;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

class TestKryoToEntityConverter {
  private static final Random RANDOM = new Random(-9082734792382L);

  @Test
  public void readInvalid() {
    // Don't use actual random bytes as that could lead to a flaky test.
    final byte[] random = {34, -21, 56, 90, 111, -34, 45, -34, 59, -3, -107};
    final Input input = new Input(random);
    Assertions.assertThrows(IllegalArgumentException.class, () -> ValueKryo.KRYO_TO_ENTITY_CONVERTER.read(input));
  }

  @Test
  public void readEmptyMap() {
    testRead(ImmutableMap.of());
  }

  @Test
  public void readBoolean() {
    testRead(ImmutableMap.of("value", Entity.ofBoolean(true)));
  }

  @Test
  public void readNumber() {
    testRead(ImmutableMap.of("value", Entity.ofNumber(55)));
  }

  @Test
  public void readString() {
    testRead(ImmutableMap.of("value", Entity.ofString("myString")));
  }

  @Test
  public void readBinary() {
    final byte[] buffer = SampleEntities.createBinary(RANDOM, 10);
    testRead(ImmutableMap.of("value", Entity.ofBinary(buffer)));
  }

  @Test
  public void readNestedEmptyMap() {
    testRead(ImmutableMap.of("value", Entity.ofMap(ImmutableMap.of())));
  }

  @Test
  public void readMap() {
    testRead(ImmutableMap.of("value", Entity.ofMap(ImmutableMap.of(
        "key1", Entity.ofString("str1"),
        "key2", Entity.ofBoolean(false),
        "key3", Entity.ofList()
    ))));
  }

  @Test
  public void readEmptyList() {
    testRead(ImmutableMap.of("value", Entity.ofList()));
  }

  @Test
  public void readList() {
    testRead(ImmutableMap.of("value", Entity.ofList(ImmutableList.of(
        Entity.ofNumber(5), Entity.ofString("Str"), Entity.ofBoolean(false), Entity.ofMap(ImmutableMap.of())
    ))));
  }

  private void testRead(Map<String, Entity> expected) {
    final Input input = new Input(TestSerializer.SERIALIZER.toBytes(expected));
    final Map<String, Entity> actual = ValueKryo.KRYO_TO_ENTITY_CONVERTER.read(input);
    Assertions.assertEquals(expected, actual);
  }
}
