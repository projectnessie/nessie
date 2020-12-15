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
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.ImmutableMap;

class TestEntityToKryoConverter {
  private static final Random RANDOM = new Random(16234910234034L);

  @Test
  public void readEmptyAttributes() {
    testRead(ImmutableMap.of());
  }

  @Test
  public void readBoolean() {
    testRead(ImmutableMap.of("value", Entity.ofBoolean(true)));
  }

  @Test
  public void readNumber() {
    testRead(ImmutableMap.of("value", Entity.ofNumber(5)));
  }

  @Test
  public void readString() {
    testRead(ImmutableMap.of("value", Entity.ofString("string")));
  }

  @Test
  public void readBinary() {
    final byte[] buffer = SampleEntities.createBinary(RANDOM, 10);
    testRead(ImmutableMap.of("value", Entity.ofBinary(buffer)));
  }

  @Test
  public void readEmptyMap() {
    testRead(ImmutableMap.of("value", Entity.ofMap(ImmutableMap.of())));
  }

  @Test
  public void readMap() {
    testRead(ImmutableMap.of("value", Entity.ofMap(ImmutableMap.of(
        "key", Entity.ofNumber(5),
        "key2", Entity.ofBoolean(false),
        "key3", Entity.ofMap(ImmutableMap.of())
      ))));
  }

  @Test
  public void readEmptyList() {
    testRead(ImmutableMap.of("value", Entity.ofList()));
  }

  @Test
  public void readList() {
    testRead(ImmutableMap.of("value",
        Entity.ofList(Entity.ofString("myValue"), Entity.ofNumber(999), Entity.ofMap(ImmutableMap.of()))));
  }

  private void testRead(Map<String, Entity> attributes) {
    final Output output = new Output(RocksDBStore.OUTPUT_SIZE);
    ValueKryo.ENTITY_TO_KRYO_CONVERTER.write(output, attributes);
    Assertions.assertArrayEquals(TestSerializer.SERIALIZER.toBytes(attributes), output.toBytes());
  }
}
