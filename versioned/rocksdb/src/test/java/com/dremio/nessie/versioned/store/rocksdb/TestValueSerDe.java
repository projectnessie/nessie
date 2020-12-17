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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

class TestValueSerDe {
  private static final Random RANDOM = new Random(16234910234034L);
  private static final ValueSerDe VALUE_SERDE = new ValueSerDe();

  @Test
  void testEmptyAttributes() {
    test(ImmutableMap.of(), ImmutableMap.of());
  }

  @Test
  void testBoolean() {
    test(Entity.ofBoolean(true));
  }

  @Test
  void testNumber() {
    test(Entity.ofNumber(5));
  }

  @Test
  void testString() {
    test(Entity.ofString("value"));
  }

  @Test
  void testBinary() {
    test(Entity.ofBinary(SampleEntities.createBinary(RANDOM, 10)));
  }

  @Test
  void testEmptyMap() {
    test(Entity.ofMap(ImmutableMap.of()));
  }

  @Test
  void testMap() {
    test(Entity.ofMap(ImmutableMap.of(
            "key", Entity.ofNumber(5),
            "key2", Entity.ofBoolean(false),
            "key3", Entity.ofMap(ImmutableMap.of())
        )));
  }

  @Test
  void testEmptyList() {
    test(Entity.ofList());
  }

  @Test
  void testList() {
    test(Entity.ofList(Entity.ofString("myValue"), Entity.ofNumber(999), Entity.ofMap(ImmutableMap.of())));
  }

  private EntityProtos.Entity convert(Entity entity) {
    final EntityProtos.Entity.Builder builder = EntityProtos.Entity.newBuilder();
    builder.setType(entity.getType().ordinal());
    switch (entity.getType()) {
      case MAP:
        final ImmutableMap.Builder<String, EntityProtos.Entity> mapBuilder = ImmutableMap.builder();
        entity.getMap().forEach((k, v) -> mapBuilder.put(k, convert(v)));
        builder.setMap(EntityProtos.Map.newBuilder().putAllValue(mapBuilder.build()));
        break;
      case LIST:
        final ImmutableList.Builder<EntityProtos.Entity> listBuilder = ImmutableList.builder();
        entity.getList().forEach(e -> listBuilder.add(convert(e)));
        builder.setList(EntityProtos.List.newBuilder().addAllValue(listBuilder.build()));
        break;
      case NUMBER:
        builder.setNumber(entity.getNumber());
        break;
      case STRING:
        builder.setString(entity.getString());
        break;
      case BOOLEAN:
        builder.setBoolean(entity.getBoolean());
        break;
      case BINARY:
        builder.setBinary(entity.getBinary());
        break;
      default:
        Assertions.fail("Unknown Entity type.");
    }
    return builder.build();
  }

  private void test(Entity entity) {
    test(ImmutableMap.of("key", entity), ImmutableMap.of("key", convert(entity)));
  }

  private void test(Map<String, Entity> attributes, Map<String, EntityProtos.Entity> expected) {
    final Map<String, EntityProtos.Entity> actual = VALUE_SERDE.serialize(attributes);
    Assertions.assertEquals(expected, actual);
  }
}
