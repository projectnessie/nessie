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
import java.util.function.Consumer;

import com.google.protobuf.ByteString;
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
  void testDeserializeEmptyAttributes() {
    testDeserialize(ImmutableMap.of(), ImmutableMap.of());
  }

  @Test
  void testDeserializeBoolean() {
    testDeserialize(build(b -> b.setBoolean(true)));
  }

  @Test
  void testDeserializeNumber() {
    testDeserialize(build(b -> b.setNumber(5)));
  }

  @Test
  void testDeserializeString() {
    testDeserialize(build(b -> b.setString("value")));
  }

  @Test
  void testDeserializeBinary() {
    testDeserialize(build(b -> b.setBinary(ByteString.copyFrom(SampleEntities.createBinary(RANDOM, 10)))));
  }

  @Test
  void testDeserializeEmptyMap() {
    testDeserialize(build(b -> b.setMap(EntityProtos.EntityMap.newBuilder().putAllValue(ImmutableMap.of()))));
  }

  @Test
  void testDeserializeMap() {
    testDeserialize(build(b -> b.setMap(EntityProtos.EntityMap.newBuilder().putAllValue(ImmutableMap.of(
      "key", build(n -> n.setNumber(5)),
      "key2", build(n -> n.setBoolean(false)),
      "key3", build(n -> n.setMap(EntityProtos.EntityMap.newBuilder().putAllValue(ImmutableMap.of())))
    )))));
  }

  @Test
  void testDeserializeEmptyList() {
    testDeserialize(build(b -> b.setList(EntityProtos.EntityList.newBuilder().addAllValue(ImmutableList.of()))));
  }

  @Test
  void testDeserializeList() {
    testDeserialize(build(b -> b.setList(EntityProtos.EntityList.newBuilder().addAllValue(ImmutableList.of(
      build(n -> n.setString("myValue")),
      build(n -> n.setNumber(999)),
      build(n -> n.setMap(EntityProtos.EntityMap.newBuilder().putAllValue(ImmutableMap.of()))))))));
  }

  @Test
  void testSerializeEmptyAttributes() {
    testSerialize(ImmutableMap.of(), ImmutableMap.of());
  }

  @Test
  void testSerializeBoolean() {
    testSerialize(Entity.ofBoolean(true));
  }

  @Test
  void testSerializeNumber() {
    testSerialize(Entity.ofNumber(5));
  }

  @Test
  void testSerializeString() {
    testSerialize(Entity.ofString("value"));
  }

  @Test
  void testSerializeBinary() {
    testSerialize(Entity.ofBinary(SampleEntities.createBinary(RANDOM, 10)));
  }

  @Test
  void testSerializeEmptyMap() {
    testSerialize(Entity.ofMap(ImmutableMap.of()));
  }

  @Test
  void testSerializeMap() {
    testSerialize(Entity.ofMap(ImmutableMap.of(
            "key", Entity.ofNumber(5),
            "key2", Entity.ofBoolean(false),
            "key3", Entity.ofMap(ImmutableMap.of())
        )));
  }

  @Test
  void testSerializeEmptyList() {
    testSerialize(Entity.ofList());
  }

  @Test
  void testSerializeList() {
    testSerialize(Entity.ofList(Entity.ofString("myValue"), Entity.ofNumber(999), Entity.ofMap(ImmutableMap.of())));
  }

  private Entity convert(EntityProtos.Entity entity) {
    switch (entity.getValueCase()) {
      case MAP:
        final ImmutableMap.Builder<String, Entity> mapBuilder = ImmutableMap.builder();
        entity.getMap().getValueMap().forEach((k, v) -> mapBuilder.put(k, convert(v)));
        return Entity.ofMap(mapBuilder.build());
      case LIST:
        final ImmutableList.Builder<Entity> listBuilder = ImmutableList.builder();
        entity.getList().getValueList().forEach(e -> listBuilder.add(convert(e)));
        return Entity.ofList(listBuilder.build());
      case NUMBER:
        return Entity.ofNumber(entity.getNumber());
      case STRING:
        return Entity.ofString(entity.getString());
      case BOOLEAN:
        return Entity.ofBoolean(entity.getBoolean());
      case BINARY:
        return Entity.ofBinary(entity.getBinary());
      default:
        Assertions.fail("Unknown Entity type.");
        throw new RuntimeException();
    }
  }

  private EntityProtos.Entity build(Consumer<EntityProtos.Entity.Builder> func) {
    final EntityProtos.Entity.Builder builder = EntityProtos.Entity.newBuilder();
    func.accept(builder);
    return builder.build();
  }

  private EntityProtos.Entity convert(Entity entity) {
    final EntityProtos.Entity.Builder builder = EntityProtos.Entity.newBuilder();
    switch (entity.getType()) {
      case MAP:
        final ImmutableMap.Builder<String, EntityProtos.Entity> mapBuilder = ImmutableMap.builder();
        entity.getMap().forEach((k, v) -> mapBuilder.put(k, convert(v)));
        builder.setMap(EntityProtos.EntityMap.newBuilder().putAllValue(mapBuilder.build()));
        break;
      case LIST:
        final ImmutableList.Builder<EntityProtos.Entity> listBuilder = ImmutableList.builder();
        entity.getList().forEach(e -> listBuilder.add(convert(e)));
        builder.setList(EntityProtos.EntityList.newBuilder().addAllValue(listBuilder.build()));
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

  private void testDeserialize(EntityProtos.Entity entity) {
    testDeserialize(ImmutableMap.of("key", entity), ImmutableMap.of("key", convert(entity)));
  }

  private void testDeserialize(Map<String, EntityProtos.Entity> attributes, Map<String, Entity> expected) {
    Assertions.assertEquals(expected, VALUE_SERDE.deserialize(attributes));
  }

  private void testSerialize(Entity entity) {
    testSerialize(ImmutableMap.of("key", entity), ImmutableMap.of("key", convert(entity)));
  }

  private void testSerialize(Map<String, Entity> attributes, Map<String, EntityProtos.Entity> expected) {
    Assertions.assertEquals(expected, VALUE_SERDE.serialize(attributes));
  }
}
