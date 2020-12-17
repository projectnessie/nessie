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

import com.dremio.nessie.versioned.store.Entity;
import com.dremio.nessie.versioned.store.ValueType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Value serializer/deserializer for use with RocksDB.
 */
class ValueSerDe {
  /**
   * Deserialize a given value into its associated Value object form.
   * @param value serialized bytes of the value.
   * @param <T> the type of the Value object.
   * @return the deserialized Value object.
   */
  <T> T deserialize(byte[] value) {
    try {
      final EntityProtos.Value protoValue = EntityProtos.Value.parseFrom(value);
      final int typeOrd = protoValue.getType();
      if (typeOrd < 0 || typeOrd > ValueType.values().length) {
        throw new IllegalArgumentException(String.format("Invalid ValueType (%d) when deserializing Value.", typeOrd));
      }

      final ValueType type = ValueType.values()[typeOrd];
      return (T) type.getSchema().mapToItem(deserialize(protoValue.getValuesMap()));
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalArgumentException("Invalid protobuf value format when deserializing Value.", e);
    }
  }

  @VisibleForTesting
  Map<String, Entity> deserialize(Map<String, EntityProtos.Entity> valuesMap) {
    final ImmutableMap.Builder<String, Entity> builder = ImmutableMap.builder();
    valuesMap.forEach((k, e) -> builder.put(k, deserialize(e)));
    return builder.build();
  }

  private Entity deserialize(EntityProtos.Entity entity) {
    final int entityType = entity.getType();
    if (entityType < 0 || entityType > Entity.EntityType.values().length) {
      throw new IllegalArgumentException(String.format("Invalid EntityType (%d) when deserializing Entity.", entityType));
    }

    final Entity.EntityType type = Entity.EntityType.values()[entityType];
    switch (type) {
      case MAP:
        return Entity.ofMap(deserialize(entity.getMapMap()));
      case LIST:
        final ImmutableList.Builder<Entity> listBuilder = ImmutableList.builder();
        entity.getListList().forEach(e -> listBuilder.add(deserialize(e)));
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
        throw new UnsupportedOperationException(String.format("Unsupported Entity type: %s", type.name()));
    }
  }

  /**
   * Serialize a given value into its associated byte form.
   * @param type type of the Value object.
   * @param value Value object to serialize.
   * @param <T> the type of the Value object.
   * @return the serialized Value object.
   */
  <T> byte[] serialize(ValueType type, T value) {
    RocksDBStore.typeCheck(type, value);
    final EntityProtos.Value.Builder builder = EntityProtos.Value.newBuilder();
    builder.setType(type.ordinal());
    builder.putAllValues(serialize(type.getSchema().itemToMap(value, true)));
    return builder.build().toByteArray();
  }

  @VisibleForTesting
  Map<String, EntityProtos.Entity> serialize(Map<String, Entity> values) {
    final ImmutableMap.Builder<String, EntityProtos.Entity> builder = ImmutableMap.builder();
    values.forEach((k, v) -> builder.put(k, serialize(v)));
    return builder.build();
  }

  private EntityProtos.Entity serialize(Entity entity) {
    final EntityProtos.Entity.Builder builder = EntityProtos.Entity.newBuilder();
    builder.setType(entity.getType().ordinal());
    switch (entity.getType()) {
      case MAP:
        builder.putAllMap(serialize(entity.getMap()));
        break;
      case LIST:
        entity.getList().forEach(e -> builder.addList(serialize(e)));
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
        throw new UnsupportedOperationException(String.format("Unsupported Entity type: %s", entity.getType().name()));
    }
    return builder.build();
  }
}
