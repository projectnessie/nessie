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

import java.util.List;
import java.util.Map;

import com.dremio.nessie.versioned.impl.InternalRef;
import com.dremio.nessie.versioned.store.Entity;
import com.dremio.nessie.versioned.store.SimpleSchema;
import com.dremio.nessie.versioned.store.ValueType;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;

/**
 * Kryo instance specialized for SerDe of Nessie Values and Entities.
 */
class ValueKryo extends Kryo {
  @VisibleForTesting
  static class EntityToKryoConverter {
    /**
     * Write the specified Entity attributes to binary for Kryo.
     * @param output the Kryo stream to write to.
     * @param attributes the Entity attributes to serialize.
     */
    @VisibleForTesting
    void write(Output output, Map<String, Entity> attributes) {
      output.writeInt(attributes.size());
      attributes.forEach((k, v) -> write(output, k, v));
    }

    private void write(Output output, String name, Entity entity) {
      output.writeString(name);
      write(output, entity);
    }

    private void write(Output output, Entity entity) {
      // Write the type indicator.
      output.writeShort(entity.getType().ordinal());

      switch (entity.getType()) {
        case MAP:
          output.writeInt(entity.getMap().size());
          entity.getMap().forEach((k, v) -> write(output, k, v));
          break;
        case LIST:
          output.writeInt(entity.getList().size());
          entity.getList().forEach(v -> write(output, v));
          break;
        case NUMBER:
          output.writeLong(entity.getNumber());
          break;
        case STRING:
          output.writeString(entity.getString());
          break;
        case BINARY:
          final ByteString value = entity.getBinary();
          output.writeInt(value.size());
          output.writeBytes(value.toByteArray());
          break;
        case BOOLEAN:
          output.writeBoolean(entity.getBoolean());
          break;
        default:
          throw new UnsupportedOperationException(String.format("Unsupported field type: %s", entity.getType().name()));
      }
    }
  }

  @VisibleForTesting
  static class KryoToEntityConverter {
    /**
     * Deserializes a Entity attributes from the Kryo input.
     *
     * @param input the input to deserialize the Entity attributes from.
     * @return the Entity attributes.
     */
    @VisibleForTesting
    Map<String, Entity> read(Input input) {
      return readMap(input);
    }

    private Map<String, Entity> readMap(Input input) {
      final ImmutableMap.Builder<String, Entity> builder = ImmutableMap.builder();
      final int size = input.readInt();
      for (int i = 0; i < size; ++i) {
        final String name = input.readString();
        builder.put(name, readEntity(input));
      }

      return builder.build();
    }

    private List<Entity> readList(Input input) {
      final ImmutableList.Builder<Entity> builder = ImmutableList.builder();
      final int size = input.readInt();
      for (int i = 0; i < size; ++i) {
        builder.add(readEntity(input));
      }

      return builder.build();
    }

    private Entity readEntity(Input input) {
      final Entity.EntityType type = readType(input);
      switch (type) {
        case MAP:
          return Entity.ofMap(readMap(input));
        case LIST:
          return Entity.ofList(readList(input));
        case NUMBER:
          return Entity.ofNumber(input.readLong());
        case STRING:
          return Entity.ofString(input.readString());
        case BINARY:
          final int numBytes = input.readInt();
          final Entity binary = Entity.ofBinary(UnsafeByteOperations.unsafeWrap(input.getBuffer(), input.position(), numBytes));
          input.setPosition(input.position() + numBytes);
          return binary;
        case BOOLEAN:
          return Entity.ofBoolean(input.readBoolean());
        default:
          throw new UnsupportedOperationException(String.format("Unsupported field type: %s", type.name()));
      }
    }

    Entity.EntityType readType(Input input) {
      final int type = input.readShort();
      if (0 > type || type >= Entity.EntityType.values().length) {
        throw new IllegalArgumentException(
            String.format("Corrupt Entity serialization, read unsupported EntityType: %d", type));
      }
      return Entity.EntityType.values()[type];
    }
  }

  private static class ValueTypeSerializer<T> extends Serializer<T> {
    private final SimpleSchema<T> schema;

    ValueTypeSerializer(SimpleSchema<T> schema) {
      this.schema = schema;
    }

    @Override
    public void write(Kryo kryo, Output output, T object) {
      ENTITY_TO_KRYO_CONVERTER.write(output, schema.itemToMap(object, true));
    }

    @Override
    public T read(Kryo kryo, Input input, Class<? extends T> clazz) {
      return schema.mapToItem(KRYO_TO_ENTITY_CONVERTER.read(input));
    }
  }

  @VisibleForTesting
  static final EntityToKryoConverter ENTITY_TO_KRYO_CONVERTER = new EntityToKryoConverter();
  @VisibleForTesting
  static final KryoToEntityConverter KRYO_TO_ENTITY_CONVERTER = new KryoToEntityConverter();

  ValueKryo() {
    super();
    for (ValueType type : ValueType.values()) {
      this.register(type.getObjectClass(), new ValueTypeSerializer<>(type.getSchema()));
    }
  }

  @Override
  public Registration getRegistration(Class clazz) {
    if (InternalRef.class.isAssignableFrom(clazz)) {
      // Override retrieval of the registration to handle internal subclasses of InternalRef.
      return super.getRegistration(InternalRef.class);
    }
    return super.getRegistration(clazz);
  }
}
