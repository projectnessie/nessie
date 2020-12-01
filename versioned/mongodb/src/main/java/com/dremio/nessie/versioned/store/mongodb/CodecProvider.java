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
package com.dremio.nessie.versioned.store.mongodb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.bson.BsonBinary;
import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;

import com.dremio.nessie.versioned.store.Entity;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.SimpleSchema;
import com.dremio.nessie.versioned.store.ValueType;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.UnsafeByteOperations;

/**
 * Codecs provide the heart of the SerDe process to/from BSON format.
 * The Codecs are inserted into a CodecRegistry. However they may require the CodecRegistry to do their job.
 * This apparent two way interdependency is resolved by using a CodecProvider.
 * The CodecProvider is a factory for Codecs.
 */
public class CodecProvider implements org.bson.codecs.configuration.CodecProvider {
  static class SerializationException extends RuntimeException {
    public SerializationException(String message, Throwable cause) {
      super(message, cause);
    }

    public SerializationException(String message) {
      super(message);
    }
  }

  static class EntityToBsonConverter {
    /**
     * Write the specified Entity attributes to BSON.
     * @param writer the BSON writer to write to.
     * @param attributes the Entity attributes to serialize.
     */
    void write(BsonWriter writer, Map<String, Entity> attributes) {
      writer.writeStartDocument();
      attributes.forEach((k, v) -> writeField(writer, k, v));
      writer.writeEndDocument();
    }

    /**
     * This creates a single field in BSON format that represents entity.
     * @param writer the BSON writer to write to.
     * @param field the name of the field as represented in BSON
     * @param value the entity that will be serialized.
     */
    private void writeField(BsonWriter writer, String field, Entity value) {
      writer.writeName(field);
      writeSingleValue(writer, value);
    }

    /**
     * Writes a single Entity to BSON.
     * Invokes different write methods based on the underlying {@code com.dremio.nessie.versioned.store.Entity} type.
     *
     * @param writer the BSON writer to write to.
     * @param value the value to convert to BSON.
     */
    private void writeSingleValue(BsonWriter writer, Entity value) {
      switch (value.getType()) {
        case MAP:
          writer.writeStartDocument();
          value.getMap().forEach((k, v) -> writeField(writer, k, v));
          writer.writeEndDocument();
          break;
        case LIST:
          writer.writeStartArray();
          value.getList().forEach(v -> writeSingleValue(writer, v));
          writer.writeEndArray();
          break;
        case NUMBER:
          writer.writeInt64(value.getNumber());
          break;
        case STRING:
          writer.writeString(value.getString());
          break;
        case BINARY:
          writer.writeBinaryData(new BsonBinary(value.getBinary().toByteArray()));
          break;
        case BOOLEAN:
          writer.writeBoolean(value.getBoolean());
          break;
        default:
          throw new SerializationException(String.format("Unsupported field type: %s", value.getType().name()));
      }
    }
  }

  static class BsonToEntityConverter {
    private static final String MONGO_ID_NAME = "_id";

    /**
     * Deserializes a Entity attributes from the BSON stream.
     *
     * @param reader the reader to deserialize the Entity attributes from.
     * @return the Entity attributes.
     */
    Map<String, Entity> read(BsonReader reader) {
      final BsonType type = reader.getCurrentBsonType();
      if (BsonType.DOCUMENT != type) {
        throw new SerializationException(
          String.format("BSON serialized data must be a document at the root, type is %s",type));
      }

      return readDocument(reader);
    }

    /**
     * Read a BSON document and deserialize to associated Entity types.
     * @param reader the reader to deserialize the Entities from.
     * @return The deserialized collection of entities with their names.
     */
    private Map<String, Entity> readDocument(BsonReader reader) {
      reader.readStartDocument();
      final Map<String, Entity> attributes = new HashMap<>();

      while (BsonType.END_OF_DOCUMENT != reader.readBsonType()) {
        final String name = reader.readName();
        if (name.equals(MONGO_ID_NAME)) {
          reader.skipValue();
          continue;
        }

        switch (reader.getCurrentBsonType()) {
          case DOCUMENT:
            attributes.put(name, Entity.ofMap(readDocument(reader)));
            break;
          case ARRAY:
            attributes.put(name, readArray(reader));
            break;
          case BOOLEAN:
            attributes.put(name, Entity.ofBoolean(reader.readBoolean()));
            break;
          case STRING:
            attributes.put(name, Entity.ofString(reader.readString()));
            break;
          case INT32:
            attributes.put(name, Entity.ofNumber(reader.readInt32()));
            break;
          case INT64:
            attributes.put(name, Entity.ofNumber(reader.readInt64()));
            break;
          case BINARY:
            attributes.put(name, Entity.ofBinary(UnsafeByteOperations.unsafeWrap(reader.readBinaryData().getData())));
            break;
          default:
            throw new SerializationException(
              String.format("Unsupported BSON type: %s", reader.getCurrentBsonType().name()));
        }
      }

      reader.readEndDocument();
      return attributes;
    }

    /**
     * Read a BSON array and deserialize to associated Entity types.
     * @param reader the reader to deserialize the Entities from.
     * @return The deserialized list-type Entity, either a LIST or STRING_SET.
     */
    private Entity readArray(BsonReader reader) {
      reader.readStartArray();
      final List<Entity> entities = new ArrayList<>();

      while (BsonType.END_OF_DOCUMENT != reader.readBsonType()) {
        switch (reader.getCurrentBsonType()) {
          case DOCUMENT:
            entities.add(Entity.ofMap(readDocument(reader)));
            break;
          case ARRAY:
            entities.add(readArray(reader));
            break;
          case BOOLEAN:
            entities.add(Entity.ofBoolean(reader.readBoolean()));
            break;
          case STRING:
            entities.add(Entity.ofString(reader.readString()));
            break;
          case INT32:
            entities.add(Entity.ofNumber(reader.readInt32()));
            break;
          case INT64:
            entities.add(Entity.ofNumber(reader.readInt64()));
            break;
          case BINARY:
            entities.add(Entity.ofBinary(UnsafeByteOperations.unsafeWrap(reader.readBinaryData().getData())));
            break;
          default:
            throw new SerializationException(
              String.format("Unsupported BSON type: %s", reader.getCurrentBsonType().name()));
        }
      }

      reader.readEndArray();
      return Entity.ofList(entities);
    }
  }

  /**
   * Codec responsible for the encoding and decoding of Entities to a BSON objects.
   */
  private static class EntityCodec<C, S> implements Codec<C> {
    private final Class<C> clazz;
    private final SimpleSchema<S> schema;

    /**
     * Constructor.
     * @param clazz the class type to encode/decode.
     * @param schema the schema of the class.
     */
    public EntityCodec(Class<C> clazz, SimpleSchema<S> schema) {
      this.clazz = clazz;
      this.schema = schema;
    }

    /**
     * This deserializes a BSON stream to create an L1 object.
     * @param bsonReader that provides the BSON
     * @param decoderContext not used
     * @return the object created from the BSON stream.
     */
    @Override
    @SuppressWarnings("unchecked")
    public C decode(BsonReader bsonReader, DecoderContext decoderContext) {
      return (C) schema.mapToItem(BSON_TO_ENTITY_CONVERTER.read(bsonReader));
    }

    /**
     * This serializes an object into a BSON stream. The serialization is delegated to
     * {@link EntityToBsonConverter}
     * @param bsonWriter that encodes each attribute to BSON
     * @param obj the object to encode
     * @param encoderContext not used
     */
    @Override
    @SuppressWarnings("unchecked")
    public void encode(BsonWriter bsonWriter, C obj, EncoderContext encoderContext) {
      ENTITY_TO_BSON_CONVERTER.write(bsonWriter, schema.itemToMap((S)obj, true));
    }

    /**
     * A getter of the class being encoded.
     * @return the class being encoded
     */
    @Override
    public Class<C> getEncoderClass() {
      return clazz;
    }
  }

  private static class IdCodec implements Codec<Id> {
    @Override
    public Id decode(BsonReader bsonReader, DecoderContext decoderContext) {
      return Id.of(bsonReader.readBinaryData().getData());
    }

    @Override
    public void encode(BsonWriter bsonWriter, Id id, EncoderContext encoderContext) {
      bsonWriter.writeBinaryData(new BsonBinary(id.getValue().toByteArray()));
    }

    @Override
    public Class<Id> getEncoderClass() {
      return Id.class;
    }
  }

  @VisibleForTesting
  static final EntityToBsonConverter ENTITY_TO_BSON_CONVERTER = new EntityToBsonConverter();
  @VisibleForTesting
  static final BsonToEntityConverter BSON_TO_ENTITY_CONVERTER = new BsonToEntityConverter();

  private static final Map<Class<?>, Codec<?>> CODECS = new HashMap<>();

  static {
    CODECS.putAll(Arrays.stream(ValueType.values()).collect(
        Collectors.toMap(ValueType::getObjectClass, v -> new EntityCodec<>(v.getObjectClass(), v.getSchema()))));
    // Specific case where the ID is not encoded as a document, but directly as a binary value.
    CODECS.put(Id.class, new IdCodec());
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> Codec<T> get(Class<T> clazz, CodecRegistry registry) {
    final Codec<T> codec = (Codec<T>)CODECS.get(clazz);
    if (null != codec) {
      return codec;
    }

    // In most cases, the codec for a class is directly entered, but also account for when there are subclasses for
    // a registered class and get the CODEC for that.
    for (Map.Entry<Class<?>, Codec<?>> entry : CODECS.entrySet()) {
      if (entry.getKey().isAssignableFrom(clazz)) {
        return (Codec<T>)entry.getValue();
      }
    }

    return null;
  }
}
