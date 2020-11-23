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
package com.dremio.nessie.versioned.store.mongodb.codecs;

import java.util.Map;

import org.bson.BsonBinary;
import org.bson.BsonWriter;

import com.dremio.nessie.versioned.store.Entity;
import com.dremio.nessie.versioned.store.mongodb.MongoDbConstants;

/**
 * Converter for serializing an Entity to BSON format.
 */
public class EntityToBsonConverter {
  /**
   * Write the specified Entity attributes to BSON.
   * @param writer the BSON writer to write to.
   * @param attributes the Entity attributes to serialize.
   */
  public static void write(BsonWriter writer, Map<String, Entity> attributes) {
    writer.writeStartDocument();
    attributes.forEach((k, v) -> EntityToBsonConverter.writeField(writer, k, v));
    writer.writeEndDocument();
  }

  /**
   * This creates a single field in BSON format that represents entity.
   * @param writer the BSON writer to write to.
   * @param field the name of the field as represented in BSON
   * @param value the entity that will be serialized.
   */
  private static void writeField(BsonWriter writer, String field, Entity value) {
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
  private static void writeSingleValue(BsonWriter writer, Entity value) {
    switch (value.getType()) {
      case MAP:
        writer.writeStartDocument();
        value.getMap().forEach((k, v) -> {
          writer.writeName(k);
          writeSingleValue(writer, v);
        });
        writer.writeEndDocument();
        break;
      case LIST:
        writer.writeStartArray();
        // Flag to differentiate LIST from STRING_SET.
        writer.writeBoolean(true);
        value.getList().forEach(v -> writeSingleValue(writer, v));
        writer.writeEndArray();
        break;
      case NUMBER:
        writer.writeString(MongoDbConstants.NUMBER_PREFIX + value.getNumber());
        break;
      case STRING:
        writer.writeString(MongoDbConstants.STRING_PREFIX + value.getString());
        break;
      case BINARY:
        writer.writeBinaryData(new BsonBinary(value.getBinary().toByteArray()));
        break;
      case BOOLEAN:
        writer.writeBoolean(value.getBoolean());
        break;
      case STRING_SET:
        writer.writeStartArray();
        // Flag to differentiate STRING_SET from LIST.
        writer.writeBoolean(false);
        // Omit the NUMBER_PREFIX or STRING_PREFIX as only STRING values can be in a STRING_SET.
        value.getStringSet().forEach(writer::writeString);
        writer.writeEndArray();
        break;
      default:
        throw new UnsupportedOperationException(String.format("Unsupported field type: %s", value.getType().name()));
    }
  }
}
