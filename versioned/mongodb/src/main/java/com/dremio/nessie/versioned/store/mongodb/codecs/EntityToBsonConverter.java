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

import org.bson.BsonBinary;
import org.bson.BsonWriter;

import com.dremio.nessie.versioned.store.Entity;

/**
 * This specifically converts an L2 object into BSON format.
 */
public class EntityToBsonConverter {
  /**
   * This creates a single field in BSON format that represents entity.
   * @param field the name of the field as represented in BSON
   * @param value the entity that will be serialized.
   */
  public static void writeField(BsonWriter writer, String field, Entity value) {
    writer.writeName(field);
    writeSingleValue(writer, value);
  }

  /**
   * Writes a single Entity to BSON.
   * Invokes different write methods based on the underlying {@code com.dremio.nessie.versioned.store.Entity} type.
   *
   * @param value the value to convert to BSON.
   */
  public static void writeSingleValue(BsonWriter writer, Entity value) {
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
        value.getList().forEach(v -> writeSingleValue(writer, v));
        writer.writeEndArray();
        break;
      case NUMBER:
        writer.writeString(value.getNumber());
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
      case STRING_SET:
        writer.writeStartArray();
        value.getStringSet().forEach(writer::writeString);
        writer.writeEndArray();
        break;
      default:
        throw new IllegalArgumentException(String.format("Unsupported field type: %s", value.getType().name()));
    }
  }
}
