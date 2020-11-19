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


import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bson.BsonBinary;
import org.bson.BsonWriter;

import com.dremio.nessie.versioned.impl.L2;
import com.dremio.nessie.versioned.store.Entity;

/**
 * This specifically converts an L2 object into BSON format.
 */
public class L2ToBsonConverter {

  private final BsonWriter writer;

  /**
   * Initializes the writing process to BSON.
   * @param writer the write into which the object will be serialized.
   */
  public L2ToBsonConverter(BsonWriter writer) {
    this.writer = writer;
  }

  public void write(L2 l2) {
    final Map<String, Entity> attributes = L2.SCHEMA.itemToMap(l2, true);
    writer.writeStartDocument();
    attributes.forEach(this::writeField);
    writer.writeEndDocument();

  }

  //TODO pull this out into a separate EntityToBsonConverter class
  /**
   * This creates a single field in BSON format that represents entity.
   * @param field the name of the field as represented in BSON
   * @param value the entity that will be serialized.
   */
  private void writeField(String field, Entity value) {
    writer.writeName(field);
    writeSingleValue(value);
  }

  //TODO pull this out into a separate EntityToBsonConverter class
    /**
     * Writes a single Entity to BSON.
     * Invokes different write methods based on the underlying {@code com.dremio.nessie.versioned.store.Entity} type.
     *
     * @param value the value to convert to BSON.
     */
  private void writeSingleValue(Entity value) {
    switch (value.getType()) {
      case MAP:
        Map<String, Entity> stringMap = value.getMap();
        writer.writeStartArray();
        for (Map.Entry<String, Entity> entry : stringMap.entrySet()) {
          writer.writeName(entry.getKey());
          writeSingleValue(entry.getValue());
        }
        writer.writeEndArray();
        break;
      case LIST:
        List<Entity> entityList = value.getList();
        writer.writeStartArray();
        for (Entity listValue : entityList) {
          writeSingleValue(listValue);
        }
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
        Set<String> stringSet = value.getStringSet();
        writer.writeStartArray();
        stringSet.forEach((s) -> {writer.writeString(s);});
        writer.writeEndArray();
      default:
        throw new IllegalArgumentException(String.format("Unsupported field type: %s", value.getType().name()));
    }
  }
}
