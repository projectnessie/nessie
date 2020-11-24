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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bson.BsonReader;
import org.bson.BsonType;

import com.dremio.nessie.versioned.store.Entity;
import com.dremio.nessie.versioned.store.mongodb.MongoDbConstants;

/**
 * Converter for serializing a BSON object to an Entity.
 */
public class BsonToEntityConverter {
  private static final String MONGO_ID_NAME = "_id";

  /**
   * Deserializes a Entity attributes from the BSON stream.
   *
   * @param reader the reader to deserialize the Entity attributes from.
   * @return the Entity attributes.
   */
  public static Map<String, Entity> read(BsonReader reader) {
    if (BsonType.DOCUMENT != reader.getCurrentBsonType()) {
      throw new UnsupportedOperationException(
          String.format("BSON serialized data must be a document at the root, type is %s",
              reader.getCurrentBsonType().name()));
    }

    return readDocument(reader);
  }

  /**
   * Read a BSON document and deserialize to associated Entity types.
   * @param reader the reader to deserialize the Entities from.
   * @return The deserialized collection of entities with their names.
   */
  private static Map<String, Entity> readDocument(BsonReader reader) {
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
        case STRING: {
          final String value = reader.readString();
          if (value.startsWith(MongoDbConstants.STRING_PREFIX)) {
            attributes.put(name, Entity.ofString(value.substring(1)));
          } else {
            attributes.put(name, Entity.ofNumber(value.substring(1)));
          }
          break;
        }
        case BINARY:
          attributes.put(name, Entity.ofBinary(reader.readBinaryData().getData()));
          break;
        case END_OF_DOCUMENT:
          break;
        default:
          throw new UnsupportedOperationException(
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
  private static Entity readArray(BsonReader reader) {
    reader.readStartArray();
    if (reader.readBoolean()) {
      // The entity is a generic list.
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
          case STRING: {
            final String value = reader.readString();
            if (value.startsWith(MongoDbConstants.STRING_PREFIX)) {
              entities.add(Entity.ofString(value.substring(1)));
            } else {
              entities.add(Entity.ofNumber(value.substring(1)));
            }
            break;
          }
          case BINARY:
            entities.add(Entity.ofBinary(reader.readBinaryData().getData()));
            break;
          default:
            throw new UnsupportedOperationException(
              String.format("Unsupported BSON type: %s", reader.getCurrentBsonType().name()));
        }
      }

      reader.readEndArray();
      return Entity.ofList(entities);
    }

    // The entity is a string set, so simply read strings from it.
    final Set<String> entities = new HashSet<>();
    while (BsonType.END_OF_DOCUMENT != reader.readBsonType()) {
      if (BsonType.STRING != reader.getCurrentBsonType()) {
        throw new UnsupportedOperationException(
          String.format("STRING_SET not composed of STRING values, found type: %s", reader.getCurrentBsonType().name()));
      }

      entities.add(reader.readString());
    }
    reader.readEndArray();
    return Entity.ofStringSet(entities);
  }
}
