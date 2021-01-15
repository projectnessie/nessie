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

import java.util.HashSet;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import org.bson.BsonBinary;
import org.bson.BsonWriter;

import com.dremio.nessie.tiered.builder.BaseConsumer;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.Store;
import com.google.common.base.Preconditions;

abstract class MongoConsumer<C extends BaseConsumer<C>> implements BaseConsumer<C> {

  static final String ID = Store.KEY_NAME;

  final BsonWriter bsonWriter;

  private final Set<String> properties = new HashSet<>();

  protected MongoConsumer(BsonWriter bsonWriter) {
    // empty
    this.bsonWriter = bsonWriter;
  }

  @SuppressWarnings("unchecked")
  @Override
  public C id(Id id) {
    if (!properties.contains(ID)) {
      // MongoSerDe calls this method as the very first one during serialization to satisfiy the
      // deserialization requirement that the ID field is the first one being deserialized.
      serializeId(ID, id);
    }
    return (C) this;
  }

  BsonWriter build() {
    checkPresent(ID, "id");

    return bsonWriter;
  }

  void addProperty(String id) {
    if (!properties.add(id)) {
      throw new IllegalStateException(String.format("Property '%s' already serialized.", id));
    }
  }

  void checkPresent(String id, String name) {
    Preconditions.checkArgument(
        properties.contains(id),
        String.format("Method %s of consumer %s has not been called", name, getClass().getSimpleName()));
  }

  void checkNotPresent(String id, String name) {
    Preconditions.checkArgument(
        !properties.contains(id),
        String.format("Method %s of consumer %s must not be called", name, getClass().getSimpleName()));
  }

  void serializeId(String property, Id id) {
    addProperty(property);
    MongoSerDe.serializeId(bsonWriter, property, id);
  }

  void serializeIds(String property, Stream<Id> ids) {
    addProperty(property);
    bsonWriter.writeStartArray(property);

    ids.forEach(id -> bsonWriter.writeBinaryData(new BsonBinary(id.toBytes())));

    bsonWriter.writeEndArray();
  }

  void serializeString(String property, String str) {
    addProperty(property);
    bsonWriter.writeString(property, str);
  }

  <X> void serializeArray(String prop, Stream<X> src, BiConsumer<BsonWriter, X> inner) {
    addProperty(prop);
    MongoSerDe.serializeArray(bsonWriter, prop, src, inner);
  }

}
