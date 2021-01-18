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

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import org.bson.BsonReader;
import org.bson.BsonWriter;

import com.dremio.nessie.tiered.builder.L3;
import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.KeyDelta;

final class MongoL3 extends MongoBaseValue<L3> implements L3 {

  static final String TREE = "tree";
  static final String TREE_KEY = "key";
  static final String TREE_ID = "id";

  static final Map<String, BiConsumer<L3, BsonReader>> PROPERTY_PRODUCERS = new HashMap<>();

  static {
    PROPERTY_PRODUCERS.put(ID, (c, r) -> c.id(MongoSerDe.deserializeId(r)));
    PROPERTY_PRODUCERS.put(TREE, (c, r) -> c.keyDelta(deserializeKeyDeltas(r)));
  }

  MongoL3(BsonWriter bsonWriter) {
    super(bsonWriter);
  }

  @Override
  public L3 keyDelta(Stream<KeyDelta> keyDelta) {
    serializeArray(TREE, keyDelta, MongoL3::serializeKeyDelta);
    return this;
  }

  private static void serializeKeyDelta(BsonWriter writer, KeyDelta keyDelta) {
    writer.writeStartDocument();

    MongoSerDe.serializeKey(writer, TREE_KEY, keyDelta.getKey());
    writer.writeBinaryData(TREE_ID, MongoSerDe.serializeId(keyDelta.getId()));

    writer.writeEndDocument();
  }

  static Stream<KeyDelta> deserializeKeyDeltas(BsonReader entity) {
    return MongoSerDe.deserializeArray(entity, r -> {
      r.readStartDocument();
      Key key = MongoSerDe.deserializeKey(entity);
      Id id = MongoSerDe.deserializeId(entity);
      r.readEndDocument();
      return KeyDelta.of(key, id);
    }).stream();
  }

  @Override
  BsonWriter build() {
    checkPresent(TREE, "keyDelta");

    return super.build();
  }
}
