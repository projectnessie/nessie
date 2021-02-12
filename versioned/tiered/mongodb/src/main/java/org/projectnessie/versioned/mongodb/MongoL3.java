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
package org.projectnessie.versioned.mongodb;

import java.util.List;
import java.util.stream.Stream;

import org.bson.BsonWriter;
import org.bson.Document;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.store.Id;
import org.projectnessie.versioned.store.KeyDelta;
import org.projectnessie.versioned.tiered.L3;

final class MongoL3 extends MongoBaseValue<L3> implements L3 {

  static final String TREE = "tree";
  static final String TREE_KEY = "key";
  static final String TREE_ID = "id";

  static void produce(Document document, L3 v) {
    produceBase(document, v)
        .keyDelta(deserializeKeyDeltas(document));
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

  static Stream<KeyDelta> deserializeKeyDeltas(Document entity) {
    List<Document> deltas = (List<Document>) entity.get(TREE);
    return deltas.stream()
        .map(MongoL3::deserializeKeyDelta);
  }

  private static KeyDelta deserializeKeyDelta(Document d) {
    Key key = MongoSerDe.deserializeKey((List<String>) d.get(TREE_KEY));
    Id id = MongoSerDe.deserializeId(d, TREE_ID);
    return KeyDelta.of(key, id);
  }

  @Override
  BsonWriter build() {
    checkPresent(TREE, "keyDelta");

    return super.build();
  }
}
