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

import static org.projectnessie.versioned.mongodb.MongoSerDe.ENTITY_TYPE;
import static org.projectnessie.versioned.mongodb.MongoSerDe.KEY;
import static org.projectnessie.versioned.mongodb.MongoSerDe.deserializeKeysWithEntityType;

import java.util.stream.Stream;

import org.bson.BsonWriter;
import org.bson.Document;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.WithEntityType;
import org.projectnessie.versioned.tiered.Fragment;

final class MongoFragment extends MongoBaseValue<Fragment> implements Fragment {

  static final String KEY_LIST = "keys";

  static void produce(Document document, Fragment v) {
    produceBase(document, v)
        .keys(deserializeKeysWithEntityType(document, KEY_LIST));
  }

  MongoFragment(BsonWriter bsonWriter) {
    super(bsonWriter);
  }

  @Override
  public Fragment keys(Stream<WithEntityType<Key>> keys) {
    addProperty(KEY_LIST);
    bsonWriter.writeStartArray(KEY_LIST);
    keys.forEach(k -> {
      bsonWriter.writeStartDocument();
      bsonWriter.writeStartArray(KEY);
      k.getValue().getElements().forEach(bsonWriter::writeString);
      bsonWriter.writeEndArray();
      bsonWriter.writeString(ENTITY_TYPE, Byte.toString(k.getEntityType()));
      bsonWriter.writeEndDocument();
    });
    bsonWriter.writeEndArray();
    return this;
  }

  @Override
  BsonWriter build() {
    checkPresent(KEY_LIST, "keys");

    return super.build();
  }
}
