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

import static com.dremio.nessie.versioned.store.mongodb.MongoSerDe.deserializeKeys;

import java.util.stream.Stream;

import org.bson.BsonWriter;
import org.bson.Document;

import com.dremio.nessie.tiered.builder.Fragment;
import com.dremio.nessie.versioned.Key;

final class MongoFragment extends MongoBaseValue<Fragment> implements Fragment {

  static final String KEY_LIST = "keys";

  static void produce(Document document, Fragment v) {
    produceBase(document, v)
        .keys(deserializeKeys(document, KEY_LIST));
  }

  MongoFragment(BsonWriter bsonWriter) {
    super(bsonWriter);
  }

  @Override
  public Fragment keys(Stream<Key> keys) {
    addProperty(KEY_LIST);
    bsonWriter.writeStartArray(KEY_LIST);
    keys.forEach(k -> {
      bsonWriter.writeStartArray();
      k.getElements().forEach(bsonWriter::writeString);
      bsonWriter.writeEndArray();
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
