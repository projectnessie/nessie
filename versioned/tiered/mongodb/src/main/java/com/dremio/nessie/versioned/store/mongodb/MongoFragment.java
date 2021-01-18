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
import java.util.function.BiFunction;
import java.util.stream.Stream;

import org.bson.BsonReader;
import org.bson.BsonWriter;

import com.dremio.nessie.tiered.builder.Fragment;
import com.dremio.nessie.versioned.Key;

final class MongoFragment extends MongoBaseValue<Fragment> implements Fragment {

  static final String KEY_LIST = "keys";

  static final Map<String, BiFunction<Fragment, BsonReader, Fragment>> PROPERTY_PRODUCERS = new HashMap<>();

  static {
    PROPERTY_PRODUCERS.put(ID, (c, r) -> c.id(MongoSerDe.deserializeId(r)));
    PROPERTY_PRODUCERS.put(KEY_LIST, (c, r) -> c.keys(MongoSerDe.deserializeKeys(r)));
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
