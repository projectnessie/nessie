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

import com.dremio.nessie.tiered.builder.L2;
import com.dremio.nessie.versioned.store.Id;

final class MongoL2 extends MongoBaseValue<L2> implements L2 {

  static final String TREE = "tree";

  static final Map<String, BiFunction<L2, BsonReader, L2>> PROPERTY_PRODUCERS = new HashMap<>();

  static {
    PROPERTY_PRODUCERS.put(ID, (c, r) -> c.id(MongoSerDe.deserializeId(r)));
    PROPERTY_PRODUCERS.put(DT, (c, r) -> c.dt(r.readInt64()));
    PROPERTY_PRODUCERS.put(TREE, (c, r) -> c.children(MongoSerDe.deserializeIds(r)));
  }

  MongoL2(BsonWriter bsonWriter) {
    super(bsonWriter);
  }

  @Override
  public L2 children(Stream<Id> ids) {
    serializeIds(TREE, ids);
    return this;
  }

  @Override
  BsonWriter build() {
    checkPresent(TREE, "children");

    return super.build();
  }
}
