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

import java.util.stream.Stream;

import org.bson.BsonWriter;
import org.bson.Document;
import org.projectnessie.versioned.store.Id;
import org.projectnessie.versioned.tiered.L2;

final class MongoL2 extends MongoBaseValue<L2> implements L2 {

  static final String TREE = "tree";

  static void produce(Document document, L2 v) {
    produceBase(document, v)
        .children(MongoSerDe.deserializeIds(document, TREE));
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
