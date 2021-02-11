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

import static com.dremio.nessie.versioned.store.mongodb.MongoSerDe.deserializeId;
import static com.dremio.nessie.versioned.store.mongodb.MongoSerDe.deserializeIds;
import static com.dremio.nessie.versioned.store.mongodb.MongoSerDe.deserializeKeyMutations;

import java.util.stream.Stream;

import org.bson.BsonWriter;
import org.bson.Document;

import com.dremio.nessie.tiered.builder.L1;
import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.store.Id;
import com.google.common.primitives.Ints;

final class MongoL1 extends MongoBaseValue<L1> implements L1 {

  static final String MUTATIONS = "mutations";
  static final String FRAGMENTS = "fragments";
  static final String IS_CHECKPOINT = "chk";
  static final String ORIGIN = "origin";
  static final String DISTANCE = "dist";
  static final String PARENTS = "parents";
  static final String TREE = "tree";
  static final String METADATA = "metadata";
  static final String KEY_LIST = "keys";

  static void produce(Document document, L1 v) {
    v = produceBase(document, v)
      .commitMetadataId(deserializeId(document, METADATA))
      .ancestors(deserializeIds(document, PARENTS))
      .children(deserializeIds(document, TREE))
      .keyMutations(deserializeKeyMutations(document, MUTATIONS));

    Document keyList = (Document) document.get(KEY_LIST);
    boolean checkpoint = keyList.getBoolean(IS_CHECKPOINT);
    if (checkpoint) {
      v.completeKeyList(deserializeIds(keyList, FRAGMENTS));
    } else {
      Id checkpointId = deserializeId(keyList, ORIGIN);
      int distanceFromCheckpoint = Ints.checkedCast(keyList.getLong(DISTANCE));
      v.incrementalKeyList(checkpointId, distanceFromCheckpoint);
    }
  }

  MongoL1(BsonWriter bsonWriter) {
    super(bsonWriter);
  }

  @Override
  public L1 commitMetadataId(Id id) {
    serializeId(METADATA, id);
    return this;
  }

  @Override
  public L1 ancestors(Stream<Id> ids) {
    serializeIds(PARENTS, ids);
    return this;
  }

  @Override
  public L1 children(Stream<Id> ids) {
    serializeIds(TREE, ids);
    return this;
  }

  @Override
  public L1 keyMutations(Stream<Key.Mutation> keyMutations) {
    serializeArray(MUTATIONS, keyMutations, MongoSerDe::serializeKeyMutation);
    return this;
  }

  @Override
  public L1 incrementalKeyList(Id checkpointId, int distanceFromCheckpoint) {
    addProperty(KEY_LIST);
    bsonWriter.writeName(KEY_LIST);
    bsonWriter.writeStartDocument();

    bsonWriter.writeBoolean(IS_CHECKPOINT, false);
    serializeId(ORIGIN, checkpointId);
    serializeLong(DISTANCE, distanceFromCheckpoint);

    bsonWriter.writeEndDocument();

    return this;
  }

  @Override
  public L1 completeKeyList(Stream<Id> fragmentIds) {
    addProperty(KEY_LIST);
    bsonWriter.writeName(KEY_LIST);
    bsonWriter.writeStartDocument();

    bsonWriter.writeBoolean(IS_CHECKPOINT, true);
    serializeIds(FRAGMENTS, fragmentIds);

    bsonWriter.writeEndDocument();

    return this;
  }

  @Override
  BsonWriter build() {
    checkPresent(METADATA, "metadata");
    checkPresent(TREE, "children");
    checkPresent(PARENTS, "ancestors");

    return super.build();
  }
}
