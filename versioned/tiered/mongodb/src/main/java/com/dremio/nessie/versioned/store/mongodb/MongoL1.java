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
import org.bson.BsonType;
import org.bson.BsonWriter;

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

  static final Map<String, BiFunction<L1, BsonReader, L1>> PROPERTY_PRODUCERS = new HashMap<>();

  static {
    PROPERTY_PRODUCERS.put(ID, (c, r) -> c.id(MongoSerDe.deserializeId(r)));
    PROPERTY_PRODUCERS.put(PARENTS, (c, r) -> c.ancestors(MongoSerDe.deserializeIds(r)));
    PROPERTY_PRODUCERS.put(TREE, (c, r) -> c.children(MongoSerDe.deserializeIds(r)));
    PROPERTY_PRODUCERS.put(METADATA, (c, r) -> c.commitMetadataId(MongoSerDe.deserializeId(r)));
    PROPERTY_PRODUCERS.put(KEY_LIST, MongoL1::deserializeKeyList);
    PROPERTY_PRODUCERS.put(MUTATIONS, MongoL1::deserializeKeyMutations);
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
    bsonWriter.writeInt64(DISTANCE, distanceFromCheckpoint);

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

  private static L1 deserializeKeyList(L1 consumer, BsonReader reader) {
    reader.readStartDocument();

    boolean chk = false;
    Id checkpointId = null;
    int distance = 0;
    Stream<Id> fragments = null;

    while (BsonType.END_OF_DOCUMENT != reader.readBsonType()) {
      String field = reader.readName();
      switch (field) {
        case IS_CHECKPOINT:
          chk = reader.readBoolean();
          break;
        case FRAGMENTS:
          fragments = MongoSerDe.deserializeIds(reader);
          break;
        case ORIGIN:
          checkpointId = MongoSerDe.deserializeId(reader);
          break;
        case DISTANCE:
          distance = Ints.checkedCast(reader.readInt64());
          break;
        default:
          throw new IllegalArgumentException(String.format("Unsupported field '%s' for BranchCommit", field));
      }
    }

    if (chk) {
      // complete key list
      consumer = consumer.completeKeyList(fragments);
    } else {
      // incremental key list
      consumer = consumer.incrementalKeyList(checkpointId, distance);
    }

    reader.readEndDocument();

    return consumer;
  }

  private static L1 deserializeKeyMutations(L1 consumer, BsonReader bsonReader) {
    return consumer.keyMutations(MongoSerDe.deserializeKeyMutations(bsonReader).stream());
  }

  @Override
  BsonWriter build() {
    checkPresent(METADATA, "metadata");
    checkPresent(TREE, "children");
    checkPresent(PARENTS, "ancestors");

    return super.build();
  }
}
