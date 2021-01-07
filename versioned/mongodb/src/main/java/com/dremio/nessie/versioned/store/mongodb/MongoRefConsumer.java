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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;

import com.dremio.nessie.tiered.builder.RefConsumer;
import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.store.Id;
import com.google.common.primitives.Ints;

final class MongoRefConsumer extends MongoConsumer<RefConsumer> implements RefConsumer {

  static final String TYPE = "type";
  static final String NAME = "name";
  static final String COMMIT = "commit";
  static final String COMMITS = "commits";
  static final String DELTAS = "deltas";
  static final String PARENT = "parent";
  static final String POSITION = "position";
  static final String NEW_ID = "new";
  static final String OLD_ID = "old";
  static final String REF_TYPE_BRANCH = "b";
  static final String REF_TYPE_TAG = "t";
  static final String TREE = "tree";
  static final String METADATA = "metadata";
  static final String KEY_LIST = "keys";

  static final Map<String, BiConsumer<RefConsumer, BsonReader>> PROPERTY_PRODUCERS = new HashMap<>();

  static {
    PROPERTY_PRODUCERS.put(ID, (c, r) -> c.id(MongoSerDe.deserializeId(r)));
    PROPERTY_PRODUCERS.put(TYPE, (c, r) -> c.type(deserializeType(r)));
    PROPERTY_PRODUCERS.put(NAME, (c, r) -> c.name(r.readString()));
    PROPERTY_PRODUCERS.put(COMMIT, (c, r) -> c.commit(MongoSerDe.deserializeId(r)));
    PROPERTY_PRODUCERS.put(METADATA, (c, r) -> c.metadata(MongoSerDe.deserializeId(r)));
    PROPERTY_PRODUCERS.put(TREE, (c, r) -> c.children(MongoSerDe.deserializeIds(r)));
    PROPERTY_PRODUCERS.put(COMMITS, (c, r) -> c.commits(deserializeBranchCommits(r)));
  }

  private RefType refType;

  MongoRefConsumer(BsonWriter bsonWriter) {
    super(bsonWriter);
  }

  @Override
  public RefConsumer type(RefType refType) {
    this.refType = refType;
    String t;
    switch (refType) {
      case TAG:
        t = REF_TYPE_TAG;
        break;
      case BRANCH:
        t = REF_TYPE_BRANCH;
        break;
      default:
        throw new IllegalArgumentException("Unknown ref-type " + refType);
    }
    serializeString(TYPE, t);
    return this;
  }

  private static RefType deserializeType(BsonReader r) {
    String t = r.readString();
    switch (t) {
      case REF_TYPE_TAG:
        return RefType.TAG;
      case REF_TYPE_BRANCH:
        return RefType.BRANCH;
      default:
        throw new IllegalArgumentException("Unknown ref-type " + t);
    }
  }

  @Override
  public RefConsumer name(String name) {
    serializeString(NAME, name);
    return this;
  }

  @Override
  public RefConsumer commit(Id commit) {
    serializeId(COMMIT, commit);
    return this;
  }

  @Override
  public RefConsumer metadata(Id metadata) {
    serializeId(METADATA, metadata);
    return this;
  }

  @Override
  public RefConsumer children(Stream<Id> children) {
    serializeIds(TREE, children);
    return this;
  }

  @Override
  public RefConsumer commits(Stream<BranchCommit> commits) {
    serializeArray(COMMITS, commits, MongoRefConsumer::serializeBranchCommit);
    return this;
  }

  @Override
  BsonWriter build() {
    checkPresent(NAME, "name");
    checkPresent(TYPE, "type");

    if (refType == RefType.TAG) {
      // tag
      checkPresent(COMMIT, "commit");
      checkNotPresent(COMMITS, "commits");
      checkNotPresent(TREE, "tree");
      checkNotPresent(METADATA, "metadata");
    } else {
      // branch
      checkNotPresent(COMMIT, "commit");
      checkPresent(COMMITS, "commits");
      checkPresent(TREE, "tree");
      checkPresent(METADATA, "metadata");
    }

    return super.build();
  }

  private static void serializeBranchCommit(BsonWriter writer, BranchCommit c) {
    writer.writeStartDocument();
    MongoSerDe.serializeId(writer, ID, c.getId());
    MongoSerDe.serializeId(writer, COMMIT, c.getCommit());

    if (c.isSaved()) {
      MongoSerDe.serializeId(writer, PARENT, c.getParent());
    } else {
      MongoSerDe.serializeArray(writer, DELTAS, c.getDeltas().stream(), MongoRefConsumer::serializeDelta);

      MongoSerDe.serializeArray(writer, KEY_LIST, c.getKeyMutations().stream(), MongoSerDe::serializeKeyMutation);
    }

    writer.writeEndDocument();
  }

  private static void serializeDelta(BsonWriter writer, BranchUnsavedDelta d) {
    writer.writeStartDocument();
    writer.writeInt64(POSITION, d.getPosition());
    MongoSerDe.serializeId(writer, OLD_ID, d.getOldId());
    MongoSerDe.serializeId(writer, NEW_ID, d.getNewId());
    writer.writeEndDocument();
  }

  private static Stream<BranchCommit> deserializeBranchCommits(BsonReader reader) {
    return MongoSerDe.deserializeArray(reader, MongoRefConsumer::deserializeBranchCommit).stream();
  }

  private static BranchCommit deserializeBranchCommit(BsonReader reader) {
    reader.readStartDocument();

    Id id = null;
    Id commit = null;
    Id parent = null;
    List<BranchUnsavedDelta> deltas = null;
    List<Key.Mutation> keyMutations = new ArrayList<>();

    while (BsonType.END_OF_DOCUMENT != reader.readBsonType()) {
      String field = reader.readName();
      switch (field) {
        case ID:
          id = MongoSerDe.deserializeId(reader);
          break;
        case PARENT:
          parent = MongoSerDe.deserializeId(reader);
          break;
        case COMMIT:
          commit = MongoSerDe.deserializeId(reader);
          break;
        case DELTAS:
          deltas = MongoSerDe.deserializeArray(reader, MongoRefConsumer::deserializeUnsavedDelta);
          break;
        case KEY_LIST:
          keyMutations = MongoSerDe.deserializeKeyMutations(reader);
          break;
        default:
          throw new IllegalArgumentException(String.format("Unsupported field '%s' for BranchCommit", field));
      }
    }

    reader.readEndDocument();

    if (deltas == null) {
      return new BranchCommit(
          id,
          commit,
          parent);
    }

    return new BranchCommit(
        id,
        commit,
        deltas,
        keyMutations
    );
  }

  private static BranchUnsavedDelta deserializeUnsavedDelta(BsonReader reader) {
    reader.readStartDocument();

    int position = 0;
    Id oldId = null;
    Id newId = null;

    while (BsonType.END_OF_DOCUMENT != reader.readBsonType()) {
      String field = reader.readName();
      switch (field) {
        case POSITION:
          position = Ints.saturatedCast(reader.readInt64());
          break;
        case OLD_ID:
          oldId = MongoSerDe.deserializeId(reader);
          break;
        case NEW_ID:
          newId = MongoSerDe.deserializeId(reader);
          break;
        default:
          throw new IllegalArgumentException(String.format("Unsupported field '%s' for BranchCommit", field));
      }
    }

    reader.readEndDocument();

    return new BranchUnsavedDelta(position, oldId, newId);
  }
}
