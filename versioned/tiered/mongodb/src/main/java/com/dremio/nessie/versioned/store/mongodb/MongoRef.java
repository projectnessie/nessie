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
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;

import com.dremio.nessie.tiered.builder.Ref;
import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.store.Id;
import com.google.common.primitives.Ints;

final class MongoRef extends MongoBaseValue<Ref> implements Ref, Ref.Tag, Ref.Branch {

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

  static final Map<String, BiFunction<Ref, BsonReader, Ref>> PROPERTY_PRODUCERS = new HashMap<>();

  static {
    PROPERTY_PRODUCERS.put(ID, (c, r) -> c.id(MongoSerDe.deserializeId(r)));
    PROPERTY_PRODUCERS.put(DT, (c, r) -> c.dt(r.readInt64()));
    PROPERTY_PRODUCERS.put(TYPE, MongoRef::handleType);
    PROPERTY_PRODUCERS.put(NAME, (c, r) -> c.name(r.readString()));
    PROPERTY_PRODUCERS.put(COMMIT, (c, r) -> ((Tag) c).commit(MongoSerDe.deserializeId(r)));
    PROPERTY_PRODUCERS.put(METADATA, (c, r) -> ((Branch) c).metadata(MongoSerDe.deserializeId(r)));
    PROPERTY_PRODUCERS.put(TREE, (c, r) -> ((Branch) c).children(MongoSerDe.deserializeIds(r)));
    PROPERTY_PRODUCERS.put(COMMITS, (c, r) -> ((Branch) c).commits(bc -> deserializeBranchCommits(bc, r)));
  }

  private boolean tag;
  private boolean branch;

  MongoRef(BsonWriter bsonWriter) {
    super(bsonWriter);
  }

  @Override
  public Tag tag() {
    if (branch) {
      throw new IllegalStateException("branch() has already been called");
    }
    tag = true;
    serializeString(TYPE, REF_TYPE_TAG);
    return this;
  }

  @Override
  public Branch branch() {
    if (tag) {
      throw new IllegalStateException("tag() has already been called");
    }
    branch = true;
    serializeString(TYPE, REF_TYPE_BRANCH);
    return this;
  }

  static Ref handleType(Ref ref, BsonReader r) {
    String t = r.readString();
    switch (t) {
      case REF_TYPE_TAG:
        return ref.tag();
      case REF_TYPE_BRANCH:
        return ref.branch();
      default:
        throw new IllegalArgumentException("Unknown ref-type " + t);
    }
  }

  @Override
  public Ref name(String name) {
    serializeString(NAME, name);
    return this;
  }

  @Override
  public Tag commit(Id commit) {
    serializeId(COMMIT, commit);
    return this;
  }

  @Override
  public Branch metadata(Id metadata) {
    serializeId(METADATA, metadata);
    return this;
  }

  @Override
  public Branch children(Stream<Id> children) {
    serializeIds(TREE, children);
    return this;
  }

  @Override
  public Branch commits(Consumer<BranchCommit> commits) {
    addProperty(COMMITS);
    bsonWriter.writeStartArray(COMMITS);
    commits.accept(new MongoBranchCommit());
    bsonWriter.writeEndArray();
    return this;
  }

  private class MongoBranchCommit implements BranchCommit, SavedCommit, UnsavedCommitDelta, UnsavedCommitMutations {

    int state;

    @Override
    public BranchCommit id(Id id) {
      maybeStart();
      assertState(1);
      MongoSerDe.serializeId(bsonWriter, ID, id);
      return this;
    }

    @Override
    public BranchCommit commit(Id commit) {
      maybeStart();
      assertState(1);
      MongoSerDe.serializeId(bsonWriter, COMMIT, commit);
      return this;
    }

    @Override
    public SavedCommit saved() {
      return this;
    }

    @Override
    public UnsavedCommitDelta unsaved() {
      return this;
    }

    @Override
    public SavedCommit parent(Id parent) {
      maybeStart();
      assertState(1);
      MongoSerDe.serializeId(bsonWriter, PARENT, parent);
      return this;
    }

    @Override
    public UnsavedCommitDelta delta(int position, Id oldId, Id newId) {
      maybeStart();
      if (state == 1) {
        state = 2;
        bsonWriter.writeStartArray(DELTAS);
      }
      assertState(2);
      serializeDelta(bsonWriter, position, oldId, newId);
      return this;
    }

    @Override
    public UnsavedCommitMutations mutations() {
      return this;
    }

    @Override
    public UnsavedCommitMutations keyMutation(Key.Mutation keyMutation) {
      maybeStart();
      if (state == 2) {
        state = 1;
        bsonWriter.writeEndArray();
      }
      if (state == 1) {
        state = 3;
        bsonWriter.writeStartArray(KEY_LIST);
      }
      assertState(3);
      MongoSerDe.serializeKeyMutation(bsonWriter, keyMutation);
      return this;
    }

    private void maybeStart() {
      if (state == 0) {
        bsonWriter.writeStartDocument();
        state = 1;
      }
    }

    private void assertState(int expected) {
      if (state != expected) {
        throw new IllegalStateException(
            "Wrong order or consumer method invocations (" + expected + " != " + state
                + ". See Javadocs.");
      }
    }

    @Override
    public BranchCommit done() {
      if (state == 3 || state == 2) {
        bsonWriter.writeEndArray();
        state = 1;
      }
      if (state == 1) {
        bsonWriter.writeEndDocument();
        state = 0;
      }
      return this;
    }
  }

  @Override
  BsonWriter build() {
    checkPresent(NAME, "name");
    checkPresent(TYPE, "type");

    if (tag) {
      checkPresent(COMMIT, "commit");
      checkNotPresent(COMMITS, "commits");
      checkNotPresent(TREE, "tree");
      checkNotPresent(METADATA, "metadata");
    } else if (branch) {
      checkNotPresent(COMMIT, "commit");
      checkPresent(COMMITS, "commits");
      checkPresent(TREE, "tree");
      checkPresent(METADATA, "metadata");
    } else {
      throw new IllegalStateException("Neither tag() nor branch() has been called");
    }

    return super.build();
  }

  private void serializeDelta(BsonWriter writer, int position, Id oldId, Id newId) {
    writer.writeStartDocument();
    serializeLong(POSITION, position);
    MongoSerDe.serializeId(writer, OLD_ID, oldId);
    MongoSerDe.serializeId(writer, NEW_ID, newId);
    writer.writeEndDocument();
  }

  private static void deserializeBranchCommits(BranchCommit consumer, BsonReader reader) {
    reader.readStartArray();
    while (BsonType.END_OF_DOCUMENT != reader.readBsonType()) {
      deserializeBranchCommit(consumer, reader);
    }
    reader.readEndArray();
  }

  private static void deserializeBranchCommit(BranchCommit consumer, BsonReader reader) {
    reader.readStartDocument();

    SavedCommit saved = null;
    UnsavedCommitDelta deltas = null;
    UnsavedCommitMutations mutations = null;

    while (BsonType.END_OF_DOCUMENT != reader.readBsonType()) {
      String field = reader.readName();
      switch (field) {
        case ID:
          consumer.id(MongoSerDe.deserializeId(reader));
          break;
        case COMMIT:
          consumer.commit(MongoSerDe.deserializeId(reader));
          break;
        case PARENT:
          saved = consumer.saved();
          saved.parent(MongoSerDe.deserializeId(reader));
          break;
        case DELTAS:
          deltas = consumer.unsaved();
          UnsavedCommitDelta deltasFinal = deltas;
          MongoSerDe.deserializeArray(reader, r -> {
            deserializeUnsavedDelta(deltasFinal, r);
            return null;
          });
          break;
        case KEY_LIST:
          if (deltas == null) {
            deltas = consumer.unsaved();
          }
          mutations = deltas.mutations();
          MongoSerDe.deserializeKeyMutations(reader).forEach(mutations::keyMutation);
          break;
        default:
          throw new IllegalArgumentException(String.format("Unsupported field '%s' for BranchCommit", field));
      }
    }

    if (saved != null) {
      saved.done();
    } else {
      if (deltas == null) {
        deltas = consumer.unsaved();
      }
      if (mutations == null) {
        mutations = deltas.mutations();
      }
      mutations.done();
    }

    reader.readEndDocument();
  }

  private static void deserializeUnsavedDelta(UnsavedCommitDelta consumer, BsonReader reader) {
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

    consumer.delta(position, oldId, newId);
  }
}
