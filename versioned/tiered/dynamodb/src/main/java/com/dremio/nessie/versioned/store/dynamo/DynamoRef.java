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
package com.dremio.nessie.versioned.store.dynamo;

import static com.dremio.nessie.versioned.store.dynamo.AttributeValueUtil.attributeValue;
import static com.dremio.nessie.versioned.store.dynamo.AttributeValueUtil.deserializeId;
import static com.dremio.nessie.versioned.store.dynamo.AttributeValueUtil.deserializeIdStream;
import static com.dremio.nessie.versioned.store.dynamo.AttributeValueUtil.deserializeInt;
import static com.dremio.nessie.versioned.store.dynamo.AttributeValueUtil.deserializeKeyMutation;
import static com.dremio.nessie.versioned.store.dynamo.AttributeValueUtil.idValue;
import static com.dremio.nessie.versioned.store.dynamo.AttributeValueUtil.list;
import static com.dremio.nessie.versioned.store.dynamo.AttributeValueUtil.map;
import static com.dremio.nessie.versioned.store.dynamo.AttributeValueUtil.number;
import static com.dremio.nessie.versioned.store.dynamo.AttributeValueUtil.serializeKeyMutation;
import static com.dremio.nessie.versioned.store.dynamo.AttributeValueUtil.string;
import static software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;

import com.dremio.nessie.tiered.builder.Ref;
import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.ValueType;
import com.google.common.base.Preconditions;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

class DynamoRef extends DynamoBaseValue<Ref> implements Ref {

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

  private enum Type {
    INIT, TAG, BRANCH
  }

  private Type type = Type.INIT;

  DynamoRef() {
    super(ValueType.REF);
  }

  @Override
  public Tag tag() {
    if (type != Type.INIT) {
      throw new IllegalStateException("branch()/tag() has already been called");
    }
    type = Type.TAG;
    addEntitySafe(TYPE, string(REF_TYPE_TAG));
    return new DynamoTag();
  }

  @Override
  public Branch branch() {
    if (type != Type.INIT) {
      throw new IllegalStateException("branch()/tag() has already been called");
    }
    type = Type.BRANCH;
    addEntitySafe(TYPE, string(REF_TYPE_BRANCH));
    return new DynamoBranch();
  }

  class DynamoTag implements Tag {
    @Override
    public Tag commit(Id commit) {
      addEntitySafe(COMMIT, idValue(commit));
      return this;
    }

    @Override
    public Ref backToRef() {
      return DynamoRef.this;
    }
  }

  class DynamoBranch implements Branch {
    @Override
    public Branch metadata(Id metadata) {
      addEntitySafe(METADATA, idValue(metadata));
      return this;
    }

    @Override
    public Branch children(Stream<Id> children) {
      addIdList(TREE, children);
      return this;
    }

    @Override
    public Branch commits(Consumer<BranchCommit> commits) {
      DynamoBranchCommit serializedCommits = new DynamoBranchCommit();
      commits.accept(serializedCommits);
      addEntitySafe(COMMITS, builder().l(serializedCommits.commitsList).build());
      return this;
    }

    @Override
    public Ref backToRef() {
      return DynamoRef.this;
    }
  }

  @Override
  public Ref name(String name) {
    return addEntitySafe(NAME, string(name));
  }

  private static class DynamoBranchCommit implements BranchCommit, SavedCommit,
      UnsavedCommitDelta, UnsavedCommitMutations {
    final Map<String, AttributeValue> builder = new HashMap<>();
    final List<AttributeValue> commitsList = new ArrayList<>();
    List<AttributeValue> deltas = null;
    List<AttributeValue> keyMutations = null;

    @Override
    public BranchCommit id(Id id) {
      builder.put(ID, idValue(id));
      return this;
    }

    @Override
    public BranchCommit commit(Id commit) {
      builder.put(COMMIT, idValue(commit));
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
      builder.put(PARENT, idValue(parent));
      return this;
    }

    @Override
    public UnsavedCommitDelta delta(int position, Id oldId, Id newId) {
      if (deltas == null) {
        deltas = new ArrayList<>();
      }
      Map<String, AttributeValue> map = new HashMap<>();
      map.put(POSITION, number(position));
      map.put(OLD_ID, idValue(oldId));
      map.put(NEW_ID, idValue(newId));
      deltas.add(map(map));
      return this;
    }

    @Override
    public UnsavedCommitMutations mutations() {
      return this;
    }

    @Override
    public UnsavedCommitMutations keyMutation(Key.Mutation keyMutation) {
      if (keyMutations == null) {
        keyMutations = new ArrayList<>();
      }
      keyMutations.add(serializeKeyMutation(keyMutation));
      return this;
    }

    @Override
    public BranchCommit done() {
      if (deltas != null) {
        builder.put(DELTAS, list(deltas.stream()));
      }
      if (keyMutations != null) {
        builder.put(KEY_LIST, list(keyMutations.stream()));
      }
      commitsList.add(map(builder));
      builder.clear();
      deltas = null;
      keyMutations = null;
      return this;
    }
  }

  @Override
  Map<String, AttributeValue> build() {
    checkPresent(NAME, "name");
    checkPresent(TYPE, "type");

    switch (type) {
      case TAG:
        checkPresent(COMMIT, "commit");
        checkNotPresent(COMMITS, "commits");
        checkNotPresent(TREE, "tree");
        checkNotPresent(METADATA, "metadata");
        break;
      case BRANCH:
        checkNotPresent(COMMIT, "commit");
        checkPresent(COMMITS, "commits");
        checkPresent(TREE, "tree");
        checkPresent(METADATA, "metadata");
        break;
      default:
        throw new IllegalStateException("Neither tag() nor branch() has been called");
    }

    return super.build();
  }

  /**
   * Deserialize a DynamoDB entity into the given consumer.
   */
  static void toConsumer(Map<String, AttributeValue> entity, Ref consumer) {
    DynamoBaseValue.toConsumer(entity, consumer);
    consumer.name(Preconditions.checkNotNull(attributeValue(entity, NAME).s()));

    String refType = Preconditions.checkNotNull(attributeValue(entity, TYPE).s());
    switch (refType) {
      case REF_TYPE_BRANCH:
        consumer.branch()
            .metadata(deserializeId(entity, METADATA))
            .children(deserializeIdStream(entity, TREE))
            .commits(cc -> deserializeCommits(entity, cc));
        break;
      case REF_TYPE_TAG:
        consumer.tag()
            .commit(deserializeId(entity, COMMIT));
        break;
      default:
        throw new IllegalStateException("Invalid ref-type '" + refType + "'");
    }
  }

  private static void deserializeCommits(Map<String, AttributeValue> map, BranchCommit cc) {
    for (AttributeValue raw : attributeValue(map, COMMITS).l()) {
      cc = deserializeCommit(raw.m(), cc);
    }
  }

  private static BranchCommit deserializeCommit(Map<String, AttributeValue> map, BranchCommit cc) {
    cc = cc.id(deserializeId(map, ID))
        .commit(deserializeId(map, COMMIT));

    if (map.containsKey(PARENT)) {
      return cc.saved()
          .parent(deserializeId(map, PARENT))
          .done();
    } else {
      UnsavedCommitDelta deltas = cc.unsaved();
      if (map.containsKey(DELTAS)) {
        for (AttributeValue av : attributeValue(map, DELTAS).l()) {
          Map<String, AttributeValue> m = av.m();
          deltas.delta(deserializeInt(m, POSITION), deserializeId(m, OLD_ID), deserializeId(m, NEW_ID));
        }
      }

      UnsavedCommitMutations mutations = deltas.mutations();
      if (map.containsKey(KEY_LIST)) {
        for (AttributeValue raw : attributeValue(map, KEY_LIST).l()) {
          mutations.keyMutation(deserializeKeyMutation(raw));
        }
      }

      return mutations.done();
    }
  }
}
