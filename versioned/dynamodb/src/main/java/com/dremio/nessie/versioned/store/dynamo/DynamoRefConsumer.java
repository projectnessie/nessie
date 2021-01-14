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
import static com.dremio.nessie.versioned.store.dynamo.AttributeValueUtil.deserializeKeyMutations;
import static com.dremio.nessie.versioned.store.dynamo.AttributeValueUtil.idValue;
import static com.dremio.nessie.versioned.store.dynamo.AttributeValueUtil.list;
import static com.dremio.nessie.versioned.store.dynamo.AttributeValueUtil.mandatoryList;
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

import com.dremio.nessie.tiered.builder.RefConsumer;
import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.ValueType;
import com.google.common.base.Preconditions;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

class DynamoRefConsumer extends DynamoConsumer<RefConsumer> implements RefConsumer {

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

  DynamoRefConsumer() {
    super(ValueType.REF);
  }

  @Override
  public RefConsumer type(RefType refType) {
    switch (refType) {
      case TAG:
        return addEntitySafe(TYPE, string(REF_TYPE_TAG));
      case BRANCH:
        return addEntitySafe(TYPE, string(REF_TYPE_BRANCH));
      default:
        throw new IllegalArgumentException("Unknown ref-type " + refType);
    }
  }

  @Override
  public RefConsumer name(String name) {
    return addEntitySafe(NAME, string(name));
  }

  @Override
  public RefConsumer commit(Id commit) {
    return addEntitySafe(COMMIT, idValue(commit));
  }

  @Override
  public RefConsumer metadata(Id metadata) {
    return addEntitySafe(METADATA, idValue(metadata));
  }

  @Override
  public RefConsumer children(Stream<Id> children) {
    return addIdList(TREE, children);
  }

  @Override
  public RefConsumer commits(Consumer<BranchCommitConsumer> commits) {
    List<AttributeValue> commitsList = new ArrayList<>();
    commits.accept(new BranchCommitConsumer() {
      final Map<String, AttributeValue> builder = new HashMap<>();
      List<AttributeValue> deltas = null;
      List<AttributeValue> keyMutations = null;

      @Override
      public BranchCommitConsumer id(Id id) {
        builder.put(ID, idValue(id));
        return this;
      }

      @Override
      public BranchCommitConsumer commit(Id commit) {
        builder.put(COMMIT, idValue(commit));
        return this;
      }

      @Override
      public BranchCommitConsumer parent(Id parent) {
        builder.put(PARENT, idValue(parent));
        return this;
      }

      @Override
      public BranchCommitConsumer delta(int position, Id oldId, Id newId) {
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
      public BranchCommitConsumer keyMutation(Key.Mutation keyMutation) {
        if (keyMutations == null) {
          keyMutations = new ArrayList<>();
        }
        keyMutations.add(serializeKeyMutation(keyMutation));
        return this;
      }

      @Override
      public BranchCommitConsumer done() {
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
    });
    return addEntitySafe(COMMITS, builder().l(commitsList).build());
  }

  @Override
  Map<String, AttributeValue> build() {
    checkPresent(NAME, "name");
    checkPresent(TYPE, "type");

    if (entity.get(TYPE).s().equals(REF_TYPE_TAG)) {
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

  /**
   * Deserialize a DynamoDB entity into the given consumer.
   */
  static void toConsumer(Map<String, AttributeValue> entity, RefConsumer consumer) {
    consumer.id(deserializeId(entity, ID))
        .name(Preconditions.checkNotNull(attributeValue(entity, NAME).s()));

    String refType = Preconditions.checkNotNull(attributeValue(entity, TYPE).s());
    switch (refType) {
      case REF_TYPE_BRANCH:
        consumer.type(RefType.BRANCH);

        consumer.metadata(deserializeId(entity, METADATA))
            .children(deserializeIdStream(entity, TREE))
            .commits(cc -> deserializeCommits(entity, cc));

        break;
      case REF_TYPE_TAG:
        consumer.type(RefType.TAG)
            .commit(deserializeId(entity, COMMIT));
        break;
      default:
        throw new IllegalStateException("Invalid ref-type '" + refType + "'");
    }
  }

  private static void deserializeCommits(Map<String, AttributeValue> map, BranchCommitConsumer cc) {
    AttributeValue raw = attributeValue(map, COMMITS);
    raw.l().stream()
        .map(AttributeValue::m)
        .forEach(m -> deserializeCommit(m, cc));
  }

  private static void deserializeCommit(Map<String, AttributeValue> map, BranchCommitConsumer cc) {
    cc.id(deserializeId(map, ID))
        .commit(deserializeId(map, COMMIT));

    if (map.containsKey(PARENT)) {
      cc.parent(deserializeId(map, PARENT));
    } else {
      if (map.containsKey(DELTAS)) {
        mandatoryList(attributeValue(map, DELTAS)).forEach(av -> {
          Map<String, AttributeValue> m = av.m();
          cc.delta(deserializeInt(m, POSITION), deserializeId(m, OLD_ID), deserializeId(m, NEW_ID));
        });
      }

      if (map.containsKey(KEY_LIST)) {
        deserializeKeyMutations(
            map,
            KEY_LIST,
            km -> km.forEach(cc::keyMutation)
        );
      }
    }
    cc.done();
  }
}
