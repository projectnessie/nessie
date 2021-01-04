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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.dremio.nessie.tiered.builder.RefConsumer;
import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.ValueType;

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
  public DynamoRefConsumer id(Id id) {
    addEntitySafe(ID, bytes(id.getValue()));
    return this;
  }

  @Override
  public boolean canHandleType(ValueType valueType) {
    return valueType == ValueType.REF;
  }

  @Override
  public DynamoRefConsumer type(RefType refType) {
    switch (refType) {
      case TAG:
        addEntitySafe(TYPE, string(REF_TYPE_TAG));
        break;
      case BRANCH:
        addEntitySafe(TYPE, string(REF_TYPE_BRANCH));
        break;
      default:
        throw new IllegalArgumentException("Unknown ref-type " + refType);
    }
    return this;
  }

  @Override
  public DynamoRefConsumer name(String name) {
    addEntitySafe(NAME, string(name));
    return this;
  }

  @Override
  public DynamoRefConsumer commit(Id commit) {
    addEntitySafe(COMMIT, idBuilder(commit));
    return this;
  }

  @Override
  public DynamoRefConsumer metadata(Id metadata) {
    addEntitySafe(METADATA, idBuilder(metadata));
    return this;
  }

  @Override
  public DynamoRefConsumer children(Stream<Id> children) {
    addIdList(TREE, children);
    return this;
  }

  @Override
  public DynamoRefConsumer commits(Stream<BranchCommit> commits) {
    List<AttributeValue> commitList = commits
        .map(DynamoRefConsumer::commitToMap)
        .collect(Collectors.toList());

    addEntitySafe(COMMITS, AttributeValue.builder().l(commitList));

    return this;
  }

  private static AttributeValue commitToMap(BranchCommit c) {
    Map<String, AttributeValue> builder = new HashMap<>();
    builder.put(ID, serializeId(c.getId()));
    builder.put(COMMIT, serializeId(c.getCommit()));

    if (c.isSaved()) {
      builder.put(PARENT, serializeId(c.getParent()));
    } else {
      List<AttributeValue> deltas = c.getDeltas().stream()
          .map(DynamoRefConsumer::serializeDelta)
          .collect(Collectors.toList());
      builder.put(DELTAS, AttributeValue.builder().l(deltas).build());

      List<AttributeValue> keyMutations = new ArrayList<>();
      c.getKeyAdditions().stream()
          .map(km -> serializeKeyMutation(KEY_ADDITION, km))
          .forEach(keyMutations::add);
      c.getKeyRemovals().stream()
          .map(km -> serializeKeyMutation(KEY_REMOVAL, km))
          .forEach(keyMutations::add);
      builder.put(KEY_LIST, AttributeValue.builder().l(keyMutations).build());
    }

    return AttributeValue.builder().m(builder).build();
  }

  private static AttributeValue serializeKeyMutation(String type, Key key) {
    return singletonMap(type, keyList(key)).build();
  }

  private static AttributeValue serializeDelta(BranchUnsavedDelta d) {
    Map<String, AttributeValue> map = new HashMap<>();
    map.put(POSITION, number(d.getPosition()).build());
    map.put(OLD_ID, idValue(d.getOldId()));
    map.put(NEW_ID, idValue(d.getNewId()));
    return AttributeValue.builder().m(map).build();
  }

  static class Producer extends DynamoProducer<RefConsumer> {
    public Producer(Map<String, AttributeValue> entity) {
      super(entity);
    }

    @Override
    public void applyToConsumer(RefConsumer consumer) {
      consumer.id(deserializeId(entity))
          .name(entity.get(NAME).s());

      String refType = entity.get(TYPE).s();
      switch (refType) {
        case REF_TYPE_BRANCH:
          consumer.type(RefType.BRANCH);

          consumer.metadata(deserializeId(entity.get(METADATA)))
              .children(deserializeIdList(entity.get(TREE)))
              .commits(deserializeCommits(entity.get(COMMITS)));

          break;
        case REF_TYPE_TAG:
          consumer.type(RefType.TAG)
              .commit(deserializeId(entity.get(COMMIT)));
          break;
        default:
          throw new IllegalStateException("Invalid ref-type '" + refType + "'");
      }
    }

    private static Stream<BranchCommit> deserializeCommits(AttributeValue attributeValue) {
      return attributeValue.l().stream().map(a -> deserializeCommit(a.m()));
    }

    private static BranchCommit deserializeCommit(Map<String, AttributeValue> map) {
      Id id = deserializeId(map);
      Id commit = deserializeId(map.get(COMMIT));

      if (!map.containsKey(DELTAS)) {
        return new BranchCommit(
            id,
            commit,
            deserializeId(map.get(PARENT)));
      }

      List<BranchUnsavedDelta> deltas = map.get(DELTAS).l().stream()
          .map(AttributeValue::m)
          .map(Producer::deserializeUnsavedDelta)
          .collect(Collectors.toList());

      List<Key> keyAdditions = new ArrayList<>();
      List<Key> keyRemovals = new ArrayList<>();

      deserializeKeyMutations(
          map.get(KEY_LIST).l(),
          keyAdditions::add,
          keyRemovals::add
      );

      return new BranchCommit(
          id,
          commit,
          deltas,
          keyAdditions,
          keyRemovals
      );
    }

    private static BranchUnsavedDelta deserializeUnsavedDelta(Map<String, AttributeValue> map) {
      return new BranchUnsavedDelta(
          deserializeInt(map.get(POSITION)),
          deserializeId(map.get(OLD_ID)),
          deserializeId(map.get(NEW_ID)));
    }
  }
}
