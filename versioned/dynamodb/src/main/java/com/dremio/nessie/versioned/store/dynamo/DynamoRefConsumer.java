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

import static com.dremio.nessie.versioned.store.dynamo.AttributeValueUtil.deserializeId;
import static com.dremio.nessie.versioned.store.dynamo.AttributeValueUtil.deserializeIdList;
import static com.dremio.nessie.versioned.store.dynamo.AttributeValueUtil.deserializeInt;
import static com.dremio.nessie.versioned.store.dynamo.AttributeValueUtil.deserializeKey;
import static com.dremio.nessie.versioned.store.dynamo.AttributeValueUtil.serializeId;
import static com.dremio.nessie.versioned.store.dynamo.DynamoConstants.COMMIT;
import static com.dremio.nessie.versioned.store.dynamo.DynamoConstants.COMMITS;
import static com.dremio.nessie.versioned.store.dynamo.DynamoConstants.DELTAS;
import static com.dremio.nessie.versioned.store.dynamo.DynamoConstants.ID;
import static com.dremio.nessie.versioned.store.dynamo.DynamoConstants.KEY_ADDITION;
import static com.dremio.nessie.versioned.store.dynamo.DynamoConstants.KEY_LIST;
import static com.dremio.nessie.versioned.store.dynamo.DynamoConstants.KEY_REMOVAL;
import static com.dremio.nessie.versioned.store.dynamo.DynamoConstants.METADATA;
import static com.dremio.nessie.versioned.store.dynamo.DynamoConstants.NAME;
import static com.dremio.nessie.versioned.store.dynamo.DynamoConstants.NEW_ID;
import static com.dremio.nessie.versioned.store.dynamo.DynamoConstants.OLD_ID;
import static com.dremio.nessie.versioned.store.dynamo.DynamoConstants.PARENT;
import static com.dremio.nessie.versioned.store.dynamo.DynamoConstants.POSITION;
import static com.dremio.nessie.versioned.store.dynamo.DynamoConstants.REF_TYPE_BRANCH;
import static com.dremio.nessie.versioned.store.dynamo.DynamoConstants.REF_TYPE_TAG;
import static com.dremio.nessie.versioned.store.dynamo.DynamoConstants.TREE;
import static com.dremio.nessie.versioned.store.dynamo.DynamoConstants.TYPE;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.dremio.nessie.tiered.builder.RefConsumer;
import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.impl.InternalRef;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.ValueType;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class DynamoRefConsumer extends DynamoConsumer<DynamoRefConsumer> implements
    RefConsumer<DynamoRefConsumer> {

  DynamoRefConsumer() {
    super(ValueType.VALUE);
  }

  @Override
  public DynamoRefConsumer id(Id id) {
    addEntitySafe(ID, bytes(id.getValue()));
    return this;
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
  public DynamoRefConsumer children(List<Id> children) {
    addIdList(TREE, children);
    return this;
  }

  @Override
  public DynamoRefConsumer commits(List<BranchCommit> commits) {
    List<AttributeValue> commitList = commits.stream()
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

  @Override
  Map<String, AttributeValue> getEntity() {
    return buildValuesMap(entity);
  }

  static class Producer implements DynamoProducer<InternalRef> {
    @Override
    public InternalRef deserialize(Map<String, AttributeValue> entity) {
      InternalRef.Builder builder = InternalRef.builder();
      builder.id(deserializeId(entity));
      builder.name(entity.get(NAME).s());

      String refType = entity.get(TYPE).s();
      switch (refType) {
        case REF_TYPE_BRANCH:
          builder.type(RefType.BRANCH);

          builder.metadata(deserializeId(entity.get(METADATA)));
          builder.children(deserializeIdList(entity.get(TREE)));
          builder.commits(deserializeCommits(entity.get(COMMITS)));

          break;
        case REF_TYPE_TAG:
          builder.type(RefType.TAG);
          builder.commit(deserializeId(entity.get(COMMIT)));
          break;
        default:
          throw new IllegalStateException("Invalid ref-type '" + refType + "'");
      }

      return builder.build();
    }

    private static List<BranchCommit> deserializeCommits(AttributeValue attributeValue) {
      return attributeValue.l().stream().map(a -> deserializeCommit(a.m())).collect(Collectors.toList());
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

      map.get(KEY_LIST).l().stream()
          .map(AttributeValue::m)
          .forEach(m -> {
            if (m.size() > 2) {
              throw new IllegalStateException("Ugh - got a keys.mutations map like this: " + m);
            }
            AttributeValue raw = m.get(KEY_ADDITION);
            if (raw != null) {
              keyAdditions.add(deserializeKey(raw));
            }
            raw = m.get(KEY_REMOVAL);
            if (raw != null) {
              keyRemovals.add(deserializeKey(raw));
            }
          });

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
