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
import static com.dremio.nessie.versioned.store.dynamo.AttributeValueUtil.singletonMap;
import static com.dremio.nessie.versioned.store.dynamo.AttributeValueUtil.string;

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
  public RefConsumer commits(Stream<BranchCommit> commits) {
    return addEntitySafe(COMMITS, list(commits.map(DynamoRefConsumer::commitToMap)));
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

  private static AttributeValue commitToMap(BranchCommit c) {
    Map<String, AttributeValue> builder = new HashMap<>();
    builder.put(ID, idValue(c.getId()));
    builder.put(COMMIT, idValue(c.getCommit()));

    if (c.isSaved()) {
      builder.put(PARENT, idValue(c.getParent()));
    } else {
      Stream<AttributeValue> deltas = c.getDeltas().stream()
          .map(DynamoRefConsumer::serializeDelta);
      builder.put(DELTAS, list(deltas));

      Stream<AttributeValue> keyMutations = Stream.concat(
          c.getKeyAdditions().stream()
              .map(AttributeValueUtil::keyElements)
              .map(km -> singletonMap(KEY_ADDITION, km)),
          c.getKeyRemovals().stream()
              .map(AttributeValueUtil::keyElements)
              .map(km -> singletonMap(KEY_REMOVAL, km)));
      builder.put(KEY_LIST, list(keyMutations));
    }

    return map(builder);
  }

  private static AttributeValue serializeDelta(BranchUnsavedDelta d) {
    Map<String, AttributeValue> map = new HashMap<>();
    map.put(POSITION, number(d.getPosition()));
    map.put(OLD_ID, idValue(d.getOldId()));
    map.put(NEW_ID, idValue(d.getNewId()));
    return map(map);
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
            .commits(deserializeCommits(entity));

        break;
      case REF_TYPE_TAG:
        consumer.type(RefType.TAG)
            .commit(deserializeId(entity, COMMIT));
        break;
      default:
        throw new IllegalStateException("Invalid ref-type '" + refType + "'");
    }
  }

  private static Stream<BranchCommit> deserializeCommits(Map<String, AttributeValue> map) {
    AttributeValue raw = attributeValue(map, COMMITS);
    return raw.l().stream()
        .map(AttributeValue::m)
        .map(DynamoRefConsumer::deserializeCommit);
  }

  private static BranchCommit deserializeCommit(Map<String, AttributeValue> map) {
    Id id = deserializeId(map, ID);
    Id commit = deserializeId(map, COMMIT);

    if (!map.containsKey(DELTAS)) {
      return new BranchCommit(
          id,
          commit,
          deserializeId(map, PARENT));
    }

    List<BranchUnsavedDelta> deltas = mandatoryList(attributeValue(map, DELTAS)).stream()
        .map(AttributeValue::m)
        .map(DynamoRefConsumer::deserializeUnsavedDelta)
        .collect(Collectors.toList());

    List<Key> keyAdditions = new ArrayList<>();
    List<Key> keyRemovals = new ArrayList<>();

    deserializeKeyMutations(
        map,
        KEY_LIST,
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
        deserializeInt(map, POSITION),
        deserializeId(map, OLD_ID),
        deserializeId(map, NEW_ID));
  }
}
