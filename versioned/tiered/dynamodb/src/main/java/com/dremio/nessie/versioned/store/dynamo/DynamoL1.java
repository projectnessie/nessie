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
import static com.dremio.nessie.versioned.store.dynamo.AttributeValueUtil.bool;
import static com.dremio.nessie.versioned.store.dynamo.AttributeValueUtil.deserializeId;
import static com.dremio.nessie.versioned.store.dynamo.AttributeValueUtil.deserializeIdStream;
import static com.dremio.nessie.versioned.store.dynamo.AttributeValueUtil.deserializeInt;
import static com.dremio.nessie.versioned.store.dynamo.AttributeValueUtil.idValue;
import static com.dremio.nessie.versioned.store.dynamo.AttributeValueUtil.idsList;
import static com.dremio.nessie.versioned.store.dynamo.AttributeValueUtil.list;
import static com.dremio.nessie.versioned.store.dynamo.AttributeValueUtil.map;
import static com.dremio.nessie.versioned.store.dynamo.AttributeValueUtil.number;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import com.dremio.nessie.tiered.builder.L1;
import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.ValueType;
import com.google.common.base.Preconditions;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

class DynamoL1 extends DynamoBaseValue<L1> implements L1 {

  static final String MUTATIONS = "mutations";
  static final String FRAGMENTS = "fragments";
  static final String IS_CHECKPOINT = "chk";
  static final String ORIGIN = "origin";
  static final String DISTANCE = "dist";
  static final String PARENTS = "parents";
  static final String TREE = "tree";
  static final String METADATA = "metadata";
  static final String KEY_LIST = "keys";

  private final Map<String, AttributeValue> keys;

  DynamoL1() {
    super(ValueType.L1);
    keys = new HashMap<>();
  }

  @Override
  public L1 commitMetadataId(Id id) {
    return addEntitySafe(METADATA, idValue(id));
  }

  @Override
  public L1 ancestors(Stream<Id> ids) {
    return addIdList(PARENTS, ids);
  }

  @Override
  public L1 children(Stream<Id> ids) {
    return addIdList(TREE, ids);
  }

  @Override
  public L1 keyMutations(Stream<Key.Mutation> keyMutations) {
    addKeysSafe(MUTATIONS, list(keyMutations.map(AttributeValueUtil::serializeKeyMutation)));
    return this;
  }

  @Override
  public L1 incrementalKeyList(Id checkpointId, int distanceFromCheckpoint) {
    addKeysSafe(IS_CHECKPOINT, bool(false));
    addKeysSafe(ORIGIN, idValue(checkpointId));
    addKeysSafe(DISTANCE, number(distanceFromCheckpoint));
    return this;
  }

  @Override
  public L1 completeKeyList(Stream<Id> fragmentIds) {
    addKeysSafe(IS_CHECKPOINT, bool(true));
    addKeysSafe(FRAGMENTS, idsList(fragmentIds));
    return this;
  }

  @Override
  Map<String, AttributeValue> build() {
    if (!keys.isEmpty()) {
      addEntitySafe(KEY_LIST, map(keys));
    }

    checkPresent(METADATA, "metadata");
    checkPresent(TREE, "children");
    checkPresent(PARENTS, "ancestors");

    return super.build();
  }

  private void addKeysSafe(String key, AttributeValue value) {
    AttributeValue old = keys.put(key, value);
    if (old != null) {
      throw new IllegalStateException("Duplicate '" + key + "' in 'keys' map. Old={" + old + "} current={" + value + "}");
    }
  }

  /**
   * Deserialize a DynamoDB entity into the given consumer.
   */
  static void toConsumer(Map<String, AttributeValue> entity, L1 consumer) {
    DynamoBaseValue.toConsumer(entity, consumer);

    if (entity.containsKey(METADATA)) {
      consumer.commitMetadataId(deserializeId(entity, METADATA));
    }
    if (entity.containsKey(TREE)) {
      consumer.children(deserializeIdStream(entity, TREE));
    }
    if (entity.containsKey(PARENTS)) {
      consumer.ancestors(deserializeIdStream(entity, PARENTS));
    }
    if (entity.containsKey(KEY_LIST)) {
      Map<String, AttributeValue> keys = Preconditions.checkNotNull(
          attributeValue(entity, KEY_LIST).m(), "mandatory map " + KEY_LIST + " is null");
      Boolean checkpoint = attributeValue(keys, IS_CHECKPOINT).bool();
      if (checkpoint != null) {
        if (checkpoint) {
          consumer.completeKeyList(deserializeIdStream(keys, FRAGMENTS));
        } else {
          consumer.incrementalKeyList(
              deserializeId(keys, ORIGIN),
              deserializeInt(keys, DISTANCE)
          );
        }
      }
      // See com.dremio.nessie.versioned.store.dynamo.DynamoL1Consumer.addKeyMutation about a
      // proposal to simplify this one.
      consumer.keyMutations(attributeValue(keys, MUTATIONS).l().stream()
          .map(AttributeValueUtil::deserializeKeyMutation));
    }
  }
}
