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
import java.util.stream.Stream;

import com.dremio.nessie.tiered.builder.L1Consumer;
import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.ValueType;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue.Builder;

class DynamoL1Consumer extends DynamoConsumer<L1Consumer> implements L1Consumer {

  static final String MUTATIONS = "mutations";
  static final String FRAGMENTS = "fragments";
  static final String IS_CHECKPOINT = "chk";
  static final String ORIGIN = "origin";
  static final String DISTANCE = "dist";
  static final String PARENTS = "parents";
  static final String TREE = "tree";
  static final String METADATA = "metadata";
  static final String KEY_LIST = "keys";

  private final Map<String, AttributeValue.Builder> keys;
  private final List<AttributeValue.Builder> keysMutations;

  DynamoL1Consumer() {
    super(ValueType.L1);
    keys = new HashMap<>();
    keysMutations = new ArrayList<>();
  }

  @Override
  public boolean canHandleType(ValueType valueType) {
    return valueType == ValueType.L1;
  }

  @Override
  public DynamoL1Consumer commitMetadataId(Id id) {
    addEntitySafe(METADATA, bytes(id.getValue()));
    return this;
  }

  @Override
  public DynamoL1Consumer ancestors(Stream<Id> ids) {
    addIdList(PARENTS, ids);
    return this;
  }

  @Override
  public DynamoL1Consumer children(Stream<Id> ids) {
    addIdList(TREE, ids);
    return this;
  }

  @Override
  public DynamoL1Consumer id(Id id) {
    addEntitySafe(ID, bytes(id.getValue()));
    return this;
  }

  @Override
  public DynamoL1Consumer addKeyAddition(Key key) {
    addKeyMutation(true, key);
    return this;
  }

  @Override
  public DynamoL1Consumer addKeyRemoval(Key key) {
    addKeyMutation(false, key);
    return this;
  }

  private void addKeyMutation(boolean add, Key key) {
    // TODO potentially peal out com.dremio.nessie.versioned.impl.KeyMutation.MutationType to get proper constants?
    String addRemove = add ? KEY_ADDITION : KEY_REMOVAL;

    // TODO can we change the data model here?
    // Current layout is:
    // keys [ "mutations" ] :=
    //    list(
    //        map(
    //            "a" / "d",
    //            list ( key-elements )
    //        )
    //    )
    //
    // Proposal:
    // keys [ "mutations-a" ] :=
    //    list(
    //        list ( key-elements )
    //    )
    // keys [ "mutations-d" ] :=
    //    list(
    //        list ( key-elements )
    //    )
    //
    // Even better:
    //    keys [ "mutations-a" ] := list ( byte-strings )
    //    keys [ "mutations-d" ] := list ( byte-strings )
    // And each "byte-string" is our custom serialzation of a key-path.
    // TL;DR a quite flat structure.

    keysMutations.add(singletonMap(addRemove, keyList(key)));
  }

  @Override
  public DynamoL1Consumer incrementalKeyList(Id checkpointId, int distanceFromCheckpoint) {
    addKeysSafe(IS_CHECKPOINT, bool(false));
    addKeysSafe(ORIGIN, bytes(checkpointId.getValue()));
    addKeysSafe(DISTANCE, number(distanceFromCheckpoint));
    return this;
  }

  @Override
  public DynamoL1Consumer completeKeyList(Stream<Id> fragmentIds) {
    addKeysSafe(IS_CHECKPOINT, bool(true));
    addKeysSafe(FRAGMENTS, idsList(fragmentIds));
    return this;
  }

  @Override
  public Map<String, AttributeValue> build() {
    addKeysSafe(MUTATIONS, list(buildValues(keysMutations)));
    if (!keys.isEmpty()) {
      addEntitySafe(KEY_LIST, map(buildValuesMap(keys)));
    }

    return super.build();
  }

  private void addKeysSafe(String key, Builder value) {
    Builder old = keys.put(key, value);
    if (old != null) {
      throw new IllegalStateException("Duplicate '" + key + "' in 'keys' map. Old={" + old + "} current={" + value + "}");
    }
  }

  static class Producer extends DynamoProducer<L1Consumer> {

    public Producer(Map<String, AttributeValue> map) {
      super(map);
    }

    @Override
    public void applyToConsumer(L1Consumer consumer) {
      consumer.id(deserializeId(entity));

      if (entity.containsKey(METADATA)) {
        consumer.commitMetadataId(deserializeId(entity.get(METADATA)));
      }
      if (entity.containsKey(TREE)) {
        consumer.children(deserializeIdList(entity.get(TREE)));
      }
      if (entity.containsKey(PARENTS)) {
        consumer.ancestors(deserializeIdList(entity.get(PARENTS)));
      }
      if (entity.containsKey(KEY_LIST)) {
        Map<String, AttributeValue> keys = entity.get(KEY_LIST).m();
        Boolean checkpoint = keys.get(IS_CHECKPOINT).bool();
        if (checkpoint != null) {
          if (checkpoint) {
            consumer.completeKeyList(deserializeIdList(keys.get(FRAGMENTS)));
          } else {
            consumer.incrementalKeyList(
                deserializeId(keys.get(ORIGIN)),
                deserializeInt(keys.get(DISTANCE))
            );
          }
        }
        // See com.dremio.nessie.versioned.store.dynamo.DynamoL1Consumer.addKeyMutation about a
        // proposal to simplify this one.
        List<AttributeValue> mutations = keys.get(MUTATIONS).l();
        deserializeKeyMutations(
            mutations,
            consumer::addKeyAddition,
            consumer::addKeyRemoval
        );
      }
    }
  }
}
