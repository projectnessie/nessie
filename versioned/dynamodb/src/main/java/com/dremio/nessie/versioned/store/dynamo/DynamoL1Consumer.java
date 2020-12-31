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

import com.dremio.nessie.tiered.builder.L1Consumer;
import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.impl.L1;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.ValueType;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue.Builder;

class DynamoL1Consumer extends DynamoConsumer<DynamoL1Consumer> implements L1Consumer<DynamoL1Consumer> {

  private final Map<String, AttributeValue.Builder> keys;
  private final List<AttributeValue.Builder> keysMutations;

  DynamoL1Consumer() {
    super(ValueType.L1);
    keys = new HashMap<>();
    keysMutations = new ArrayList<>();
  }

  @Override
  public DynamoL1Consumer commitMetadataId(Id id) {
    addEntitySafe(METADATA, bytes(id.getValue()));
    return this;
  }

  @Override
  public DynamoL1Consumer addAncestors(List<Id> ids) {
    addIdList(PARENTS, ids);
    return this;
  }

  @Override
  public DynamoL1Consumer children(List<Id> ids) {
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
  public DynamoL1Consumer completeKeyList(List<Id> fragmentIds) {
    addKeysSafe(IS_CHECKPOINT, bool(true));
    addKeysSafe(FRAGMENTS, idsList(fragmentIds));
    return this;
  }

  @Override
  Map<String, AttributeValue> getEntity() {
    // TODO add validation

    addKeysSafe(MUTATIONS, list(buildValues(keysMutations)));
    if (!keys.isEmpty()) {
      addEntitySafe(KEY_LIST, map(buildValuesMap(keys)));
    }
    return buildValuesMap(entity);
  }

  private void addKeysSafe(String key, Builder value) {
    Builder old = keys.put(key, value);
    if (old != null) {
      throw new IllegalStateException("Duplicate '" + key + "' in 'keys' map. Old={" + old + "} current={" + value + "}");
    }
  }

  static class Producer implements DynamoProducer<L1> {
    @Override
    public L1 deserialize(Map<String, AttributeValue> entity) {
      L1.Builder builder = L1.builder()
          .id(deserializeId(entity));

      if (entity.containsKey(METADATA)) {
        builder.commitMetadataId(deserializeId(entity.get(METADATA)));
      }
      if (entity.containsKey(TREE)) {
        builder.children(deserializeIdList(entity.get(TREE)));
      }
      if (entity.containsKey(PARENTS)) {
        builder.addAncestors(deserializeIdList(entity.get(PARENTS)));
      }
      if (entity.containsKey(KEY_LIST)) {
        Map<String, AttributeValue> keys = entity.get(KEY_LIST).m();
        Boolean checkpoint = keys.get(IS_CHECKPOINT).bool();
        if (checkpoint != null) {
          if (checkpoint) {
            builder.completeKeyList(deserializeIdList(keys.get(FRAGMENTS)));
          } else {
            builder.incrementalKeyList(
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
            builder::addKeyAddition,
            builder::addKeyRemoval
        );
      }

      return builder.build();
    }
  }
}
