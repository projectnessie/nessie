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

import static com.dremio.nessie.versioned.store.dynamo.DynamoConstants.DISTANCE;
import static com.dremio.nessie.versioned.store.dynamo.DynamoConstants.FRAGMENTS;
import static com.dremio.nessie.versioned.store.dynamo.DynamoConstants.ID;
import static com.dremio.nessie.versioned.store.dynamo.DynamoConstants.IS_CHECKPOINT;
import static com.dremio.nessie.versioned.store.dynamo.DynamoConstants.KEY_LIST;
import static com.dremio.nessie.versioned.store.dynamo.DynamoConstants.METADATA;
import static com.dremio.nessie.versioned.store.dynamo.DynamoConstants.MUTATIONS;
import static com.dremio.nessie.versioned.store.dynamo.DynamoConstants.ORIGIN;
import static com.dremio.nessie.versioned.store.dynamo.DynamoConstants.PARENTS;
import static com.dremio.nessie.versioned.store.dynamo.DynamoConstants.TREE;
import static com.dremio.nessie.versioned.store.dynamo.DynamoConstants.KEY_ADDITION;
import static com.dremio.nessie.versioned.store.dynamo.DynamoConstants.KEY_REMOVAL;

import com.dremio.nessie.tiered.builder.L1Consumer;
import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.ValueType;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue.Builder;
import software.amazon.awssdk.utils.builder.SdkBuilder;

class DynamoL1Consumer implements L1Consumer<DynamoL1Consumer> {

  private final Map<String, AttributeValue.Builder> entity;
  private final Map<String, AttributeValue.Builder> keys;
  private final List<AttributeValue.Builder> keysMutations;

  DynamoL1Consumer() {
    entity = new HashMap<>();
    entity.put("t", string(ValueType.L1.getValueName()));
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
    System.err.println("addAncestors " + ids);
    addIdList(PARENTS, ids);
    return this;
  }

  @Override
  public DynamoL1Consumer children(List<Id> ids) {
    System.err.println("children " + ids);
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
    System.err.println("addKeyAddition " + key);
    addKeyMutation(true, key);
    return this;
  }

  @Override
  public DynamoL1Consumer addKeyRemoval(Key key) {
    System.err.println("addKeyRemoval " + key);
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

    keysMutations.add(
        map(Collections.singletonMap(
            addRemove,
            list(key.getElements().stream()
                .map(elem -> string(elem).build()).collect(Collectors.toList()))
                .build())
        )
    );
  }

  @Override
  public DynamoL1Consumer incrementalKeyList(Id checkpointId, int distanceFromCheckpoint) {
    System.err.println("incrementalKeyList " + checkpointId + " " + distanceFromCheckpoint);
    addKeysSafe(IS_CHECKPOINT, bool(false));
    addKeysSafe(ORIGIN, bytes(checkpointId.getValue()));
    addKeysSafe(DISTANCE, number(distanceFromCheckpoint));
    return this;
  }

  @Override
  public DynamoL1Consumer completeKeyList(List<Id> fragmentIds) {
    System.err.println("fragmentIds " + fragmentIds);
    addKeysSafe(IS_CHECKPOINT, bool(true));
    addKeysSafe(FRAGMENTS, idsList(fragmentIds));
    return this;
  }

  public Map<String, AttributeValue> getEntity() {
    // TODO the original 'toEntity' implementation adds empty 'mutations' - is that necessary?
    addKeysSafe(MUTATIONS, list(buildValues(keysMutations)));
    if (!keys.isEmpty()) {
      addEntitySafe(KEY_LIST, map(buildValuesMap(keys)));
    }
    return buildValuesMap(entity);
  }

  private void addIdList(String key, List<Id> ids) {
    addEntitySafe(key, idsList(ids));
  }

  private void addEntitySafe(String key, Builder value) {
    Builder old = entity.put(key, value);
    if (old != null) {
      throw new IllegalStateException("Duplicate '" + key + "' in 'entity' map. Old={" + old + "} current={" + value + "}");
    }
  }

  private void addKeysSafe(String key, Builder value) {
    Builder old = keys.put(key, value);
    if (old != null) {
      throw new IllegalStateException("Duplicate '" + key + "' in 'keys' map. Old={" + old + "} current={" + value + "}");
    }
  }

  private static Builder idsList(List<Id> ids) {
    List<AttributeValue> idsList = ids.stream()
        .map(id -> bytes(id.getValue()).build())
        .collect(Collectors.toList());
    return list(idsList);
  }

  private static Map<String, AttributeValue> buildValuesMap(Map<String, AttributeValue.Builder> source) {
    return source.entrySet().stream()
        .collect(Collectors.toMap(
            Entry::getKey,
            e -> e.getValue().build()
        ));
  }

  private static List<AttributeValue> buildValues(List<AttributeValue.Builder> source) {
    return source.stream()
        .map(SdkBuilder::build)
        .collect(Collectors.toList());
  }

  private static Builder bytes(ByteString bytes) {
    return AttributeValue.builder().b(SdkBytes.fromByteBuffer(bytes.asReadOnlyByteBuffer()));
  }

  private static Builder bool(boolean bool) {
    return AttributeValue.builder().bool(bool);
  }

  private static Builder number(int number) {
    return AttributeValue.builder().n(Integer.toString(number));
  }

  private static Builder string(String string) {
    return AttributeValue.builder().s(string);
  }

  private static Builder list(List<AttributeValue> list) {
    return AttributeValue.builder().l(list);
  }

  private static Builder map(Map<String, AttributeValue> map) {
    return AttributeValue.builder().m(map);
  }
}
