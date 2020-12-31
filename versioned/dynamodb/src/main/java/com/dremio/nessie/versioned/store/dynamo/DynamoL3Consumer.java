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
import static com.dremio.nessie.versioned.store.dynamo.AttributeValueUtil.deserializeKey;
import static com.dremio.nessie.versioned.store.dynamo.DynamoConstants.ID;
import static com.dremio.nessie.versioned.store.dynamo.DynamoConstants.TREE;
import static com.dremio.nessie.versioned.store.dynamo.DynamoConstants.TREE_ID;
import static com.dremio.nessie.versioned.store.dynamo.DynamoConstants.TREE_KEY;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import com.dremio.nessie.tiered.builder.L3Consumer;
import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.impl.L3;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.ValueType;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue.Builder;

class DynamoL3Consumer extends DynamoConsumer<DynamoL3Consumer> implements L3Consumer<DynamoL3Consumer> {

  private final TreeMap<Key, Id> tree;

  DynamoL3Consumer() {
    super(ValueType.L3);
    tree = new TreeMap<>();
  }

  @Override
  public DynamoL3Consumer addKeyDelta(Key key, Id id) {
    Id old = tree.put(key, id);
    if (old != null) {
      throw new IllegalArgumentException("Key '" + key + "' added twice: " + old + " + " + id);
    }
    return this;
  }

  @Override
  public DynamoL3Consumer id(Id id) {
    addEntitySafe(ID, bytes(id.getValue()));
    return this;
  }

  @Override
  Map<String, AttributeValue> getEntity() {
    // TODO is this correct ??
    List<AttributeValue> maps = tree.entrySet().stream()
        .filter(e -> !e.getValue().isEmpty())
        .map(e ->
            map(dualMap(
                TREE_KEY, keyList(e.getKey()),
                TREE_ID, idValue(e.getValue())
            )).build()
        ).collect(Collectors.toList());

    Builder treeBuilder = AttributeValue.builder().l(maps);

    addEntitySafe(TREE, treeBuilder);

    return buildValuesMap(entity);
  }

  static class Producer implements DynamoProducer<L3> {
    @Override
    public L3 deserialize(Map<String, AttributeValue> entity) {
      L3.Builder builder = L3.builder()
          .id(deserializeId(entity));

      if (entity.containsKey(TREE)) {
        // TODO is this correct ??
        for (AttributeValue mapValue : entity.get(TREE).l()) {
          Map<String, AttributeValue> map = mapValue.m();
          Key key = deserializeKey(map.get(TREE_KEY));
          Id id = deserializeId(map.get(TREE_ID));
          builder.addKeyDelta(key, id);
        }
      }

      return builder.build();
    }
  }
}
