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

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.dremio.nessie.tiered.builder.L3Consumer;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.KeyDelta;
import com.dremio.nessie.versioned.store.ValueType;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue.Builder;

class DynamoL3Consumer extends DynamoConsumer<L3Consumer> implements L3Consumer {

  static final String TREE = "tree";
  static final String TREE_KEY = "key";
  static final String TREE_ID = "id";

  DynamoL3Consumer() {
    super(ValueType.L3);
  }

  @Override
  public DynamoL3Consumer addKeyDelta(Stream<KeyDelta> keyDelta) {
    Stream<AttributeValue> maps = keyDelta.map(kd -> map(dualMap(
        TREE_KEY, keyList(kd.getKey()),
        TREE_ID, idValue(kd.getId())
    )).build());

    Builder treeBuilder = AttributeValue.builder().l(maps.collect(Collectors.toList()));
    addEntitySafe(TREE, treeBuilder);
    return this;
  }

  @Override
  public DynamoL3Consumer id(Id id) {
    addEntitySafe(ID, bytes(id.getValue()));
    return this;
  }

  @Override
  public boolean canHandleType(ValueType valueType) {
    return valueType == ValueType.L3;
  }

  static class Producer extends DynamoProducer<L3Consumer> {
    public Producer(Map<String, AttributeValue> entity) {
      super(entity);
    }

    @Override
    public void applyToConsumer(L3Consumer consumer) {
      consumer.id(deserializeId(entity));

      if (entity.containsKey(TREE)) {
        Stream<KeyDelta> keyDelta = entity.get(TREE).l().stream()
            .map(AttributeValue::m)
            .map(m -> KeyDelta.of(
                deserializeKey(m.get(TREE_KEY)),
                deserializeId(m.get(TREE_ID))
            ));

        consumer.addKeyDelta(keyDelta);
      }
    }
  }
}
