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
import static com.dremio.nessie.versioned.store.dynamo.DynamoConstants.ID;
import static com.dremio.nessie.versioned.store.dynamo.DynamoConstants.TREE;
import static java.util.stream.Collectors.toList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import com.dremio.nessie.tiered.builder.L3Consumer;
import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.impl.L3;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.ValueType;
import com.google.protobuf.ByteString;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue.Builder;

class DynamoL3Consumer implements L3Consumer<DynamoL3Consumer>, DynamoConsumer<DynamoL3Consumer> {

  private final Map<String, Builder> entity;
  // TODO private final TreeMap<>

  DynamoL3Consumer() {
    entity = new HashMap<>();
    entity.put("t", string(ValueType.L3.getValueName()));
  }

  @Override
  public DynamoL3Consumer addKeyDelta(Key key, Id id) {
    System.err.println("addKeyDelta " + key + " " + id);
    return null;
  }

  @Override
  public DynamoL3Consumer id(Id id) {
    addEntitySafe(ID, bytes(id.getValue()));
    return this;
  }

  @Override
  public Map<String, AttributeValue> getEntity() {
    return buildValuesMap(entity);
  }

  private static Map<String, AttributeValue> buildValuesMap(Map<String, Builder> source) {
    return source.entrySet().stream()
        .collect(Collectors.toMap(
            Entry::getKey,
            e -> e.getValue().build()
        ));
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

  private static Builder idsList(List<Id> ids) {
    List<AttributeValue> idsList = ids.stream()
        .map(id -> bytes(id.getValue()).build())
        .collect(toList());
    return list(idsList);
  }

  private static Builder bytes(ByteString bytes) {
    return AttributeValue.builder().b(SdkBytes.fromByteBuffer(bytes.asReadOnlyByteBuffer()));
  }

  private static Builder string(String string) {
    return AttributeValue.builder().s(string);
  }

  private static Builder list(List<AttributeValue> list) {
    return AttributeValue.builder().l(list);
  }

  public static class Producer implements DynamoProducer<L3> {
    @Override
    public L3 deserialize(Map<String, AttributeValue> entity) {
      L3.Builder builder = L3.builder()
          .id(deserializeId(entity));

      if (entity.containsKey(TREE)) {
        throw new UnsupportedOperationException("Oh oh");
        /*
        builder.add
      TreeMap<InternalKey, PositionDelta> tree = attributeMap.get(TREE).getList().stream().map(av -> av.getMap()).collect(Collectors.toMap(
          m -> InternalKey.fromEntity(m.get(TREE_KEY)),
          m -> PositionDelta.of(0, Id.fromEntity(m.get(TREE_ID))),
          (a,b) -> {
            throw new UnsupportedOperationException();
          },
          TreeMap::new));

        builder = builder.children(deserializeIdList(entity.get(TREE)));
        */
      }

      return builder.build();
    }
  }
}
