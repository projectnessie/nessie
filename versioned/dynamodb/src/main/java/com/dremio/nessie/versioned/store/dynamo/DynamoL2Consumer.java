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
import static com.dremio.nessie.versioned.store.dynamo.DynamoConstants.ID;
import static com.dremio.nessie.versioned.store.dynamo.DynamoConstants.TREE;
import static java.util.stream.Collectors.toList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import com.dremio.nessie.tiered.builder.L2Consumer;
import com.dremio.nessie.versioned.impl.L2;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.ValueType;
import com.google.protobuf.ByteString;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue.Builder;

class DynamoL2Consumer implements L2Consumer<DynamoL2Consumer>, DynamoConsumer<DynamoL2Consumer> {

  private final Map<String, Builder> entity;

  DynamoL2Consumer() {
    entity = new HashMap<>();
    entity.put("t", string(ValueType.L2.getValueName()));
  }

  @Override
  public DynamoL2Consumer children(List<Id> ids) {
    System.err.println("children " + ids);
    addIdList(TREE, ids);
    return this;
  }

  @Override
  public DynamoL2Consumer id(Id id) {
    addEntitySafe(ID, bytes(id.getValue()));
    return this;
  }

  @Override
  public Map<String, AttributeValue> getEntity() {
    return buildValuesMap(entity);
  }

  private static Map<String, AttributeValue> buildValuesMap(Map<String, AttributeValue.Builder> source) {
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

  public static class Producer implements DynamoProducer<L2> {
    @Override
    public L2 deserialize(Map<String, AttributeValue> entity) {
      L2.Builder builder = L2.builder()
          .id(deserializeId(entity));

      if (entity.containsKey(TREE)) {
        builder = builder.children(deserializeIdList(entity.get(TREE)));
      }

      return builder.build();
    }
  }
}
