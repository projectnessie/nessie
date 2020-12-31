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

import static java.util.stream.Collectors.toList;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.dremio.nessie.tiered.builder.HasIdConsumer;
import com.dremio.nessie.versioned.ImmutableKey;
import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.store.HasId;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.Store;
import com.dremio.nessie.versioned.store.ValueType;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue.Builder;
import software.amazon.awssdk.utils.builder.SdkBuilder;

abstract class DynamoConsumer<C extends HasIdConsumer<C>> extends DynamoConstants implements HasIdConsumer<C> {

  protected final Map<String, Builder> entity = new HashMap<>();

  DynamoConsumer(ValueType valueType) {
    entity.put("t", string(valueType.getValueName()));
  }

  void addIdList(String key, List<Id> ids) {
    addEntitySafe(key, idsList(ids));
  }

  void addEntitySafe(String key, Builder value) {
    Builder old = entity.put(key, value);
    if (old != null) {
      throw new IllegalStateException("Duplicate '" + key + "' in 'entity' map. Old={" + old + "} current={" + value + "}");
    }
  }

  abstract Map<String, AttributeValue> getEntity();

  /**
   * TODO add some javadoc.
   */
  @SuppressWarnings("unchecked")
  static <C extends DynamoConsumer<C>> DynamoConsumer<C> newConsumer(ValueType type) {
    switch (type) {
      case L1:
        return (DynamoConsumer<C>) new DynamoL1Consumer();
      case L2:
        return (DynamoConsumer<C>) new DynamoL2Consumer();
      case L3:
        return (DynamoConsumer<C>) new DynamoL3Consumer();
      case COMMIT_METADATA:
        return (DynamoConsumer<C>) new DynamoCommitMetadataConsumer();
      case VALUE:
        return (DynamoConsumer<C>) new DynamoValueConsumer();
      case REF:
        return (DynamoConsumer<C>) new DynamoRefConsumer();
      case KEY_FRAGMENT:
        return (DynamoConsumer<C>) new DynamoFragmentConsumer();
      default:
        throw new IllegalArgumentException("No DynamoConsumer implementation for " + type);
    }
  }

  /**
   * TODO add some javadoc.
   */
  @SuppressWarnings("unchecked")
  static <E extends HasId, P extends DynamoProducer<E>> P newProducer(ValueType type) {
    switch (type) {
      case L1:
        return (P) new DynamoL1Consumer.Producer();
      case L2:
        return (P) new DynamoL2Consumer.Producer();
      case L3:
        return (P) new DynamoL3Consumer.Producer();
      case COMMIT_METADATA:
        return (P) new DynamoCommitMetadataConsumer.Producer();
      case VALUE:
        return (P) new DynamoValueConsumer.Producer();
      case REF:
        return (P) new DynamoRefConsumer.Producer();
      case KEY_FRAGMENT:
        return (P) new DynamoFragmentConsumer.Producer();
      default:
        throw new IllegalArgumentException("No DynamoConsumer implementation for " + type);
    }
  }

  static AttributeValue keyList(Key key) {
    return list(key.getElements().stream()
        .map(elem -> string(elem).build()).collect(toList()))
        .build();
  }

  static Builder idsList(List<Id> ids) {
    List<AttributeValue> idsList = ids.stream()
        .map(DynamoConsumer::idValue)
        .collect(toList());
    return list(idsList);
  }

  static Map<String, AttributeValue> buildValuesMap(Map<String, AttributeValue.Builder> source) {
    return source.entrySet().stream()
        .collect(Collectors.toMap(
            Entry::getKey,
            e -> e.getValue().build()
        ));
  }

  static List<AttributeValue> buildValues(List<AttributeValue.Builder> source) {
    return source.stream()
        .map(SdkBuilder::build)
        .collect(toList());
  }

  static AttributeValue idValue(Id id) {
    return idBuilder(id).build();
  }

  static Builder idBuilder(Id id) {
    return bytes(id.getValue());
  }

  static Builder bytes(ByteString bytes) {
    return AttributeValue.builder().b(SdkBytes.fromByteBuffer(bytes.asReadOnlyByteBuffer()));
  }

  static Builder bool(boolean bool) {
    return AttributeValue.builder().bool(bool);
  }

  static Builder number(int number) {
    return AttributeValue.builder().n(Integer.toString(number));
  }

  static Builder string(String string) {
    return AttributeValue.builder().s(string);
  }

  static Builder list(List<AttributeValue> list) {
    return AttributeValue.builder().l(list);
  }

  static Builder singletonMap(String key, AttributeValue value) {
    return map(Collections.singletonMap(key, value));
  }

  static Map<String, AttributeValue> dualMap(String key1, AttributeValue value1, String key2, AttributeValue value2) {
    Map<String, AttributeValue> map = new HashMap<>();
    map.put(key1, value1);
    map.put(key2, value2);
    return map;
  }

  static Builder map(Map<String, AttributeValue> map) {
    return AttributeValue.builder().m(map);
  }

  static void checkCalled(Object arg, String name) {
    Preconditions.checkArgument(arg == null, String.format("Cannot call %s more than once", name));
  }

  static void checkSet(Object arg, String name) {
    Preconditions.checkArgument(arg != null, String.format("Must call %s", name));
  }

  static void deserializeKeyMutations(
      List<AttributeValue> mutations,
      Consumer<Key> addConsumer,
      Consumer<Key> removalConsumer
  ) {
    for (AttributeValue mutation : mutations) {
      Map<String, AttributeValue> m = mutation.m();
      if (m.size() > 2) {
        throw new IllegalStateException("Ugh - got a keys.mutations map like this: " + m);
      }
      AttributeValue raw = m.get(KEY_ADDITION);
      if (raw != null) {
        addConsumer.accept(deserializeKey(raw));
      }
      raw = m.get(KEY_REMOVAL);
      if (raw != null) {
        removalConsumer.accept(deserializeKey(raw));
      }
    }
  }

  static Key deserializeKey(AttributeValue raw) {
    ImmutableKey.Builder keyBuilder = ImmutableKey.builder();
    for (AttributeValue keyPart : raw.l()) {
      keyBuilder.addElements(keyPart.s());
    }
    return keyBuilder.build();
  }

  static List<Id> deserializeIdList(AttributeValue raw) {
    return raw.l()
        .stream()
        .map(DynamoConsumer::deserializeId)
        .collect(toList());
  }

  static Id deserializeId(Map<String, AttributeValue> item) {
    return deserializeId(item.get(Store.KEY_NAME));
  }

  static Id deserializeId(AttributeValue raw) {
    return Id.of(raw.b().asByteArrayUnsafe());
  }

  static int deserializeInt(AttributeValue v) {
    return Integer.parseInt(v.n());
  }

  static ByteString deserializeBytes(AttributeValue raw) {
    return ByteString.copyFrom(raw.b().asByteArrayUnsafe());
  }

  static AttributeValue serializeId(Id id) {
    return AttributeValue.builder().b(SdkBytes.fromByteBuffer(id.getValue().asReadOnlyByteBuffer())).build();
  }
}
