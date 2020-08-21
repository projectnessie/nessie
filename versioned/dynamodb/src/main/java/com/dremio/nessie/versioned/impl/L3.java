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
package com.dremio.nessie.versioned.impl;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;

import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

class L3 extends MemoizedId {

  private final TreeMap<InternalKey, PositionMutation> map;

  static L3 EMPTY = new L3(new TreeMap<>());
  static Id EMPTY_ID = EMPTY.getId();

  public L3() {
    this.map = new TreeMap<>();
  }

  private L3(TreeMap<InternalKey, PositionMutation> keys) {
    this(null, keys);
  }

  private L3(Id id, TreeMap<InternalKey, PositionMutation> keys) {
    super(id);
    this.map = keys;
    ensureConsistentId();
  }

  public Id getId(InternalKey key) {
    PositionMutation delta = map.get(key);
    if(delta == null) {
      return Id.EMPTY;
    }
    return delta.getNewId();
  }

  /**
   * Get the key if it exists.
   * @param id
   * @return If the key exists, provide. Else, provide Optional.empty()
   */
  Optional<Id> getPossibleId(InternalKey key) {
    Id id = getId(key);
    if(Id.EMPTY.equals(id)) {
      return Optional.empty();
    }
    return Optional.of(id);
  }

  @SuppressWarnings("unchecked")
  L3 set(InternalKey key, Id valueId) {
    TreeMap<InternalKey, PositionMutation> newMap = (TreeMap<InternalKey, PositionMutation>) map.clone();
    PositionMutation newDelta = newMap.get(key);
    if(newDelta == null) {
      newDelta = PositionMutation.EMPTY_ZERO;
    }

    newDelta = ImmutablePositionMutation.builder().from(newDelta).newId(valueId).build();
    if(!newDelta.isDirty()) {
      // this turned into a no-op delta, remove it entirely from the map.
      newMap.remove(key);
    } else {
      newMap.put(key, newDelta);
    }
    return new L3(newMap);
  }

  /**
   * An Id constructed of the key + id in sorted order.
   */
  @Override
  Id generateId() {
    return Id.build(hasher -> {
      map.forEach((key, delta) -> {
        if(delta.getNewId().isEmpty()) {
          return;
        }

        InternalKey.addToHasher(key, hasher);
        hasher.putBytes(delta.getNewId().getValue().asReadOnlyByteBuffer());
      });
    });
  }


  static final TableSchema<L3> SCHEMA = new SimpleSchema<L3>(L3.class) {

    private static final String ID = "id";
    private static final String TREE = "tree";
    private static final String TREE_KEY = "key";
    private static final String TREE_ID = "id";

    @Override
    public L3 deserialize(Map<String, AttributeValue> attributeMap) {
      TreeMap<InternalKey, PositionMutation> tree = attributeMap.get(TREE).l().stream().map(av -> av.m()).collect(Collectors.toMap(
              m -> InternalKey.fromAttributeValue(m.get(TREE_KEY)),
              m -> PositionMutation.of(0, Id.fromAttributeValue(m.get(TREE_ID))),
              (a,b) -> {throw new UnsupportedOperationException();},
              TreeMap::new));

      return new L3(
          Id.fromAttributeValue(attributeMap.get(ID)),
          tree
      );
    }

    @Override
    public Map<String, AttributeValue> itemToMap(L3 item, boolean ignoreNulls) {
      List<AttributeValue> values = item.map.entrySet().stream().map(e -> {
        InternalKey key = e.getKey();
        PositionMutation pm = e.getValue();
        Map<String, AttributeValue> pmm = ImmutableMap.of(
            TREE_KEY, key.toAttributeValue(),
            TREE_ID, pm.getNewId().toAttributeValue());
        return AttributeValue.builder().m(pmm).build();
      }).collect(Collectors.toList());
      return ImmutableMap.<String, AttributeValue>builder()
          .put(TREE, AttributeValue.builder().l(values).build())
          .put(ID, item.getId().toAttributeValue())
          .build();
    }

  };
}
