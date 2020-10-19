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
import java.util.stream.Stream;

import com.dremio.nessie.versioned.impl.KeyMutation.KeyAddition;
import com.dremio.nessie.versioned.impl.KeyMutation.KeyRemoval;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

class L3 extends MemoizedId {

  private static final long HASH_SEED = 4604180344422375655L;

  private final TreeMap<InternalKey, PositionDelta> map;

  static L3 EMPTY = new L3(new TreeMap<>());
  static Id EMPTY_ID = EMPTY.getId();

  private L3(TreeMap<InternalKey, PositionDelta> keys) {
    this(null, keys);
  }

  private L3(Id id, TreeMap<InternalKey, PositionDelta> keys) {
    super(id);
    this.map = keys;
    ensureConsistentId();
  }

  public Id getId(InternalKey key) {
    PositionDelta delta = map.get(key);
    if (delta == null) {
      return Id.EMPTY;
    }
    return delta.getNewId();
  }

  /**
   * Get the key if it exists.
   * @param id The id of the key to retrieve
   * @return If the key exists, provide. Else, provide Optional.empty()
   */
  Optional<Id> getPossibleId(InternalKey key) {
    Id id = getId(key);
    if (Id.EMPTY.equals(id)) {
      return Optional.empty();
    }
    return Optional.of(id);
  }

  @SuppressWarnings("unchecked")
  L3 set(InternalKey key, Id valueId) {
    TreeMap<InternalKey, PositionDelta> newMap = (TreeMap<InternalKey, PositionDelta>) map.clone();
    PositionDelta newDelta = newMap.get(key);
    if (newDelta == null) {
      newDelta = PositionDelta.EMPTY_ZERO;
    }

    newDelta = ImmutablePositionDelta.builder().from(newDelta).newId(valueId).build();
    if (!newDelta.isDirty()) {
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
      hasher.putLong(HASH_SEED);
      map.forEach((key, delta) -> {
        if (delta.getNewId().isEmpty()) {
          return;
        }

        InternalKey.addToHasher(key, hasher);
        hasher.putBytes(delta.getNewId().getValue().asReadOnlyByteBuffer());
      });
    });
  }


  static final SimpleSchema<L3> SCHEMA = new SimpleSchema<L3>(L3.class) {

    private static final String ID = "id";
    private static final String TREE = "tree";
    private static final String TREE_KEY = "key";
    private static final String TREE_ID = "id";

    @Override
    public L3 deserialize(Map<String, AttributeValue> attributeMap) {
      TreeMap<InternalKey, PositionDelta> tree = attributeMap.get(TREE).l().stream().map(av -> av.m()).collect(Collectors.toMap(
          m -> InternalKey.fromAttributeValue(m.get(TREE_KEY)),
          m -> PositionDelta.of(0, Id.fromAttributeValue(m.get(TREE_ID))),
          (a,b) -> {
            throw new UnsupportedOperationException();
          },
          TreeMap::new));

      return new L3(
          Id.fromAttributeValue(attributeMap.get(ID)),
          tree
      );
    }

    @Override
    public Map<String, AttributeValue> itemToMap(L3 item, boolean ignoreNulls) {
      List<AttributeValue> values = item.map.entrySet().stream()
          .filter(e -> !e.getValue().getNewId().isEmpty())
          .map(e -> {
            InternalKey key = e.getKey();
            PositionDelta pm = e.getValue();
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

  Stream<KeyMutation> getMutations() {
    return map.entrySet().stream().filter(e -> e.getValue().wasAddedOrRemoved())
        .map(e -> {
          PositionDelta d = e.getValue();
          if (d.wasAdded()) {
            return KeyAddition.of(e.getKey());
          } else if (d.wasRemoved()) {
            return KeyRemoval.of(e.getKey());
          } else {
            throw new IllegalStateException("This list should have been filtered to only items that were either added or removed.");
          }
        });
  }

  Stream<InternalKey> getKeys() {
    return map.keySet().stream();
  }

  /**
   * return the number of keys defined.
   * @return
   */
  int size() {
    return map.size();
  }

  public static MapDifference<InternalKey, Id> compare(L3 from, L3 to) {
    MapDifference<InternalKey, Id> comparison =  Maps.difference(
        Maps.transformValues(from.map, p -> p.getNewId()),
        Maps.transformValues(to.map, p -> p.getNewId())
        );
    return comparison;
  }
}
