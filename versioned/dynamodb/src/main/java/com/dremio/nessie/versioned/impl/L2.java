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

import java.util.Map;

import com.google.common.collect.ImmutableMap;

import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

class L2 extends MemoizedId {

  static final int SIZE = 199;
  static L2 EMPTY = new L2(null, new IdMap(SIZE, L3.EMPTY_ID));
  static Id EMPTY_ID = EMPTY.getId();

  private final IdMap map;

  L2() {
    map = new IdMap(SIZE);
  }

  private L2(Id id, IdMap map) {
    super(id);
    assert map.size() == SIZE;
    this.map = map;
  }

  private L2(IdMap map) {
    this(null, map);
  }


  Id getId(int position) {
    return map.getId(position);
  }

  L2 set(int position, Id l2Id) {
    return new L2(map.setId(position, l2Id));
  }

  @Override
  Id generateId() {
    return Id.build(h -> {
      map.forEach(id -> h.putBytes(id.getValue().asReadOnlyByteBuffer()));
    });
  }

  static final TableSchema<L2> SCHEMA = new SimpleSchema<L2>(L2.class) {

    private static final String ID = "id";
    private static final String TREE = "tree";

    @Override
    public L2 deserialize(Map<String, AttributeValue> attributeMap) {
      return new L2(
          Id.fromAttributeValue(attributeMap.get(ID)),
          IdMap.fromAttributeValue(attributeMap.get(TREE), SIZE)
      );
    }

    @Override
    public Map<String, AttributeValue> itemToMap(L2 item, boolean ignoreNulls) {
      return ImmutableMap.<String, AttributeValue>builder()
          .put(TREE, item.map.toAttributeValue())
          .put(ID, item.getId().toAttributeValue())
          .build();
    }

  };
}
