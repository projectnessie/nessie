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

import com.google.common.collect.ImmutableMap;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

class L1 extends MemoizedId {

  private static final long HASH_SEED = 3506039963025592061L;

  static final int SIZE = 151;
  static L1 EMPTY = new L1(Id.EMPTY, Id.EMPTY, new IdMap(SIZE, L2.EMPTY_ID));
  static Id EMPTY_ID = EMPTY.getId();

  private final IdMap tree;

  private final Id metadataId;
  private final Id parentId;

  L1(Id metadataId, Id parentId) {
    this.metadataId = metadataId;
    this.parentId = parentId;
    tree = new IdMap(SIZE);
  }

  L1(Id commitId, Id parentId, IdMap map) {
    this(commitId, parentId, map, null);
  }

  private L1(Id commitId, Id parentId, IdMap tree, Id id) {
    super(id);
    this.metadataId = commitId;
    this.parentId = parentId;
    this.tree = tree;

    assert tree.size() == SIZE;
    assert id == null || id.equals(generateId());
  }

  Id getId(int position) {
    return tree.getId(position);
  }

  Id getMetadataId() {
    return metadataId;
  }

  Id getParentId() {
    return parentId;
  }

  L1 set(int position, Id l2Id) {
    return new L1(metadataId, parentId, tree.setId(position, l2Id), null);
  }

  @Override
  Id generateId() {
    return Id.build(h -> {
      h.putLong(HASH_SEED)
        .putBytes(metadataId.getValue().asReadOnlyByteBuffer())
          .putBytes(parentId.getValue().asReadOnlyByteBuffer());
      tree.forEach(id -> h.putBytes(id.getValue().asReadOnlyByteBuffer()));
    });
  }

  IdMap getMap() {
    return tree;
  }

  public List<PositionDelta> getChanges() {
    return tree.getChanges();
  }

  static final SimpleSchema<L1> SCHEMA = new SimpleSchema<L1>(L1.class) {

    private static final String ID = "id";
    private static final String TREE = "tree";
    private static final String METADATA = "metadata";
    private static final String PARENT = "parent";

    @Override
    public L1 deserialize(Map<String, AttributeValue> attributeMap) {
      return new L1(
          Id.fromAttributeValue(attributeMap.get(METADATA)),
          Id.fromAttributeValue(attributeMap.get(PARENT)),
          IdMap.fromAttributeValue(attributeMap.get(TREE), SIZE),
          Id.fromAttributeValue(attributeMap.get(ID))
      );
    }

    @Override
    public Map<String, AttributeValue> itemToMap(L1 item, boolean ignoreNulls) {
      return ImmutableMap.<String, AttributeValue>builder()
          .put(TREE, item.tree.toAttributeValue())
          .put(ID, item.getId().toAttributeValue())
          .put(METADATA, item.metadataId.toAttributeValue())
          .put(PARENT, item.parentId.toAttributeValue())
          .build();
    }

  };

}
