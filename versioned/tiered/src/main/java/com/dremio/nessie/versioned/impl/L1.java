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
import java.util.stream.Stream;

import com.dremio.nessie.versioned.store.Entity;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.SimpleSchema;
import com.dremio.nessie.versioned.store.Store;
import com.google.common.collect.ImmutableMap;

public class L1 extends MemoizedId {

  private static final long HASH_SEED = 3506039963025592061L;

  public static final int SIZE = 43;
  public static L1 EMPTY = new L1(Id.EMPTY, new IdMap(SIZE, L2.EMPTY_ID), null, KeyList.EMPTY, ParentList.EMPTY);
  public static Id EMPTY_ID = EMPTY.getId();

  private final IdMap tree;

  private final Id metadataId;
  private final KeyList keyList;
  private final ParentList parentList;

  private L1(Id commitId, IdMap tree, Id id, KeyList keyList, ParentList parentList) {
    super(id);
    this.metadataId = commitId;
    this.parentList = parentList;
    this.keyList = keyList;
    this.tree = tree;

    assert tree.size() == SIZE;
    assert id == null || id.equals(generateId());
  }

  L1 getChildWithTree(Id metadataId, IdMap tree, KeyMutationList mutations) {
    KeyList keyList = this.keyList.plus(getId(), mutations.getMutations());
    ParentList parents = this.parentList.cloneWithAdditional(getId());
    return new L1(metadataId, tree, null, keyList, parents);
  }

  public L1 withCheckpointAsNecessary(Store store) {
    return keyList.createCheckpointIfNeeded(this, store).map(keylist -> new L1(metadataId, tree, null, keylist, parentList)).orElse(this);
  }

  Id getId(int position) {
    return tree.getId(position);
  }

  Id getMetadataId() {
    return metadataId;
  }

  ParentList getParentList() {
    return parentList;
  }

  Id getParentId() {
    return parentList.getParent();
  }

  L1 set(int position, Id l2Id) {
    return new L1(metadataId, tree.withId(position, l2Id), null, keyList, parentList);
  }

  @Override
  Id generateId() {
    return Id.build(h -> {
      h.putLong(HASH_SEED)
        .putBytes(metadataId.getValue().asReadOnlyByteBuffer())
          .putBytes(parentList.getParent().getValue().asReadOnlyByteBuffer());
      tree.forEach(id -> h.putBytes(id.getValue().asReadOnlyByteBuffer()));
    });
  }

  Stream<InternalKey> getKeys(Store store) {
    return keyList.getKeys(this, store);
  }

  IdMap getMap() {
    return tree;
  }

  public List<PositionDelta> getChanges() {
    return tree.getChanges();
  }

  public static final SimpleSchema<L1> SCHEMA = new SimpleSchema<L1>(L1.class) {

    private static final String ID = "id";
    private static final String TREE = "tree";
    private static final String METADATA = "metadata";
    private static final String PARENTS = "parents";
    private static final String KEY_LIST = "keys";

    @Override
    public L1 deserialize(Map<String, Entity> attributeMap) {
      return new L1(
          Id.fromEntity(attributeMap.get(METADATA)),
          IdMap.fromEntity(attributeMap.get(TREE), SIZE),
          Id.fromEntity(attributeMap.get(ID)),
          KeyList.fromEntity(attributeMap.get(KEY_LIST)),
          ParentList.fromEntity(attributeMap.get(PARENTS))
      );
    }

    @Override
    public Map<String, Entity> itemToMap(L1 item, boolean ignoreNulls) {
      return ImmutableMap.<String, Entity>builder()
          .put(METADATA, item.metadataId.toEntity())
          .put(TREE, item.tree.toEntity())
          .put(ID, item.getId().toEntity())
          .put(KEY_LIST, item.keyList.toEntity())
          .put(PARENTS, item.parentList.toEntity())
          .build();
    }

  };

  KeyList getKeyList() {
    return keyList;
  }

  /**
   * return the number of positions that are non-empty.
   * @return number of non-empty positions.
   */
  int size() {
    int count = 0;
    for (Id id : tree) {
      if (!id.equals(L2.EMPTY_ID)) {
        count++;
      }
    }
    return count;
  }

}
