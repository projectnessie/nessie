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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import com.dremio.nessie.tiered.builder.L1Consumer;
import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.impl.KeyList.CompleteList;
import com.dremio.nessie.versioned.impl.KeyList.IncrementalList;
import com.dremio.nessie.versioned.impl.KeyMutation.KeyAddition;
import com.dremio.nessie.versioned.impl.KeyMutation.KeyRemoval;
import com.dremio.nessie.versioned.store.Entity;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.SimpleSchema;
import com.dremio.nessie.versioned.store.Store;
import com.dremio.nessie.versioned.store.ValueType;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

public class L1 extends MemoizedId implements Persistent<L1Consumer<?>> {

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

    if (tree.size() != SIZE) {
      throw new AssertionError("tree.size(" + tree.size() + ") != " + SIZE);
    }
    if (id != null && !id.equals(generateId())) {
      throw new AssertionError("wrong id=" + id + ", expected=" + generateId());
    }
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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    L1 l1 = (L1) o;

    return Objects.equals(tree, l1.tree)
        && Objects.equals(metadataId, l1.metadataId)
        && Objects.equals(keyList, l1.keyList)
        && Objects.equals(parentList, l1.parentList);
  }

  @Override
  public int hashCode() {
    int result = tree != null ? tree.hashCode() : 0;
    result = 31 * result + (metadataId != null ? metadataId.hashCode() : 0);
    result = 31 * result + (keyList != null ? keyList.hashCode() : 0);
    result = 31 * result + (parentList != null ? parentList.hashCode() : 0);
    return result;
  }

  @Override
  public ValueType type() {
    return ValueType.L1;
  }

  /**
   * TODO Javadoc for checkstyle.
   * TODO Needs to be pulled up into {@link com.dremio.nessie.versioned.store.HasId}.
   */
  @Override
  public L1Consumer<?> applyToConsumer(L1Consumer<?> consumer) {
    consumer.id(this.getId());
    consumer.commitMetadataId(this.metadataId);
    //todo likely we want to change this interface to InternalKey

    for (KeyMutation mutation : this.keyList.getMutations()) {
      Key key = mutation.getKey().toKey();
      if (mutation instanceof KeyAddition) {
        consumer.addKeyAddition(key);
      } else if (mutation instanceof KeyRemoval) {
        consumer.addKeyRemoval(key);
      }
    }

    ArrayList<Id> children = new ArrayList<>();
    this.tree.iterator().forEachRemaining(children::add);
    consumer.children(children);

    consumer.addAncestors(parentList.getParents());

    if (keyList.isFull()) {
      consumer.completeKeyList(((CompleteList)keyList).getFragments());
    } else {
      IncrementalList list = (IncrementalList) keyList;
      consumer.incrementalKeyList(list.getPreviousCheckpoint(), list.getDistanceFromCheckpointCommits());
    }

    return consumer;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder implements L1Consumer<Builder> {

    private Id metadataId;
    private List<Id> ancestors;
    private List<Id> children;
    private final List<KeyMutation> keyChanges = new ArrayList<>();
    private Id id;
    private Id checkpointId;
    private int distanceFromCheckpoint;
    private List<Id> fragmentIds;

    private Builder() {
      // empty
    }

    /**
     * Built the {@link L1}.
     */
    public L1 build() {
      return new L1(
          metadataId,
          IdMap.of(children),
          id,
          buildKeyList(),
          ParentList.of(ancestors));
    }

    private KeyList buildKeyList() {
      if (checkpointId != null) {
        return KeyList.incremental(checkpointId, keyChanges, distanceFromCheckpoint);
      }
      if (fragmentIds != null) {
        return new KeyList.CompleteList(fragmentIds, keyChanges);
      }
      // todo what if its neither? Fail
      throw new IllegalStateException("Neither a checkpoint nor a incremental key list were found.");
    }

    @Override
    public Builder commitMetadataId(Id id) {
      checkCalled(this.metadataId, "commitMetadataId");
      this.metadataId = id;
      return this;
    }

    @Override
    public Builder addAncestors(List<Id> ids) {
      checkCalled(this.ancestors, "addAncestors");
      this.ancestors = ids;
      return this;
    }

    @Override
    public Builder children(List<Id> ids) {
      checkCalled(this.children, "children");
      this.children = ids;
      return this;
    }

    @Override
    public Builder id(Id id) {
      checkCalled(this.id, "id");
      this.id = id;
      return this;
    }

    @Override
    public Builder addKeyAddition(Key key) {
      keyChanges.add(KeyMutation.KeyAddition.of(new InternalKey(key)));
      return this;
    }

    @Override
    public Builder addKeyRemoval(Key key) {
      keyChanges.add(KeyMutation.KeyRemoval.of(new InternalKey(key)));
      return this;
    }

    @Override
    public Builder incrementalKeyList(Id checkpointId, int distanceFromCheckpoint) {
      checkCalled(this.checkpointId, "incrementalKeyList");
      if (this.fragmentIds != null) {
        throw new UnsupportedOperationException("Cannot call incrementalKeyList after completeKeyList.");
      }
      this.checkpointId = checkpointId;
      this.distanceFromCheckpoint = distanceFromCheckpoint;
      return this;
    }

    @Override
    public Builder completeKeyList(List<Id> fragmentIds) {
      checkCalled(this.fragmentIds, "completeKeyList");
      if (this.checkpointId != null) {
        throw new UnsupportedOperationException("Cannot call completeKeyList after incrementalKeyList.");
      }
      this.fragmentIds = fragmentIds;
      return this;
    }

    private static void checkCalled(Object arg, String name) {
      Preconditions.checkArgument(arg == null, String.format("Cannot call %s more than once", name));
    }
  }

}
