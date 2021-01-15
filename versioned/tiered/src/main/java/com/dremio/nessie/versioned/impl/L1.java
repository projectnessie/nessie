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

import static com.dremio.nessie.versioned.impl.ValidationHelper.checkCalled;
import static com.dremio.nessie.versioned.impl.ValidationHelper.checkSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.dremio.nessie.tiered.builder.L1Consumer;
import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.impl.KeyList.IncrementalList;
import com.dremio.nessie.versioned.store.Id;

class L1 extends PersistentBase<L1Consumer> {

  private static final long HASH_SEED = 3506039963025592061L;

  static final int SIZE = 43;
  static L1 EMPTY = new L1(Id.EMPTY, new IdMap(SIZE, L2.EMPTY_ID), null, KeyList.EMPTY, ParentList.EMPTY);
  static Id EMPTY_ID = EMPTY.getId();

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

  L1 withCheckpointAsNecessary(EntityStore store) {
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

  Stream<InternalKey> getKeys(EntityStore store) {
    return keyList.getKeys(this, store);
  }

  IdMap getMap() {
    return tree;
  }

  List<PositionDelta> getChanges() {
    return tree.getChanges();
  }

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
    return Objects.hash(tree, metadataId, keyList, parentList);
  }

  @Override
  public L1Consumer applyToConsumer(L1Consumer consumer) {
    super.applyToConsumer(consumer)
        .commitMetadataId(this.metadataId)
        .keyMutations(this.keyList.getMutations().stream().map(KeyMutation::toMutation))
        .children(this.tree.stream())
        .ancestors(parentList.getParents().stream());

    if (keyList.isFull()) {
      consumer.completeKeyList(keyList.getFragments().stream());
    } else {
      IncrementalList list = (IncrementalList) keyList;
      consumer.incrementalKeyList(list.getPreviousCheckpoint(), list.getDistanceFromCheckpointCommits());
    }

    return consumer;
  }

  /**
   * Create a new {@link Builder} instance that implement
   * {@link L1Consumer} to build an {@link L1} object.
   *
   * @return new builder instance
   */
  static Builder builder() {
    return new Builder();
  }

  /**
   * Implements {@link L1Consumer} to build an {@link L1} object.
   */
  // Needs to be a package private class, otherwise class-initialization of ValueType fails with j.l.IllegalAccessError
  static final class Builder extends EntityBuilder<L1> implements L1Consumer {

    private Id metadataId;
    private Stream<Id> ancestors;
    private Stream<Id> children;
    private final List<KeyMutation> keyChanges = new ArrayList<>();
    private Id id;
    private Id checkpointId;
    private int distanceFromCheckpoint;
    private Stream<Id> fragmentIds;

    private Builder() {
      // empty
    }

    @Override
    public Builder commitMetadataId(Id id) {
      checkCalled(this.metadataId, "commitMetadataId");
      this.metadataId = id;
      return this;
    }

    @Override
    public Builder ancestors(Stream<Id> ids) {
      checkCalled(this.ancestors, "addAncestors");
      this.ancestors = ids;
      return this;
    }

    @Override
    public Builder children(Stream<Id> ids) {
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
    public L1Consumer keyMutations(Stream<Key.Mutation> keyMutations) {
      keyMutations.map(KeyMutation::fromMutation)
          .forEach(keyChanges::add);
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
    public Builder completeKeyList(Stream<Id> fragmentIds) {
      checkCalled(this.fragmentIds, "completeKeyList");
      if (this.checkpointId != null) {
        throw new UnsupportedOperationException("Cannot call completeKeyList after incrementalKeyList.");
      }
      this.fragmentIds = fragmentIds;
      return this;
    }

    @Override
    public L1 build() {
      // null-id is allowed (will be generated)
      checkSet(metadataId, "metadataId");
      checkSet(children, "children");
      checkSet(ancestors, "ancestors");

      return new L1(
          metadataId,
          children.collect(IdMap.collector(SIZE)),
          id,
          buildKeyList(),
          ParentList.of(ancestors));
    }

    private KeyList buildKeyList() {
      if (checkpointId != null) {
        return KeyList.incremental(checkpointId, keyChanges, distanceFromCheckpoint);
      }
      if (fragmentIds != null) {
        return new KeyList.CompleteList(
            fragmentIds.collect(Collectors.toList()),
            keyChanges);
      }
      // todo what if its neither? Fail
      throw new IllegalStateException("Neither a checkpoint nor a incremental key list were found.");
    }
  }

}
