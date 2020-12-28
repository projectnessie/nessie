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

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.dremio.nessie.versioned.Serializer;
import com.dremio.nessie.versioned.impl.InternalBranch.Commit;
import com.dremio.nessie.versioned.impl.InternalBranch.UnsavedDelta;
import com.dremio.nessie.versioned.impl.InternalKey.Position;
import com.dremio.nessie.versioned.impl.InternalRef.Type;
import com.dremio.nessie.versioned.impl.condition.ConditionExpression;
import com.dremio.nessie.versioned.impl.condition.ExpressionFunction;
import com.dremio.nessie.versioned.impl.condition.ExpressionPath;
import com.dremio.nessie.versioned.impl.condition.SetClause;
import com.dremio.nessie.versioned.impl.condition.UpdateExpression;
import com.dremio.nessie.versioned.store.Entity;
import com.dremio.nessie.versioned.store.HasId;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.LoadOp;
import com.dremio.nessie.versioned.store.LoadStep;
import com.dremio.nessie.versioned.store.SaveOp;
import com.dremio.nessie.versioned.store.SimpleSchema;
import com.dremio.nessie.versioned.store.ValueType;
import com.google.common.base.Preconditions;
import com.google.common.collect.Streams;

/**
 * Holds the portion of the commit tree structure that is necessary to manipulate the identified key(s).
 * Holds the information for a Tag, Hash or Branch.
 *
 * <p>If pointing to a branch, also provides mutability to allow updates and then commits of those updates.
 *
 * <p>Supports a collated loading model that will minimize the number of LoadSteps required to
 * populate the tree from the underlying storage.
 *
 */
class PartialTree<V> {

  public static enum LoadType {
    NO_VALUES, SELECT_VALUES;
  }

  private final Serializer<V> serializer;
  private final InternalRefId refId;
  private InternalRef.Type refType;
  private Id rootId;
  private Pointer<L1> l1;
  private final Map<Integer, Pointer<L2>> l2s = new HashMap<>();
  private final Map<Position, Pointer<L3>> l3s = new HashMap<>();
  private final Map<InternalKey, ValueHolder<V>> values = new HashMap<>();
  private final Collection<InternalKey> keys;

  static <V> PartialTree<V> of(Serializer<V> serializer, InternalRefId id, List<InternalKey> keys) {
    return new PartialTree<V>(serializer, id, keys);
  }

  static <V> PartialTree<V> of(Serializer<V> serializer, InternalRef.Type refType, L1 l1, Collection<InternalKey> keys) {
    PartialTree<V> tree = new PartialTree<V>(serializer, InternalRefId.ofHash(l1.getId()), keys);
    tree.l1 = new Pointer<L1>(l1);
    tree.refType = refType;
    return tree;
  }

  private void checkMutable() {
    Preconditions.checkArgument(refType == Type.BRANCH,
        "You can only mutate a partial tree that references a branch. This is type %s.", refType.name());
  }

  private PartialTree(Serializer<V> serializer, InternalRefId refId, Collection<InternalKey> keys) {
    super();
    this.refId = refId;
    this.serializer = serializer;
    this.keys = keys;
  }

  public LoadStep getLoadChain(Function<InternalBranch, L1> l1Converter, LoadType loadType) {
    if (refId.getType() == Type.HASH && l1 == null) {
      rootId = refId.getId();
      refType = Type.HASH;
      return getLoadStep1(loadType).get();
    }

    if (l1 != null) {
      return getLoadStep1(loadType).get();
    }

    LoadOp<InternalRef> op = new LoadOp<InternalRef>(ValueType.REF, refId.getId(), loadedRef -> {
      refType = loadedRef.getType();
      if (loadedRef.getType() == Type.BRANCH) {
        L1 loaded = l1Converter.apply(loadedRef.getBranch());
        l1 = new Pointer<L1>(loaded);
        rootId = loaded.getId();
      } else if (loadedRef.getType() == Type.TAG) {
        rootId = loadedRef.getTag().getCommit();
      } else {
        throw new IllegalStateException("Unknown type of ref to be loaded from store.");
      }
    });
    return new LoadStep(java.util.Collections.singleton(op), () -> getLoadStep1(loadType));
  }

  public L1 getCurrentL1() {
    return l1.get();
  }

  /**
   * Gets value, l3 and l2 save ops. These ops are all non-conditional.
   * @return
   */
  public Stream<SaveOp<?>> getMostSaveOps() {
    checkMutable();
    return Streams.<SaveOp<?>>concat(
        l2s.values().stream().filter(Pointer::isDirty).map(l2p -> new SaveOp<L2>(ValueType.L2, l2p.get())).distinct(),
        l3s.values().stream().filter(Pointer::isDirty).map(l3p -> new SaveOp<L3>(ValueType.L3, l3p.get())).distinct(),
        values.values().stream().map(v -> new SaveOp<WrappedValueBean>(ValueType.VALUE, v.getPersistentValue())).distinct()
        );
  }

  /**
   * Gets L1 mutations required to save tree.
   *
   * @return
   */
  public CommitOp getCommitOp(Id metadataId, Collection<InternalKey> unchangedKeys,
      boolean includeTreeUpdates,
      boolean includeCommitUpdates) {
    checkMutable();

    UpdateExpression treeUpdate = UpdateExpression.initial();

    // record positions that we're checking so we don't add the same positional check twice (for unchanged statements).
    final Set<Integer> conditionPositions = new HashSet<>();

    List<UnsavedDelta> deltas = new ArrayList<>();

    ConditionExpression treeCondition = ConditionExpression.of(
        ExpressionFunction.equals(ExpressionPath.builder(InternalRef.TYPE).build(), InternalRef.Type.BRANCH.toEntity()));

    // for all mutations that are dirty, create conditional and update expressions.
    for (PositionDelta pm : l1.get().getChanges()) {
      boolean added = conditionPositions.add(pm.getPosition());
      assert added;
      ExpressionPath p = ExpressionPath.builder(InternalBranch.TREE).position(pm.getPosition()).build();
      if (includeTreeUpdates) {
        treeUpdate = treeUpdate.and(SetClause.equals(p, pm.getNewId().toEntity()));
        treeCondition = treeCondition.and(ExpressionFunction.equals(p, pm.getOldId().toEntity()));
      }
      deltas.add(pm.toUnsavedDelta());
    }

    for (InternalKey unchanged : unchangedKeys) {
      int position = unchanged.getL1Position();
      if (includeTreeUpdates && conditionPositions.add(position)) {
        // this doesn't already have a condition. Add one.
        ExpressionPath p = ExpressionPath.builder(InternalBranch.TREE).position(position).build();
        treeCondition = treeCondition.and(ExpressionFunction.equals(p, getCurrentL1().getId(position).toEntity()));
      }
    }

    Commit commitIntention = null;
    if (includeCommitUpdates) {
      // Add the new commit
      commitIntention = new Commit(Id.generateRandom(), metadataId, deltas,
          KeyMutationList.of(l3s.values().stream().map(Pointer::get).flatMap(L3::getMutations).collect(Collectors.toList())));
    }

    return new CommitOp(
        includeCommitUpdates ? commitIntention : null,
        includeTreeUpdates ? treeUpdate : null,
        includeTreeUpdates ? treeCondition : null);
  }

  public static class CommitOp  {
    private final Commit commitIntention;
    private final UpdateExpression treeUpdate;
    private final ConditionExpression treeCondition;

    public CommitOp(Commit commitIntention, UpdateExpression treeUpdate, ConditionExpression condition) {
      super();
      this.commitIntention = commitIntention;
      this.treeUpdate = treeUpdate;
      this.treeCondition = condition;
    }

    public UpdateExpression getTreeUpdate() {
      return Preconditions.checkNotNull(treeUpdate);
    }

    public UpdateExpression getUpdateWithCommit() {
      return getTreeUpdate().and(getCommitSet(Collections.singletonList(commitIntention)));
    }

    public Commit getCommitIntention() {
      return Preconditions.checkNotNull(commitIntention);
    }

    public ConditionExpression getTreeCondition() {
      return Preconditions.checkNotNull(treeCondition);
    }

    static SetClause getCommitSet(List<Commit> commits) {
      return SetClause.appendToList(
          ExpressionPath.builder(InternalBranch.COMMITS).build(),
          Entity.ofList(commits.stream().map(Commit::toEntity)));
    }
  }

  private Optional<LoadStep> getLoadStep1(LoadType loadType) {
    final Supplier<Optional<LoadStep>> loadFunc = () -> getLoadStep2(loadType == LoadType.SELECT_VALUES);

    if (l1 != null) { // if we loaded a branch, we were able to pre-populate the l1 information.
      return loadFunc.get();
    }

    LoadOp<L1> op = new LoadOp<L1>(ValueType.L1, rootId, l -> {
      l1 = new Pointer<L1>(l);
    });
    return Optional.of(new LoadStep(java.util.Collections.singleton(op), loadFunc));
  }

  private Optional<LoadStep> getLoadStep2(boolean includeValues) {
    Collection<LoadOp<?>> loads = keys.stream()
        .map(id -> {
          Id l2Id = l1.get().getId(id.getL1Position());
          return new LoadOp<L2>(ValueType.L2, l2Id, l -> {
            l2s.putIfAbsent(id.getL1Position(), new Pointer<L2>(l));
          });
        })
        .collect(Collectors.toList());

    return Optional.of(new LoadStep(loads, (Supplier<Optional<LoadStep>>) (() -> getLoadStep3(includeValues))));
  }

  private Optional<LoadStep> getLoadStep3(boolean includeValues) {
    Collection<LoadOp<?>> loads = keys.stream().map(keyId -> {
      L2 l2 = l2s.get(keyId.getL1Position()).get();
      Id l3Id = l2.getId(keyId.getL2Position());
      return new LoadOp<L3>(ValueType.L3, l3Id, l -> l3s.putIfAbsent(keyId.getPosition(), new Pointer<L3>(l)));
    }).collect(Collectors.toList());
    return Optional.of(new LoadStep(loads, () -> getLoadStep4(includeValues)));
  }

  private Optional<LoadStep> getLoadStep4(boolean includeValues) {
    if (!includeValues) {
      return Optional.empty();
    }
    Collection<LoadOp<?>> loads = keys.stream().map(
        key -> {
          L3 l3 = l3s.get(key.getPosition()).get();
          Id id = l3.getId(key);
          if (id.isEmpty()) {
            // no load needed for empty values.
            return null;
          }
          return new LoadOp<InternalValue>(ValueType.VALUE, l3.getId(key),
              (wvb) -> {
                values.putIfAbsent(key, ValueHolder.of(serializer, wvb));
              });
      }).filter(n -> n != null)
        .collect(Collectors.toList());
    return Optional.of(new LoadStep(loads, () -> Optional.empty()));
  }

  public Optional<Id> getValueIdForKey(InternalKey key) {
    return l3s.get(key.getPosition()).get().getPossibleId(key);
  }

  public Optional<V> getValueForKey(InternalKey key) {
    ValueHolder<V> vh = values.get(key);
    if (vh == null) {
      return Optional.empty();
    }

    return Optional.of(vh.getValue());
  }

  /**
   * Set operation that doesn't store values.
   *
   * <p>This should be used in operations like merge and
   * cherry-pick, when we know that the values are already stored.
   *
   * @param key The key to set.
   * @param id The value or empty to set.
   */
  public void setValueIdForKey(InternalKey key, Optional<Id> id) {
    checkMutable();
    final Pointer<L1> l1 = this.l1;
    final Pointer<L2> l2 = l2s.get(key.getL1Position());
    final Pointer<L3> l3 = l3s.get(key.getPosition());

    // now we'll do the save.
    Id valueId;
    if (id.isPresent()) {
      valueId = id.get();
    } else {
      values.remove(key);
      valueId = Id.EMPTY;
    }

    final Id newL3Id = l3.apply(l -> l.set(key, valueId));
    final Id newL2Id = l2.apply(l -> l.set(key.getL2Position(), newL3Id));
    l1.apply(l -> l.set(key.getL1Position(), newL2Id));
  }

  public void setValueForKey(InternalKey key, Optional<V> value) {
    checkMutable();
    final Pointer<L1> l1 = this.l1;
    final Pointer<L2> l2 = l2s.get(key.getL1Position());
    final Pointer<L3> l3 = l3s.get(key.getPosition());

    // now we'll do the save.
    Id valueId;
    if (value.isPresent()) {
      ValueHolder<V> holder = ValueHolder.of(serializer,  value.get());
      values.put(key, holder);
      valueId = holder.getId();
    } else {
      values.remove(key);
      valueId = Id.EMPTY;
    }

    final Id newL3Id = l3.apply(l -> l.set(key, valueId));
    final Id newL2Id = l2.apply(l -> l.set(key.getL2Position(), newL3Id));
    l1.apply(l -> l.set(key.getL1Position(), newL2Id));
  }

  public Stream<InternalKey> getRetrievedKeys() {
    return l3s.values().stream().flatMap(p -> p.get().getKeys());
  }


  public PartialTree<V> cleanClone() {
    PartialTree<V> clone = new PartialTree<V>(serializer, refId, keys);
    this.l3s.entrySet().stream()
            .map(x -> new SimpleImmutableEntry<>(x.getKey(), cloneInner(L3.SCHEMA, x.getValue())))
            .forEach(x -> clone.l3s.put(x.getKey(), x.getValue()));
    this.l2s.entrySet().stream()
            .map(x -> new SimpleImmutableEntry<>(x.getKey(), cloneInner(L2.SCHEMA, x.getValue())))
            .forEach(x -> clone.l2s.put(x.getKey(), x.getValue()));
    this.values.forEach(clone.values::put);
    clone.refType = this.refType;
    clone.rootId = this.rootId;
    clone.l1 = cloneInner(L1.SCHEMA, this.l1);
    return clone;
  }

  private static <T extends HasId> Pointer<T> cloneInner(SimpleSchema<T> schema, Pointer<T> value) {
    if (value.isDirty()) {
      return new Pointer<>(schema.mapToItem(schema.itemToMap(value.get(), true)));
    } else {
      return value;
    }
  }
}
