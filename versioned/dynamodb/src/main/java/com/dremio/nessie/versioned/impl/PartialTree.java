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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.dremio.nessie.versioned.Serializer;
import com.dremio.nessie.versioned.impl.DynamoStore.ValueType;
import com.dremio.nessie.versioned.impl.InternalRef.Type;
import com.google.common.base.Preconditions;
import com.google.common.collect.Streams;

/**
 * Holds the portion of the commit tree structure that is necessary to manipulate the identified key(s). Holds the information for a Tag, Hash or Branch.
 *
 * If pointing to a branch, also provides mutability to allow updates and then commits of those updates.
 *
 * Supports a collated loading model that will minimize the number of LoadSteps required to populate the tree from the underlying storage.
 *
 */
class PartialTree<V> {

  private final Serializer<V> serializer;
  private final InternalRefId refId;
  private InternalRef ref;
  private Id rootId;
  private Pointer<L1> l1;
  private Map<Id, Pointer<L2>> l2s = new HashMap<>();
  private Map<Id, Pointer<L3>> l3s = new HashMap<>();
  private Map<InternalKey, ValueHolder<V>> values = new HashMap<>();
  private List<InternalKey> keys = new ArrayList<>();

  public static <V> PartialTree<V> of(Serializer<V> serializer, InternalRefId id, List<InternalKey> keys) {
    return new PartialTree<V>(serializer, id, keys);
  }

  private void checkMutable() {
    Preconditions.checkArgument(ref.getType() == Type.BRANCH, "You can only mutate a partial tree that references a branch. This is type %s.", ref.getType());
  }

  private PartialTree(Serializer<V> serializer, InternalRefId refId, List<InternalKey> keys) {
    super();
    this.refId = refId;
    this.serializer = serializer;
    this.keys = keys;
  }

  public LoadStep getLoadChain(Function<InternalBranch, L1> l1Converter, boolean includeValues) {
    if (refId.getType() == Type.HASH) {
      rootId = refId.getId();
      ref = InternalRef.of(refId.getId());
      return getLoadStep1(includeValues).get();
    }

    LoadOp<InternalRef> op = new LoadOp<InternalRef>(ValueType.REF, refId.getId(), loadedRef -> {
      ref = loadedRef;
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
    return new LoadStep(java.util.Collections.singleton(op), () -> getLoadStep1(includeValues));
  }

  public L1 getCurrentL1() {
    return l1.get();
  }

  /**
   * Gets value, l3 and l2 save ops. These ops are all non-conditional.
   * @return
   */
  public List<SaveOp<?>> getMostSaveOps() {
    checkMutable();
    return Streams.concat(
        l2s.values().stream().filter(Pointer::isDirty).map(l2p -> new SaveOp<L2>(ValueType.L2, l2p.get())),
        l3s.values().stream().filter(Pointer::isDirty).map(l3p -> new SaveOp<L3>(ValueType.L3, l3p.get())),
        values.values().stream().map(v -> new SaveOp<WrappedValueBean>(ValueType.VALUE, v.getPersistentValue())))
    .collect(Collectors.toList());
  }

  /**
   * Gets L1 mutations required to save tree.
   *
   * @return
   */
  public List<PositionMutation> getL1Mutations() {
    checkMutable();
    return l1.get().getChanges();
  }

  private Optional<LoadStep> getLoadStep1(boolean includeValues) {
    if(l1 != null) { // if we loaded a branch, we were able to prepopulate the l1 information.
      return getLoadStep2(includeValues);
    }

    LoadOp<L1> op = new LoadOp<L1>(ValueType.L1, rootId, l -> {l1 = new Pointer<L1>(l);});
    return Optional.of(new LoadStep(java.util.Collections.singleton(op), () -> getLoadStep2(includeValues)));
  }

  private Optional<LoadStep> getLoadStep2(boolean includeValues) {
    Collection<LoadOp<?>> loads = keys.stream()
        .map(id -> {
          Id l2Id = l1.get().getId(id.getL1Position());
          return new LoadOp<L2>(ValueType.L2, l2Id, l -> {
            l2s.putIfAbsent(l2Id, new Pointer<L2>(l));
          });
        })
        .collect(Collectors.toList());

    return Optional.of(new LoadStep(loads, (Supplier<Optional<LoadStep>>) (() -> getLoadStep3(includeValues))));
  }

  private Optional<LoadStep> getLoadStep3(boolean includeValues) {
    Collection<LoadOp<?>> loads = keys.stream().map(keyId -> {
      Id l2Id = l1.get().getId(keyId.getL1Position());
      L2 l2 = l2s.get(l2Id).get();
      Id l3Id = l2.getId(keyId.getL2Position());
      return new LoadOp<L3>(ValueType.L3, l3Id, l -> l3s.putIfAbsent(l3Id, new Pointer<L3>(l)));
    }).collect(Collectors.toList());
    return Optional.of(new LoadStep(loads, () -> getLoadStep4(includeValues)));
  }

  private Optional<LoadStep> getLoadStep4(boolean includeValues) {
    if(!includeValues) {
      return Optional.empty();
    }
    Collection<LoadOp<?>> loads = keys.stream().map(
        key -> {
          Id l2Id = l1.get().getId(key.getL1Position());
          L2 l2 = l2s.get(l2Id).get();
          Id l3Id = l2.getId(key.getL2Position());
          L3 l3 = l3s.get(l3Id).get();
          return new LoadOp<InternalValue>(ValueType.VALUE, l3.getId(key), (wvb) -> {values.putIfAbsent(key, ValueHolder.of(serializer, wvb));});
    }).collect(Collectors.toList());
    return Optional.of(new LoadStep(loads, () -> Optional.empty()));
  }

  public Optional<Id> getValueIdForKey(InternalKey key) {
    Id l2Id = l1.get().getId(key.getL1Position());
    Id l3Id = l2s.get(l2Id).get().getId(key.getL2Position());
    return l3s.get(l3Id).get().getPossibleId(key);
  }

  public Optional<V> getValueForKey(InternalKey key) {
    ValueHolder<V> vh = values.get(key);
    if(vh == null ) {
      return Optional.empty();
    }

    return Optional.of(vh.getValue());
  }

  public void setValueForKey(InternalKey key, Optional<V> value) {
    checkMutable();
    final Pointer<L1> l1 = this.l1;
    final Id l2Id = l1.get().getId(key.getL1Position());
    final Pointer<L2> l2 = l2s.get(l2Id);
    final Id l3Id = l2.get().getId(key.getL2Position());
    final Pointer<L3> l3 = l3s.get(l3Id);

    // now we'll do the save.
    Id valueId;
    if(value.isPresent()) {
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

}