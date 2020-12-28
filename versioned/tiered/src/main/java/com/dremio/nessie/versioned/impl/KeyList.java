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

import com.dremio.nessie.versioned.impl.KeyMutation.MutationType;
import com.dremio.nessie.versioned.store.Entity;
import com.dremio.nessie.versioned.store.HasId;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.SaveOp;
import com.dremio.nessie.versioned.store.Store;
import com.dremio.nessie.versioned.store.ValueType;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.immutables.value.Value.Immutable;

/** Interface and implementations related to managing the key list within Dynamo. */
abstract class KeyList {

  private static final String IS_CHECKPOINT = "chk";

  public static final KeyList EMPTY = new CompleteList(Collections.emptyList(), ImmutableList.of());

  static enum Type {
    INCREMENTAL,
    FULL
  }

  abstract KeyList plus(Id parent, List<KeyMutation> mutations);

  abstract Optional<KeyList> createCheckpointIfNeeded(L1 startingPoint, Store store);

  abstract Type getType();

  static IncrementalList incremental(
      Id previousCheckpointL1, List<KeyMutation> mutations, int distanceFromCheckpointCommits) {
    return ImmutableIncrementalList.builder()
        .previousCheckpoint(previousCheckpointL1)
        .distanceFromCheckpointCommits(distanceFromCheckpointCommits)
        .mutations(mutations)
        .build();
  }

  abstract Stream<InternalKey> getKeys(L1 startingPoint, Store store);

  abstract List<KeyMutation> getMutations();

  abstract Entity toEntity();

  static KeyList fromEntity(Entity value) {
    if (value.getMap().get(IS_CHECKPOINT).getBoolean()) {
      return CompleteList.fromEntity(value.getMap());
    } else {
      return IncrementalList.fromEntity(value.getMap());
    }
  }

  boolean isEmptyIncremental() {
    return getType() == Type.INCREMENTAL && ((IncrementalList) this).getMutations().isEmpty();
  }

  boolean isFull() {
    return getType() == Type.FULL;
  }

  @Immutable
  abstract static class IncrementalList extends KeyList {

    private static final String MUTATIONS = "mutations";
    private static final String ORIGIN = "origin";
    private static final String DISTANCE = "dist";

    public static final int MAX_DELTAS = 50;

    public abstract List<KeyMutation> getMutations();

    public abstract Id getPreviousCheckpoint();

    public abstract int getDistanceFromCheckpointCommits();

    @Override
    public KeyList plus(Id parent, List<KeyMutation> mutations) {
      return ImmutableIncrementalList.builder()
          .addAllMutations(mutations)
          .distanceFromCheckpointCommits(getDistanceFromCheckpointCommits() + 1)
          .previousCheckpoint(getPreviousCheckpoint())
          .build();
    }

    @Override
    public Optional<KeyList> createCheckpointIfNeeded(L1 startingPoint, Store store) {
      if (getDistanceFromCheckpointCommits() < MAX_DELTAS) {
        return Optional.empty();
      }

      return Optional.of(generateNewCheckpoint(startingPoint, store));
    }

    @Override
    Stream<InternalKey> getKeys(L1 startingPoint, Store store) {
      IterResult keys = getKeysIter(startingPoint, store);
      if (keys.isChanged()) {
        return keys.keyList;
      }

      return keys.list.getKeys(startingPoint, store);
    }

    private CompleteList generateNewCheckpoint(L1 startingPoint, Store store) {

      IterResult result = getKeysIter(startingPoint, store);
      if (!result.isChanged()) {
        return result.list;
      }

      final KeyAccumulator accum = new KeyAccumulator(store, result.previousFragmentIds);
      result.keyList.forEach(accum::addKey);
      accum.close();

      return accum.getCompleteList(getMutations());
    }

    private IterResult getKeysIter(L1 startingPoint, Store store) {
      HistoryRetriever retriever =
          new HistoryRetriever(store, startingPoint, getPreviousCheckpoint(), true, false, true);
      final CompleteList complete;
      // incrementals, from oldest to newest.
      final List<KeyList> incrementals;

      { // load the lists.
        ImmutableList<KeyList> keyLists =
            retriever
                .getStream()
                .map(h -> h.getL1().getKeyList())
                .filter(kl -> !kl.isEmptyIncremental())
                .collect(ImmutableList.toImmutableList());

        // the very last keylist should be a completelist, given the correct stop.
        KeyList last = keyLists.get(keyLists.size() - 1);
        Preconditions.checkArgument(last.isFull());
        complete = (CompleteList) last;
        incrementals = Lists.reverse(keyLists.subList(0, keyLists.size() - 1));
      }

      Set<InternalKey> removals = new HashSet<>();
      Set<InternalKey> adds = new HashSet<>();

      // determine the unique list of mutations. Operations that cancel each other out are ignored
      // for checkpoint purposes.
      for (KeyList kl : incrementals) {
        Preconditions.checkArgument(kl.getType() == Type.INCREMENTAL);
        IncrementalList il = (IncrementalList) kl;
        il.getMutations()
            .forEach(
                m -> {
                  final InternalKey key = m.getKey();
                  if (m.getType() == MutationType.ADDITION) {
                    if (removals.contains(key)) {
                      removals.remove(key);
                    } else {
                      adds.add(key);
                    }
                  } else if (m.getType() == MutationType.REMOVAL) {
                    if (adds.contains(key)) {
                      adds.remove(key);
                    } else {
                      removals.add(key);
                    }
                  } else {
                    throw new IllegalStateException("Invalid mutation type: " + m.getType().name());
                  }
                });
      }

      if (removals.isEmpty() && adds.isEmpty()) {
        return IterResult.unchanged(complete);
      }

      return IterResult.changed(
          complete.fragmentIds.stream().collect(ImmutableSet.toImmutableSet()),
          Stream.concat(
              complete.getKeys(startingPoint, store).filter(k -> !removals.contains(k)),
              adds.stream()));
    }

    @Override
    public Entity toEntity() {
      return Entity.ofMap(
          ImmutableMap.<String, Entity>of(
              IS_CHECKPOINT, Entity.ofBoolean(false),
              MUTATIONS, Entity.ofList(getMutations().stream().map(KeyMutation::toEntity)),
              ORIGIN, getPreviousCheckpoint().toEntity(),
              DISTANCE, Entity.ofNumber(getDistanceFromCheckpointCommits())));
    }

    @Override
    public Type getType() {
      return Type.INCREMENTAL;
    }

    static KeyList fromEntity(Map<String, Entity> value) {
      return ImmutableIncrementalList.builder()
          .addAllMutations(
              value.get(MUTATIONS).getList().stream()
                  .map(KeyMutation::fromEntity)
                  .collect(Collectors.toList()))
          .previousCheckpoint(Id.fromEntity(value.get(ORIGIN)))
          .distanceFromCheckpointCommits(Ints.saturatedCast(value.get(DISTANCE).getNumber()))
          .build();
    }

    private static class IterResult {
      private final CompleteList list;
      private final Stream<InternalKey> keyList;
      private final Set<Id> previousFragmentIds;

      private IterResult(
          CompleteList list, Stream<InternalKey> keyList, Set<Id> previousFragmentIds) {
        super();
        this.list = list;
        this.keyList = keyList;
        this.previousFragmentIds = previousFragmentIds;
      }

      public static IterResult unchanged(CompleteList list) {
        return new IterResult(list, null, null);
      }

      public static IterResult changed(Set<Id> previousFragmentIds, Stream<InternalKey> keys) {
        return new IterResult(null, keys, previousFragmentIds);
      }

      public boolean isChanged() {
        return keyList != null;
      }
    }
  }

  /**
   * A complete list is composed as one or more fragments. Each fragment's id is generated by the
   * hashed value of its contents.
   *
   * <p>Fragments lists are designed to minimize Dynamo record churn. Early fragment lists have the
   * oldest entries. Whenever a key is added, it is added to the last fragment (or a new fragment if
   * the last fragment is oversized). As such, over time the early fragments rarely if ever get
   * restated.
   */
  static class CompleteList extends KeyList {
    private static final String FRAGMENTS = "fragments";
    private static final String MUTATIONS = "mutations";

    private final List<Id> fragmentIds;
    private final List<KeyMutation> mutations;

    public CompleteList(List<Id> fragmentIds, List<KeyMutation> mutations) {
      this.fragmentIds = Preconditions.checkNotNull(fragmentIds);
      this.mutations = ImmutableList.copyOf(mutations);
    }

    @Override
    public KeyList plus(Id parent, List<KeyMutation> mutations) {
      return ImmutableIncrementalList.builder()
          .addAllMutations(mutations)
          .distanceFromCheckpointCommits(1)
          .previousCheckpoint(parent)
          .build();
    }

    @Override
    public Type getType() {
      return Type.FULL;
    }

    @Override
    public Optional<KeyList> createCheckpointIfNeeded(L1 startingPoint, Store store) {
      // checkpoint not needed, already a checkpoint.
      return Optional.empty();
    }

    @Override
    public Entity toEntity() {
      return Entity.ofMap(
          ImmutableMap.<String, Entity>of(
              IS_CHECKPOINT,
              Entity.ofBoolean(true),
              FRAGMENTS,
              Entity.ofList(
                  fragmentIds.stream().map(Id::toEntity).collect(ImmutableList.toImmutableList())),
              MUTATIONS,
              Entity.ofList(mutations.stream().map(KeyMutation::toEntity))));
    }

    static KeyList fromEntity(Map<String, Entity> value) {
      return new CompleteList(
          value.get(FRAGMENTS).getList().stream()
              .map(Id::fromEntity)
              .collect(ImmutableList.toImmutableList()),
          value.get(MUTATIONS).getList().stream()
              .map(KeyMutation::fromEntity)
              .collect(ImmutableList.toImmutableList()));
    }

    @Override
    Stream<InternalKey> getKeys(L1 startingPoint, Store store) {
      return fragmentIds.stream()
          .flatMap(
              f -> {
                Fragment fragment = store.loadSingle(ValueType.KEY_FRAGMENT, f);
                return fragment.getKeys().stream();
              });
    }

    @Override
    List<KeyMutation> getMutations() {
      return mutations;
    }
  }

  /**
   * Accumulates keys until we have enough to fill the ~max DynamoDB record size.
   *
   * <p>TODO: consider moving this data to S3.
   *
   * <p>TODO: move to a prefix encoded format.
   */
  static class KeyAccumulator {
    private static final int MAX_SIZE = 400_000 - 8096;
    private Store store;
    private Set<Id> presaved;
    private List<InternalKey> currentList = new ArrayList<>();
    private List<Id> fragmentIds = new ArrayList<>();
    private int currentListSize;

    public KeyAccumulator(Store store, Set<Id> presaved) {
      super();
      this.store = store;
      this.presaved = presaved;
    }

    public void addKey(InternalKey key) {
      currentList.add(key);
      currentListSize += key.estimatedSize();

      rotate(false);
    }

    private void rotate(boolean always) {
      if (!currentList.isEmpty() && (always || aboveThreshold())) {
        Fragment fragment = new Fragment(currentList);
        currentList.clear();
        currentListSize = 0;
        if (!presaved.contains(fragment.getId())) {
          // only save if we didn't save on the last checkpoint. This could still be a dupe of an
          // older list but since the object
          // is hashed, the value will be a simple overwrite of the same data.
          store.save(
              Collections.singletonList(new SaveOp<HasId>(ValueType.KEY_FRAGMENT, fragment)));
          fragmentIds.add(fragment.getId());
        }
      }
    }

    private boolean aboveThreshold() {
      if (currentListSize > MAX_SIZE) {
        return true;
      }

      return false;
    }

    public void close() {
      rotate(true);
    }

    public CompleteList getCompleteList(List<KeyMutation> mutations) {
      Preconditions.checkArgument(currentList.isEmpty());
      return new CompleteList(fragmentIds, mutations);
    }
  }
}
