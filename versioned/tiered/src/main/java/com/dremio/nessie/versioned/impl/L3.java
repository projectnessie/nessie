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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.dremio.nessie.tiered.builder.L3Consumer;
import com.dremio.nessie.tiered.builder.Producer;
import com.dremio.nessie.versioned.ImmutableKey;
import com.dremio.nessie.versioned.impl.DiffFinder.KeyDiff;
import com.dremio.nessie.versioned.impl.KeyMutation.KeyAddition;
import com.dremio.nessie.versioned.impl.KeyMutation.KeyRemoval;
import com.dremio.nessie.versioned.store.Entity;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.KeyDelta;
import com.dremio.nessie.versioned.store.SimpleSchema;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;

public class L3 extends MemoizedId<L3Consumer> {

  private static final long HASH_SEED = 4604180344422375655L;

  private final TreeMap<InternalKey, PositionDelta> map;

  public static L3 EMPTY = new L3(new TreeMap<>());
  public static Id EMPTY_ID = EMPTY.getId();

  private L3(TreeMap<InternalKey, PositionDelta> keys) {
    this(null, keys);
  }

  private L3(Id id, TreeMap<InternalKey, PositionDelta> keys) {
    super(id);
    this.map = keys;
    ensureConsistentId();
  }

  Id getId(InternalKey key) {
    PositionDelta delta = map.get(key);
    if (delta == null) {
      return Id.EMPTY;
    }
    return delta.getNewId();
  }

  /**
   * Get the key if it exists.
   * @param key The id of the key to retrieve
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
      newDelta = PositionDelta.SINGLE_ZERO;
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


  public static final SimpleSchema<L3> SCHEMA = new SimpleSchema<L3>() {

    private static final String ID = "id";
    private static final String TREE = "tree";
    private static final String TREE_KEY = "key";
    private static final String TREE_ID = "id";

    @Override
    public L3 deserialize(Map<String, Entity> attributeMap) {
      TreeMap<InternalKey, PositionDelta> tree = attributeMap.get(TREE).getList().stream().map(av -> av.getMap()).collect(Collectors.toMap(
          m -> InternalKey.fromEntity(m.get(TREE_KEY)),
          m -> PositionDelta.of(0, Id.fromEntity(m.get(TREE_ID))),
          (a,b) -> {
            throw new UnsupportedOperationException();
          },
          TreeMap::new));

      return new L3(
          Id.fromEntity(attributeMap.get(ID)),
          tree
      );
    }

    @Override
    public Map<String, Entity> itemToMap(L3 item, boolean ignoreNulls) {
      List<Entity> values = item.map.entrySet().stream()
          .filter(e -> !e.getValue().getNewId().isEmpty())
          .map(e -> {
            InternalKey key = e.getKey();
            PositionDelta pm = e.getValue();
            Map<String, Entity> pmm = ImmutableMap.of(
                TREE_KEY, key.toEntity(),
                TREE_ID, pm.getNewId().toEntity());
            return Entity.ofMap(pmm);
          }).collect(Collectors.toList());
      return ImmutableMap.<String, Entity>builder()
          .put(TREE, Entity.ofList(values))
          .put(ID, item.getId().toEntity())
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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    L3 l3 = (L3) o;
    return Objects.equal(map, l3.map);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(map);
  }

  @Override
  public L3Consumer applyToConsumer(L3Consumer consumer) {
    consumer.id(this.getId());

    Stream<KeyDelta> keyDelta = this.map.entrySet().stream()
        .filter(e -> !e.getValue().getNewId().isEmpty())
        .map(e -> KeyDelta.of(e.getKey().toKey(), e.getValue().getNewId()));
    consumer.keyDelta(keyDelta);

    return consumer;
  }

  /**
   * Create a new {@link Builder} instance that implements both
   * {@link L3Consumer} and a matching {@link Producer} that
   * builds an {@link L3} object.
   *
   * @return new builder instance
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Implements both
   * {@link L3Consumer} and a matching {@link Producer} that
   * builds an {@link L3} object.
   */
  // Needs to be a public class, otherwise class-initialization of ValueType fails with j.l.IllegalAccessError
  public static final class Builder implements L3Consumer, Producer<L3, L3Consumer> {

    private Id id;
    private Stream<KeyDelta> keyDelta;

    private Builder() {
      // empty
    }

    @Override
    public L3.Builder id(Id id) {
      checkCalled(this.id, "id");
      this.id = id;
      return this;
    }

    @Override
    public Builder keyDelta(Stream<KeyDelta> keyDelta) {
      checkCalled(this.keyDelta, "keyDelta");
      this.keyDelta = keyDelta;
      return this;
    }

    @Override
    public L3 build() {
      // null-id is allowed (will be generated)
      checkSet(keyDelta, "keyDelta");

      return new L3(
          id,
          keyDelta.collect(
              Collectors.toMap(
                  kd -> new InternalKey(ImmutableKey.builder().addAllElements(kd.getKey().getElements()).build()),
                  kd -> PositionDelta.of(0, kd.getId()),
                  (a, b) -> {
                    throw new IllegalArgumentException(String.format("Got Id %s and %s for same key",
                        a.getNewId(), b.getNewId()));
                  },
                  TreeMap::new
              )
          ));
    }
  }

  /**
   * Get a list of all the key to valueId differences between two L3s.
   *
   * <p>This returns the difference between the updated (not original) state of the two L3s (if the L3s have been mutated).
   *
   * @param from The initial tree state.
   * @param to The final tree state.
   * @return The differences when going from initial to final state.
   */
  public static Stream<KeyDiff> compare(L3 from, L3 to) {
    MapDifference<InternalKey, Id> difference =  Maps.difference(
        Maps.transformValues(from.map, p -> p.getNewId()),
        Maps.transformValues(to.map, p -> p.getNewId())
        );
    return Stream.concat(
        difference.entriesDiffering().entrySet().stream().map(KeyDiff::new),
        Stream.concat(
            difference.entriesOnlyOnLeft().entrySet().stream().map(KeyDiff::onlyOnLeft),
            difference.entriesOnlyOnRight().entrySet().stream().map(KeyDiff::onlyOnRight)));
  }
}
