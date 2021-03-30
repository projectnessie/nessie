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
package org.projectnessie.versioned.impl;

import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.projectnessie.versioned.ImmutableKey;
import org.projectnessie.versioned.impl.DiffFinder.KeyDiff;
import org.projectnessie.versioned.store.Id;
import org.projectnessie.versioned.store.KeyDelta;
import org.projectnessie.versioned.tiered.L3;

import com.google.common.base.Objects;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;

class InternalL3 extends PersistentBase<L3> {

  private static final long HASH_SEED = 4604180344422375655L;

  private final TreeMap<InternalKey, PositionDeltaWithPayload> map;

  static InternalL3 EMPTY = new InternalL3(new TreeMap<>());
  static Id EMPTY_ID = EMPTY.getId();

  private InternalL3(TreeMap<InternalKey, PositionDeltaWithPayload> keys) {
    this(null, keys, DT.now());
  }

  private InternalL3(Id id, TreeMap<InternalKey, PositionDeltaWithPayload> keys, Long dt) {
    super(id, dt);
    this.map = keys;
    ensureConsistentId();
  }

  Id getId(InternalKey key) {
    PositionDeltaWithPayload delta = map.get(key);
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
  InternalL3 set(InternalKey key, Id valueId, Byte payload) {
    TreeMap<InternalKey, PositionDeltaWithPayload> newMap = (TreeMap<InternalKey, PositionDeltaWithPayload>) map.clone();
    PositionDeltaWithPayload newDelta = newMap.get(key);
    if (newDelta == null) {
      newDelta = PositionDeltaWithPayload.SINGLE_ZERO;
    }

    newDelta = PositionDeltaWithPayload.builderWithPayload().from(newDelta).newId(valueId).newPayload(payload).build();
    if (!newDelta.isDirty()) {
      // this turned into a no-op delta, remove it entirely from the map.
      newMap.remove(key);
    } else {
      newMap.put(key, newDelta);
    }
    return new InternalL3(newMap);
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

  Stream<InternalMutation> getMutations() {
    return map.entrySet().stream().filter(e -> e.getValue().isDirty())
        .flatMap(e -> {
          PositionDeltaWithPayload d = e.getValue();
          if (d.wasAdded()) {
            return Stream.of(InternalMutation.InternalAddition.of(e.getKey(), d.getNewPayload()));
          } else if (d.wasRemoved()) {
            return Stream.of(InternalMutation.InternalRemoval.of(e.getKey()));
          } else if (e.getValue().isPayloadDirty()) {
            // existing key that has changed type
            return Stream.of(InternalMutation.InternalRemoval.of(e.getKey()),
                InternalMutation.InternalAddition.of(e.getKey(), d.getNewPayload()));
          } else {
            return Stream.of();
          }
        });
  }

  Stream<InternalKey> getKeys() {
    return map.keySet().stream();
  }

  /**
   * return the number of keys defined.
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
    InternalL3 l3 = (InternalL3) o;
    return Objects.equal(map, l3.map);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(map);
  }

  @Override
  L3 applyToConsumer(L3 consumer) {
    super.applyToConsumer(consumer);

    Stream<KeyDelta> keyDelta = this.map.entrySet().stream()
        .filter(e -> !e.getValue().getNewId().isEmpty())
        .map(e -> KeyDelta.of(e.getKey().toKey(), e.getValue().getNewId(), e.getValue().getNewPayload()));
    consumer.keyDelta(keyDelta);

    return consumer;
  }

  /**
   * Implements {@link L3} to build an {@link InternalL3} object.
   */
  // Needs to be a package private class, otherwise class-initialization of ValueType fails with j.l.IllegalAccessError
  static final class Builder extends EntityBuilder<InternalL3, L3> implements L3 {

    private Stream<KeyDelta> keyDelta;

    Builder() {
      // empty
    }

    @Override
    public Builder keyDelta(Stream<KeyDelta> keyDelta) {
      checkCalled(this.keyDelta, "keyDelta");
      this.keyDelta = keyDelta;
      return this;
    }

    @Override
    InternalL3 build() {
      // null-id is allowed (will be generated)
      checkSet(keyDelta, "keyDelta");

      return new InternalL3(
          id,
          keyDelta.collect(
              Collectors.toMap(
                  kd -> new InternalKey(ImmutableKey.builder().addAllElements(kd.getKey().getElements()).build()),
                  kd -> PositionDeltaWithPayload.of(0, kd.getId(), kd.getPayload()),
                  (a, b) -> {
                    throw new IllegalArgumentException(String.format("Got Id %s and %s for same key",
                        a.getNewId(), b.getNewId()));
                  },
                  TreeMap::new
              )
          ),
          dt);
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
  public static Stream<KeyDiff> compare(InternalL3 from, InternalL3 to) {
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

  @SuppressWarnings("unchecked")
  @Override
  EntityType<L3, InternalL3, InternalL3.Builder> getEntityType() {
    return EntityType.L3;
  }
}
