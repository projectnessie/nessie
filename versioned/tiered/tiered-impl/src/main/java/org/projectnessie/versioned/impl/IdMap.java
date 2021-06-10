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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.projectnessie.versioned.store.Entity;
import org.projectnessie.versioned.store.Id;

/**
 * Maintains a map of positions to ids. The map is immutable. Each operation, generates a new map.
 * All maps keep track of their original state so one can see what items changed over time.
 */
class IdMap implements Iterable<Id> {

  private final PositionDelta[] deltas;

  private IdMap(PositionDelta[] deltas) {
    this.deltas = deltas;
  }

  IdMap(int size) {
    this(size, Id.EMPTY);
  }

  IdMap(int size, Id fill) {
    deltas = new PositionDelta[size];
    for (int i = 0; i < size; i++) {
      deltas[i] = PositionDelta.builder().oldId(fill).newId(fill).position(i).build();
    }
  }

  public Id getId(int position) {
    check(position);
    return deltas[position].getNewId();
  }

  private void check(int position) {
    Preconditions.checkPositionIndex(position, deltas.length);
  }

  /**
   * Create a copy of this map that applies the given update.
   *
   * @param position The position to update.
   * @param newId The new value to set.
   * @return A copy of this map with the mutation applied.
   */
  public IdMap withId(int position, Id newId) {
    check(position);
    PositionDelta[] newDeltas = new PositionDelta[deltas.length];
    System.arraycopy(deltas, 0, newDeltas, 0, deltas.length);
    newDeltas[position] = PositionDelta.builder().from(newDeltas[position]).newId(newId).build();
    return new IdMap(newDeltas);
  }

  public int size() {
    return deltas.length;
  }

  /** Returns the new-IDs of the deltas as a {@link Stream}. */
  public Stream<Id> stream() {
    return Arrays.stream(deltas).map(PositionDelta::getNewId);
  }

  @Override
  public Iterator<Id> iterator() {
    return Iterators.unmodifiableIterator(
        Arrays.stream(deltas).map(PositionDelta::getNewId).iterator());
  }

  /**
   * Get any changes that have been applied to the tree.
   *
   * @return A list of positions that have been mutated from the base tree.
   */
  List<PositionDelta> getChanges() {
    return Arrays.stream(deltas).filter(PositionDelta::isDirty).collect(Collectors.toList());
  }

  Entity toEntity() {
    return Entity.ofList(
        Arrays.stream(deltas)
            .map(p -> p.getNewId().toEntity())
            .collect(ImmutableList.toImmutableList()));
  }

  /**
   * Deserialize a map from a given input value.
   *
   * @param value The value to deserialize.
   * @param size The expected size of the map to be loaded.
   * @return The deserialized map.
   */
  public static IdMap fromEntity(Entity value, int size) {
    PositionDelta[] deltas = new PositionDelta[size];
    List<Entity> items = value.getList();
    Preconditions.checkArgument(
        items.size() == size, "Expected size %s but actual size was %s.", size, items.size());

    int i = 0;
    for (Entity v : items) {
      deltas[i] = PositionDelta.of(i, Id.fromEntity(v));
      i++;
    }

    return new IdMap(deltas);
  }

  /**
   * Constructs an {@link IdMap} from a list of {@link Id}s.
   *
   * @param expectedSize the size of the resulting {@link IdMap}
   */
  public static IdMap of(Stream<Id> children, int expectedSize) {
    AtomicInteger counter = new AtomicInteger();
    PositionDelta[] deltas =
        children
            .map(id -> PositionDelta.of(counter.getAndIncrement(), id))
            .toArray(PositionDelta[]::new);

    if (deltas.length != expectedSize) {
      throw new IllegalStateException(
          "Must collect exactly " + expectedSize + " Id elements, " + "but got " + deltas.length);
    }

    return new IdMap(deltas);
  }

  /**
   * Constructs an {@link IdMap} from a list of {@link Id}s.
   *
   * @param expectedSize the size of the resulting {@link IdMap}
   */
  public static IdMap of(List<Id> children, int expectedSize) {
    int sz = children.size();
    PositionDelta[] deltas = new PositionDelta[sz];

    if (sz != expectedSize) {
      throw new IllegalStateException(
          "Must collect exactly " + expectedSize + " Id elements, " + "but got " + sz);
    }

    for (int i = 0; i < sz; i++) {
      Id id = children.get(i);
      deltas[i] = PositionDelta.of(i, id);
    }

    return new IdMap(deltas);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(deltas);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof IdMap)) {
      return false;
    }
    IdMap other = (IdMap) obj;
    return Arrays.equals(deltas, other.deltas);
  }

  /**
   * Collects a {@link Stream} of {@link Id}s as an {@link IdMap}, must collect exactly the expected
   * number of {@link Id}s as given in the {@code expectedSize} parameter.
   *
   * @param expectedSize the size of the resulting {@link IdMap}
   */
  public static Collector<Id, List<Id>, IdMap> collector(int expectedSize) {
    return new Collector<Id, List<Id>, IdMap>() {
      @Override
      public Supplier<List<Id>> supplier() {
        return ArrayList::new;
      }

      @Override
      public BiConsumer<List<Id>, Id> accumulator() {
        return List::add;
      }

      @Override
      public BinaryOperator<List<Id>> combiner() {
        return (l1, l2) -> {
          l1.addAll(l2);
          return l1;
        };
      }

      @Override
      public Function<List<Id>, IdMap> finisher() {
        return l -> IdMap.of(l, expectedSize);
      }

      @Override
      public Set<Characteristics> characteristics() {
        return EnumSet.noneOf(Characteristics.class);
      }
    };
  }
}
