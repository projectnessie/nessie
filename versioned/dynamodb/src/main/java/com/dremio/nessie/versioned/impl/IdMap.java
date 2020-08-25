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

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Maintains a map of positions to ids. The map is immutable. Each operation, generates a new map. All maps keep track
 * of their original state so one can see what items changed over time.
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
    if(position >= deltas.length || position < 0) {
      throw new IndexOutOfBoundsException(String.format("Position must be [0..%d), was actually %d.", deltas.length, position));
    }
  }

  /**
   * Create a copy of this map that applies the given update.
   * @param position The position to update.
   * @param newId The new value to set.
   * @return A copy of this map with the mutation applied.
   */
  public IdMap setId(int position, Id newId) {
    check(position);
    PositionDelta[] newDeltas = new PositionDelta[deltas.length];
    System.arraycopy(deltas, 0, newDeltas, 0, deltas.length);
    newDeltas[position] = PositionDelta.builder().from(newDeltas[position]).newId(newId).build();
    return new IdMap(newDeltas);
  }

  public int size() {
    return deltas.length;
  }

  @Override
  public Iterator<Id> iterator() {
    return Iterators.unmodifiableIterator(Arrays.stream(deltas).map(d -> d.getNewId()).iterator());
  }

  /**
   * Get any changes that have been applied to the tree.
   * @return A list of positions that have been mutated from the base tree.
   */
  List<PositionDelta> getChanges() {
    return Arrays.stream(deltas).filter(PositionDelta::isDirty).collect(Collectors.toList());
  }

  AttributeValue toAttributeValue() {
    return AttributeValue.builder().l(Arrays.stream(deltas).map(p -> p.getNewId().toAttributeValue()).collect(Collectors.toList())).build();
  }

  /**
   * Deserialize a map from a given input value.
   * @param value The value to deserialize.
   * @param size The expected size of the map to be loaded.
   * @return The deserialized map.
   */
  public static IdMap fromAttributeValue(AttributeValue value, int size) {
    PositionDelta[] deltas = new PositionDelta[size];
    List<AttributeValue> items = value.l();
    Preconditions.checkArgument(items.size() == size, "Expected size %s but actual size was %s.", size, items.size());

    int i = 0;
    for (AttributeValue v : items) {
      deltas[i] = PositionDelta.of(i, Id.fromAttributeValue(v));
      i++;
    }

    return new IdMap(deltas);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.hashCode(deltas);
    return result;
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

}
