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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Maintains a map of ids to positions. The map is immutable. Each operation, generates a new map. All maps keep track
 * of their original state so one can see what items changed over time.
 */
class IdMap implements Iterable<Id> {

  static final String POSITION_PREFIX = "f";

  private final PositionMutation[] deltas;

  private IdMap(PositionMutation[] deltas) {
    this.deltas = deltas;
  }

  IdMap(int size) {
    this(size, Id.EMPTY);
  }

  IdMap(int size, Id fill) {
    deltas = new PositionMutation[size];
    for (int i = 0; i < size; i++) {
      deltas[i] = PositionMutation.builder().oldId(fill).newId(fill).position(i).build();
    }
  }

  public Id getId(int position) {
    check(position);
    return deltas[position].getNewId();
  }

  private void check(int position) {
    Preconditions.checkArgument(position < deltas.length);
    Preconditions.checkArgument(position >= 0);
  }

  public IdMap setId(int position, Id newId) {
    check(position);
    PositionMutation[] newDeltas = new PositionMutation[deltas.length];
    System.arraycopy(deltas, 0, newDeltas, 0, deltas.length);
    newDeltas[position] = ImmutablePositionMutation.builder().from(newDeltas[position]).newId(newId).build();
    return new IdMap(newDeltas);
  }

  public int size() {
    return deltas.length;
  }

  @Override
  public Iterator<Id> iterator() {
    return Arrays.stream(deltas).map(d -> d.getNewId()).iterator();
  }

  /**
   * Get any changes that have been applied to the tree.
   * @return
   */
  List<PositionMutation> getChanges() {
    return Arrays.stream(deltas).filter(PositionMutation::isDirty).collect(Collectors.toList());
  }

  AttributeValue toAttributeValue() {
    Map<String, AttributeValue> values = new HashMap<>();
    for(int i =0; i < deltas.length; i++) {
      Id id = deltas[i].getNewId();
      values.put(POSITION_PREFIX + i, id.toAttributeValue());
    }
    return AttributeValue.builder().m(values).build();
  }

  public static IdMap fromAttributeValue(AttributeValue value, int size) {
    PositionMutation[] deltas = new PositionMutation[size];
    Map<String, AttributeValue> items = value.m();
    Preconditions.checkArgument(items.size() == size, "Expected size %s but actual size was %s.", size, items.size());

    for (int i =0; i < size; i++) {
      deltas[i] = PositionMutation.of(i, Id.fromAttributeValue(Preconditions.checkNotNull(items.get(POSITION_PREFIX + i))));
    }
    return new IdMap(deltas);
  }


  void applyToFields(Object obj, List<Field> fields) {
    assert size() == fields.size();
    int i =0;
    try {
      for(Id id : this) {
        fields.get(i).set(obj, id.getValue().toByteArray());
        i++;
      }
    } catch (IllegalArgumentException | IllegalAccessException e) {
      throw new IllegalStateException("Failure while generating bean translation.");
    }
  }

  static IdMap of(Object obj, List<Field> fields) {
    IdMap map = new IdMap(L1.SIZE);
    try {
      for(int i =0; i < L1.SIZE; i++) {
        map.setId(i, Id.of((byte[]) fields.get(i).get(obj)));
      }
      return map;
    } catch (IllegalArgumentException | IllegalAccessException e) {
      throw new IllegalStateException(e);
    }
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

  static ImmutableList<Field> getNumberedFields(Class<?> clazz, int size) {
    Map<Integer, Field> mappedFields = Arrays.stream(clazz.getDeclaredFields())
        .filter(f -> !Modifier.isStatic(f.getModifiers()))
        .collect(
        Collectors.toMap(f -> {
          String name = f.getName();
          return Integer.parseInt(name.substring(1, name.length()));
        },
     f -> f));
    ImmutableList.Builder<Field> builder = ImmutableList.builder();
    for (int i =0; i < size; i++) {
      builder.add(Preconditions.checkNotNull(mappedFields.get(i)));
    }

    return builder.build();
  }
}
