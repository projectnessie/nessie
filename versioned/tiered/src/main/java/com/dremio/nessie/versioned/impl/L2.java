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

import java.util.stream.Stream;

import com.dremio.nessie.tiered.builder.L2Consumer;
import com.dremio.nessie.versioned.store.Id;
import com.google.common.base.Objects;

class L2 extends PersistentBase<L2Consumer> {

  private static final long HASH_SEED = -6352836103271505167L;

  static final int SIZE = 199;
  static L2 EMPTY = new L2(null, new IdMap(SIZE, L3.EMPTY_ID));
  static Id EMPTY_ID = EMPTY.getId();

  private final IdMap map;

  private L2(Id id, IdMap map) {
    super(id);
    assert map.size() == SIZE;
    this.map = map;
  }

  private L2(IdMap map) {
    this(null, map);
  }


  Id getId(int position) {
    return map.getId(position);
  }

  L2 set(int position, Id l2Id) {
    return new L2(map.withId(position, l2Id));
  }

  @Override
  Id generateId() {
    return Id.build(h -> {
      h.putLong(HASH_SEED);
      map.forEach(id -> h.putBytes(id.getValue().asReadOnlyByteBuffer()));
    });
  }

  /**
   * return the number of positions that are non-empty.
   * @return number of non-empty positions.
   */
  int size() {
    int count = 0;
    for (Id id : map) {
      if (!id.equals(L3.EMPTY_ID)) {
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
    L2 l2 = (L2) o;
    return Objects.equal(map, l2.map);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(map);
  }

  @Override
  L2Consumer applyToConsumer(L2Consumer consumer) {
    return super.applyToConsumer(consumer)
        .children(this.map.stream());
  }

  /**
   * implements {@link L2Consumer} to build an {@link L2} object.
   */
  // Needs to be a package private class, otherwise class-initialization of ValueType fails with j.l.IllegalAccessError
  static final class Builder extends EntityBuilder<L2> implements L2Consumer {

    private Id id;
    private Stream<Id> children;

    Builder() {
      // empty
    }

    @Override
    public L2.Builder children(Stream<Id> ids) {
      checkCalled(this.children, "children");
      this.children = ids;
      return this;
    }

    @Override
    public L2.Builder id(Id id) {
      checkCalled(this.id, "id");
      this.id = id;
      return this;
    }

    @Override
    L2 build() {
      // null-id is allowed (will be generated)
      checkSet(children, "children");

      return new L2(
          id,
          IdMap.of(children, SIZE));
    }
  }
}
