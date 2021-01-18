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

import com.dremio.nessie.tiered.builder.L2;
import com.dremio.nessie.versioned.store.Id;
import com.google.common.base.Objects;

class InternalL2 extends PersistentBase<L2> {

  private static final long HASH_SEED = -6352836103271505167L;

  static final int SIZE = 199;
  static InternalL2 EMPTY = new InternalL2(null, new IdMap(SIZE, InternalL3.EMPTY_ID), DT.UNKNOWN);
  static Id EMPTY_ID = EMPTY.getId();

  private final IdMap map;

  private InternalL2(Id id, IdMap map, Long dt) {
    super(id, dt);
    assert map.size() == SIZE;
    this.map = map;
  }

  private InternalL2(IdMap map) {
    this(null, map, DT.now());
  }


  Id getId(int position) {
    return map.getId(position);
  }

  InternalL2 set(int position, Id l2Id) {
    return new InternalL2(map.withId(position, l2Id));
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
      if (!id.equals(InternalL3.EMPTY_ID)) {
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
    InternalL2 l2 = (InternalL2) o;
    return Objects.equal(map, l2.map);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(map);
  }

  @Override
  L2 applyToConsumer(L2 consumer) {
    return super.applyToConsumer(consumer)
        .children(this.map.stream());
  }

  /**
   * implements {@link L2} to build an {@link InternalL2} object.
   */
  // Needs to be a package private class, otherwise class-initialization of ValueType fails with j.l.IllegalAccessError
  static final class Builder extends EntityBuilder<InternalL2> implements L2 {

    private Id id;
    private Long dt;
    private Stream<Id> children;

    Builder() {
      // empty
    }

    @Override
    public InternalL2.Builder children(Stream<Id> ids) {
      checkCalled(this.children, "children");
      this.children = ids;
      return this;
    }

    @Override
    public InternalL2.Builder id(Id id) {
      checkCalled(this.id, "id");
      this.id = id;
      return this;
    }

    @Override
    public Builder dt(long dt) {
      checkCalled(this.dt, "dt");
      this.dt = dt;
      return this;
    }

    @Override
    InternalL2 build() {
      // null-id is allowed (will be generated)
      checkSet(children, "children");

      return new InternalL2(
          id,
          IdMap.of(children, SIZE),
          dt);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public EntityType<L2, InternalL2, InternalL2.Builder> getEntityType() {
    return EntityType.L2;
  }
}
