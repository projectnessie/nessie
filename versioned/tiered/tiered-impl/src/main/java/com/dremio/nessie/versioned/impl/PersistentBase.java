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

import com.dremio.nessie.tiered.builder.BaseValue;
import com.dremio.nessie.versioned.store.HasId;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.SaveOp;
import com.google.common.base.Preconditions;

abstract class PersistentBase<C extends BaseValue<C>> implements HasId {

  //unchanging but only generated once needed.
  private Id id;

  private final long dt;

  PersistentBase() {
    this.id = null;
    this.dt = DT.now();
  }

  PersistentBase(Id id, Long dt) {
    this.id = id;
    this.dt = dt == null ? DT.UNKNOWN : dt;
  }

  abstract Id generateId();

  /**
   * Apply the contents this entity to the given consumer.
   *
   * @param consumer consumer that will receive the properties of this entity
   * @return the consumer passed into the function
   */
  C applyToConsumer(C consumer) {
    return consumer.id(getId()).dt(dt);
  }

  @Override
  public final Id getId() {
    if (id == null) {
      id = generateId();
    }
    return id;
  }

  abstract <E extends PersistentBase<C>> EntityType<C, E, ?> getEntityType();

  SaveOp<C> toSaveOp() {
    return getEntityType().createSaveOpForEntity(this);
  }

  void ensureConsistentId() {
    assert id == null || id.equals(generateId());
  }

  public long getDt() {
    return dt;
  }

  abstract static class EntityBuilder<E extends HasId, C extends BaseValue<C>> implements BaseValue<C> {
    Id id;
    Long dt;

    @SuppressWarnings("unchecked")
    @Override
    public C id(Id id) {
      checkCalled(this.id, "id");
      this.id = id;
      return (C) this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public C dt(long dt) {
      checkCalled(this.dt, "dt");
      this.dt = dt;
      return (C) this;
    }

    abstract E build();
  }

  static void checkCalled(Object arg, String name) {
    Preconditions.checkArgument(arg == null, String.format("Cannot call %s more than once", name));
  }

  static void checkSet(Object arg, String name) {
    Preconditions.checkArgument(arg != null, String.format("Must call %s", name));
  }
}
