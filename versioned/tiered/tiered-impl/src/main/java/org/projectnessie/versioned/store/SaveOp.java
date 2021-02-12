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
package org.projectnessie.versioned.store;

import java.util.Objects;

import org.projectnessie.versioned.tiered.BaseValue;

/**
 * Save operations push the properties of the value to save via the {@link BaseValue consumer}
 * passed to the {@link #serialize(BaseValue)} method.
 *
 * @param <C> {@link BaseValue consumer} used to handle the save-operation
 */
public abstract class SaveOp<C extends BaseValue<C>> {
  private final ValueType<C> type;
  private final Id id;

  /**
   * Constructs a new save-operation for the given type, id and serializer.
   *
   * @param type value type
   * @param id the entity's id
   */
  public SaveOp(ValueType<C> type, Id id) {
    this.type = type;
    this.id = id;
  }

  public ValueType<C> getType() {
    return type;
  }

  public Id getId() {
    return id;
  }

  /**
   * Called by store implementations instructing the implementation to serialize the properties
   * to the given {@link BaseValue}.
   *
   * @param consumer the consumer that will receive the properties
   */
  public abstract void serialize(C consumer);

  @Override
  public String toString() {
    return "SaveOp [type=" + type + ", id=" + id + "]";
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, id);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (!(obj instanceof SaveOp)) {
      return false;
    }

    SaveOp<?> other = (SaveOp<?>) obj;
    return type == other.type && Objects.equals(id, other.id);
  }
}
