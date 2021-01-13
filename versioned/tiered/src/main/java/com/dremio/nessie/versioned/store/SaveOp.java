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
package com.dremio.nessie.versioned.store;

import java.util.Objects;

import com.dremio.nessie.tiered.builder.HasIdConsumer;
import com.dremio.nessie.versioned.impl.PersistentBase;

public class SaveOp<V extends HasId> {
  private final ValueType type;
  private final V value;

  public SaveOp(ValueType type, V value) {
    this.type = type;
    this.value = value;
  }

  public ValueType getType() {
    return type;
  }

  public V getValue() {
    return value;
  }

  /**
   * Apply the contents of this {@link SaveOp}'s {@code value} to the given {@code consumer}.
   * <p>
   * Works only, if {@code value} is an instance of {@link PersistentBase}.
   * </p>
   *
   * @param consumer the consumer that will receive the contents of {@code value}
   * @param <C> the type of the consumer
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  public <C extends HasIdConsumer<C>> void serialize(C consumer) {
    PersistentBase v = (PersistentBase) value;
    v.applyToConsumer(consumer);
  }

  @Override
  public String toString() {
    return "SaveOp [type=" + type + ", id=" + value.getId() + "]";
  }

  @Override
  public int hashCode() {
    if (type.isImmutable()) {
      return Objects.hash(type, value.getId());
    }

    return Objects.hash(type, value);
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (!(obj instanceof SaveOp)) {
      return false;
    }

    SaveOp<V> other = (SaveOp<V>) obj;
    if (type.isImmutable()) {
      // if the items are immutable, their id is sufficient to determine equality.
      return type == other.type && Objects.equals(value.getId(), other.value.getId());
    }

    // otherwise, use the actual values for equality.
    return type == other.type && Objects.equals(value, other.value);
  }

}
