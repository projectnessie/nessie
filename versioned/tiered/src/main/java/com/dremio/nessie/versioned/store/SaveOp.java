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
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.dremio.nessie.tiered.builder.BaseConsumer;
import com.dremio.nessie.versioned.impl.PersistentBase;

public class SaveOp<C extends BaseConsumer<C>> {
  private final ValueType type;
  private final Supplier<Id> idSupplier;
  private final Consumer<BaseConsumer<C>> serializer;

  /**
   * Constructs a new save-operation for the given type, id and serializer.
   *
   * @param type value type
   * @param idSupplier supplier for the entity's id
   * @param serializer Java-consumer that will receive a consumer to serialize the entity to
   */
  public SaveOp(ValueType type, Supplier<Id> idSupplier, Consumer<BaseConsumer<C>> serializer) {
    this.type = type;
    this.idSupplier = idSupplier;
    this.serializer = serializer;
  }

  public ValueType getType() {
    return type;
  }

  public Id getId() {
    return idSupplier.get();
  }

  /**
   * Apply the contents of this {@link SaveOp}'s {@code value} to the given {@code consumer}.
   * <p>
   * Works only, if {@code value} is an instance of {@link PersistentBase}.
   * </p>
   *
   * @param consumer the consumer that will receive the contents of {@code value}
   */
  public void serialize(BaseConsumer<C> consumer) {
    serializer.accept(consumer);
  }

  @Override
  public String toString() {
    return "SaveOp [type=" + type + ", id=" + idSupplier.get() + "]";
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, idSupplier.get());
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

    SaveOp<C> other = (SaveOp<C>) obj;
    return type == other.type && Objects.equals(idSupplier.get(), other.idSupplier.get());
  }
}
