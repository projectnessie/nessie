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

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.projectnessie.versioned.impl.PersistentBase.EntityBuilder;
import org.projectnessie.versioned.store.Id;
import org.projectnessie.versioned.store.NotFoundException;
import org.projectnessie.versioned.store.SaveOp;
import org.projectnessie.versioned.store.Store;
import org.projectnessie.versioned.store.StoreOperationException;
import org.projectnessie.versioned.store.ValueType;
import org.projectnessie.versioned.tiered.BaseValue;
import org.projectnessie.versioned.tiered.CommitMetadata;
import org.projectnessie.versioned.tiered.Fragment;
import org.projectnessie.versioned.tiered.L1;
import org.projectnessie.versioned.tiered.L2;
import org.projectnessie.versioned.tiered.L3;
import org.projectnessie.versioned.tiered.Ref;
import org.projectnessie.versioned.tiered.Value;

final class EntityType<C extends BaseValue<C>, E extends PersistentBase<C>, B extends EntityBuilder<E, C>> {

  private static final Map<ValueType<?>, EntityType<?, ?, ?>> BY_VALUE_TYPE = new HashMap<>();

  static final EntityType<Ref, InternalRef, InternalRef.Builder<?>> REF =
      new EntityType<>(ValueType.REF, InternalRef.Builder::new);
  static final EntityType<L1, InternalL1, InternalL1.Builder> L1 =
      new EntityType<>(ValueType.L1, org.projectnessie.versioned.impl.InternalL1.Builder::new);
  static final EntityType<L2, InternalL2, InternalL2.Builder> L2 =
      new EntityType<>(ValueType.L2, org.projectnessie.versioned.impl.InternalL2.Builder::new);
  static final EntityType<L3, InternalL3, InternalL3.Builder> L3 =
      new EntityType<>(ValueType.L3, org.projectnessie.versioned.impl.InternalL3.Builder::new);
  static final EntityType<Value, InternalValue, InternalValue.Builder> VALUE =
      new EntityType<>(ValueType.VALUE, InternalValue.Builder::new);
  static final EntityType<Fragment, InternalFragment, InternalFragment.Builder> KEY_FRAGMENT =
      new EntityType<>(ValueType.KEY_FRAGMENT, InternalFragment.Builder::new);
  static final EntityType<CommitMetadata, InternalCommitMetadata, InternalCommitMetadata.Builder> COMMIT_METADATA =
      new EntityType<>(ValueType.COMMIT_METADATA, InternalCommitMetadata.Builder::new);

  final ValueType<C> valueType;
  final Supplier<B> producerSupplier;

  private EntityType(ValueType<C> valueType, Supplier<B> producerSupplier) {
    if (!valueType.getValueClass().isInstance(producerSupplier.get())) {
      throw new IllegalStateException("While you can't formally expose a value instance as the subclass of "
          + "two separate generic parameters, this class does so internally. The builders provided must be of both C and B types.");
    }
    this.valueType = valueType;
    this.producerSupplier = producerSupplier;
    BY_VALUE_TYPE.put(valueType, this);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  static <C extends BaseValue<C>, E extends PersistentBase<C>, B extends EntityBuilder<E, C>> EntityType<C, E, B>
      forType(ValueType<C> type) {
    return (EntityType) BY_VALUE_TYPE.get(type);
  }

  /**
   * Retrieve a single value.
   *
   * @param store store to load the value from.
   * @param id The id of the value.
   * @return The value at the given Id.
   * @throws NotFoundException If the value is not found.
   * @throws StoreOperationException Thrown if some kind of underlying storage operation fails.
   */
  E loadSingle(Store store, Id id) {
    return buildEntity(consumer -> store.loadSingle(valueType, id, consumer));
  }

  SaveOp<C> createSaveOpForEntity(E value) {
    return new EntitySaveOp<>(valueType, value);
  }

  /**
   * Create a new "plain entity" producer for this type.
   * <p>
   * Using {@link #buildEntity(Consumer)} is a simpler approach though.
   * </p>
   * <p>
   * Example:
   * </p>
   * <pre><code>
   *   ValueConsumer producer = ValueType.VALUE.newEntityProducer();
   *   producer.id(theId);
   *   producer.value(someBytes);
   *   InternalValue value = ValueType.VALUE.buildFromProducer(producer);
   * </code></pre>
   *
   * @return new created producer instance
   */
  B newEntityProducer() {
    return producerSupplier.get();
  }

  /**
   * Allows to create an entity instance of the type represented by this {@link ValueType}.
   * This is a simplification of the code example mentioned in {@link #newEntityProducer()}.
   * <p>
   * Example:
   * </p>
   * <pre><code>
   * InternalValue value = ValueType.VALUE.buildEntity(
   *     (ValueConsumer producer) -&gt; producer.id(theId).value(someBytes));
   * </code></pre>
   *
   * @param producerConsumer Java consumer that receives the producer created by {@link #newEntityProducer()}
   * @return the built entity
   */
  @SuppressWarnings("unchecked")
  E buildEntity(Consumer<C> producerConsumer) {
    B producer = newEntityProducer();
    producerConsumer.accept((C) producer);
    return producer.build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    EntityType<?, ?, ?> that = (EntityType<?, ?, ?>) o;
    return valueType == that.valueType;
  }

  @Override
  public int hashCode() {
    return valueType.hashCode();
  }

  @Override
  public String toString() {
    return "EntityType{" + "valueType=" + valueType + '}';
  }
}
