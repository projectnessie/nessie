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

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.dremio.nessie.tiered.builder.BaseConsumer;
import com.dremio.nessie.tiered.builder.CommitMetadataConsumer;
import com.dremio.nessie.tiered.builder.FragmentConsumer;
import com.dremio.nessie.tiered.builder.L1Consumer;
import com.dremio.nessie.tiered.builder.L2Consumer;
import com.dremio.nessie.tiered.builder.L3Consumer;
import com.dremio.nessie.tiered.builder.RefConsumer;
import com.dremio.nessie.tiered.builder.ValueConsumer;
import com.dremio.nessie.versioned.impl.PersistentBase.EntityBuilder;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.NotFoundException;
import com.dremio.nessie.versioned.store.SaveOp;
import com.dremio.nessie.versioned.store.Store;
import com.dremio.nessie.versioned.store.StoreOperationException;
import com.dremio.nessie.versioned.store.ValueType;

final class EntityType<C extends BaseConsumer<C>, E extends PersistentBase<C>> {

  static final Map<ValueType<?>, EntityType<?, ?>> byValueType = new HashMap<>();

  static final EntityType<RefConsumer, InternalRef> REF = new EntityType<>(ValueType.REF, InternalRef.Builder::new);
  static final EntityType<L1Consumer, L1> L1 = new EntityType<>(ValueType.L1, com.dremio.nessie.versioned.impl.L1.Builder::new);
  static final EntityType<L2Consumer, L2> L2 = new EntityType<>(ValueType.L2, com.dremio.nessie.versioned.impl.L2.Builder::new);
  static final EntityType<L3Consumer, L3> L3 = new EntityType<>(ValueType.L3, com.dremio.nessie.versioned.impl.L3.Builder::new);
  static final EntityType<ValueConsumer, InternalValue> VALUE = new EntityType<>(ValueType.VALUE, InternalValue.Builder::new);
  static final EntityType<FragmentConsumer, Fragment> KEY_FRAGMENT = new EntityType<>(ValueType.KEY_FRAGMENT, Fragment.Builder::new);
  static final EntityType<CommitMetadataConsumer, InternalCommitMetadata> COMMIT_METADATA = new EntityType<>(ValueType.COMMIT_METADATA,
      InternalCommitMetadata.Builder::new);

  final ValueType<C> valueType;
  final Supplier<C> producerSupplier;

  private EntityType(ValueType<C> valueType, Supplier<C> producerSupplier) {
    this.valueType = valueType;
    this.producerSupplier = producerSupplier;
    byValueType.put(valueType, this);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  static <C extends BaseConsumer<C>, E extends PersistentBase<C>> EntityType<C, E> forType(ValueType<C> type) {
    return (EntityType) byValueType.get(type);
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
   * The instance returned from this function can, after all necessary properties have been
   * set, be passed to {@link #buildFromProducer(BaseConsumer)} to build the entity instance.
   * </p>
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
  C newEntityProducer() {
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
  E buildEntity(Consumer<C> producerConsumer) {
    C producer = newEntityProducer();
    producerConsumer.accept(producer);
    return buildFromProducer(producer);
  }

  /**
   * Used to create a new entity instance for this value-type using a consumer retrieved via
   * {@link #newEntityProducer()}. Passing in an instance that has not been retrieved via
   * {@link #newEntityProducer()} will result in an {@link IllegalArgumentException}.
   *
   * @param producer a producer retrieved via {@link #newEntityProducer()}
   * @return the built entity
   */
  E buildFromProducer(BaseConsumer<C> producer) {
    if (!(producer instanceof PersistentBase.EntityBuilder)) {
      throw new IllegalArgumentException("Given producer " + producer + " has not been created via ValueType.newEntityProducer()");
    }
    @SuppressWarnings("unchecked") EntityBuilder<E> builder = (EntityBuilder<E>) producer;
    return builder.build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    EntityType<?, ?> that = (EntityType<?, ?>) o;
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
