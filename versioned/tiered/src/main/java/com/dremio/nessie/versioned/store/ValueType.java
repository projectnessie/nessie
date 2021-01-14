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

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.dremio.nessie.tiered.builder.BaseConsumer;
import com.dremio.nessie.versioned.impl.EntityLoadOps;
import com.dremio.nessie.versioned.impl.Fragment;
import com.dremio.nessie.versioned.impl.InternalCommitMetadata;
import com.dremio.nessie.versioned.impl.InternalRef;
import com.dremio.nessie.versioned.impl.InternalValue;
import com.dremio.nessie.versioned.impl.L1;
import com.dremio.nessie.versioned.impl.L2;
import com.dremio.nessie.versioned.impl.L3;
import com.dremio.nessie.versioned.impl.PersistentBase;
import com.dremio.nessie.versioned.impl.PersistentBase.EntityBuilder;

public enum ValueType {

  REF(InternalRef.class, "r", "refs", InternalRef::builder),
  L1(L1.class, "l1", "l1", com.dremio.nessie.versioned.impl.L1::builder),
  L2(L2.class, "l2", "l2", com.dremio.nessie.versioned.impl.L2::builder),
  L3(L3.class, "l3", "l3", com.dremio.nessie.versioned.impl.L3::builder),
  VALUE(InternalValue.class, "v", "values", InternalValue::builder),
  KEY_FRAGMENT(Fragment.class, "k", "key_lists", Fragment::builder),
  COMMIT_METADATA(InternalCommitMetadata.class, "m", "commit_metadata", InternalCommitMetadata::builder);

  /**
   * Schema type field name "{@value #SCHEMA_TYPE}".
   */
  public static final String SCHEMA_TYPE = "t";

  private static final Map<String, ValueType> byValueName = new HashMap<>();

  static {
    for (ValueType type : ValueType.values()) {
      byValueName.put(type.valueName, type);
    }
  }

  private final Class<?> objectClass;
  private final String valueName;
  private final String defaultTableSuffix;
  private final Supplier<BaseConsumer<?>> producerSupplier;

  ValueType(Class<?> objectClass, String valueName,
      String defaultTableSuffix, Supplier<BaseConsumer<?>> producerSupplier) {
    this.objectClass = objectClass;
    this.valueName = valueName;
    this.defaultTableSuffix = defaultTableSuffix;
    this.producerSupplier = producerSupplier;
  }

  /**
   * Get the {@link ValueType} by its {@code valueName} as given in the {@link #SCHEMA_TYPE} field.
   *
   * @param t the schema-type value
   * @return the matching value-type
   * @throws IllegalArgumentException if no value-type matches
   */
  public static ValueType byValueName(String t) {
    ValueType type = byValueName.get(t);
    if (type == null) {
      throw new IllegalArgumentException("No ValueType for table '" + t + "'");
    }
    return type;
  }

  /**
   * Get the value of this {@link ValueType} as persisted in the {@link #SCHEMA_TYPE} field.
   *
   * @return value-name for this value-type
   */
  public String getValueName() {
    return valueName;
  }

  /**
   * Get the object class associated with this {@code ValueType}.
   *
   * @return The object class for this {@code ValueType}.
   */
  public Class<?> getObjectClass() {
    return objectClass;
  }

  /**
   * Get the name of the table for this object optionally added the provided prefix.
   *
   * @param prefix The prefix to append (if defined and non-empty).
   * @return The complete table name for this {@code ValueType}.
   */
  public String getTableName(String prefix) {
    if (prefix == null || prefix.isEmpty()) {
      return defaultTableSuffix;
    }

    return prefix + defaultTableSuffix;
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
   * @param <C> the consumer interface
   * @return new created producer instance
   */
  public <C extends BaseConsumer<C>> C newEntityProducer() {
    @SuppressWarnings("unchecked") C p = (C) producerSupplier.get();
    return p;
  }

  /**
   * Used to create a new entity instance for this value-type using a consumer retrieved via
   * {@link #newEntityProducer()}. Passing in an instance that has not been retrieved via
   * {@link #newEntityProducer()} will result in an {@link IllegalArgumentException}.
   *
   * @param producer a producer retrieved via {@link #newEntityProducer()}
   * @param <E> the entity type
   * @param <C> the consumer interface
   * @return the built entity
   */
  public <E extends HasId, C extends BaseConsumer<C>> E buildFromProducer(BaseConsumer<C> producer) {
    if (!(producer instanceof PersistentBase.EntityBuilder)) {
      throw new IllegalArgumentException("Given producer " + producer + " has not been created via ValueType.newEntityProducer()");
    }
    @SuppressWarnings("unchecked") EntityBuilder<E> builder = (EntityBuilder<E>) producer;
    return builder.build();
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
   * @param <E> the entity type
   * @param <C> the consumer interface
   * @return the built entity
   */
  public <E extends HasId, C extends BaseConsumer<C>> E buildEntity(Consumer<C> producerConsumer) {
    C producer = newEntityProducer();
    producerConsumer.accept(producer);
    return buildFromProducer(producer);
  }

  /**
   * Check whether {@code value} is an instance of the entity class represented by this value-type.
   *
   * @param value entity instance to check
   * @param <V> expected value type
   * @return whether {@code value} is an instance of the entity class represented by this value-type
   */
  public <V extends HasId> boolean isEntityType(V value) {
    return objectClass.isAssignableFrom(value.getClass());
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public <C extends BaseConsumer<C>> SaveOp<C> createSaveOp(Supplier<Id> idSupplier, Consumer<C> serializer) {
    return new SaveOp(this, idSupplier, serializer);
  }

  public <C extends BaseConsumer<C>, V extends HasId> SaveOp<C> createSaveOpForEntity(V value) {
    return createSaveOp(value::getId, c -> ((PersistentBase) value).applyToConsumer(c));
  }

  /**
   * Creates a new {@link LoadOp} for this value-type.
   * <p>
   * To load entity instances, use {@link EntityLoadOps}.
   * </p>
   *
   * @param id the ID to load
   * @param producer the {@link BaseConsumer instance} that receives the load-events of the entity
   * @param produced Gets called with the value passed in as {@code producer}, when deserialization has finished.
   * @param <C> consumer-type
   * @return load operation
   */
  public <C extends BaseConsumer<C>> LoadOp<C> createLoadOp(Id id, C producer, Consumer<C> produced) {
    return new LoadOp<>(this, id, producer, produced);
  }
}
