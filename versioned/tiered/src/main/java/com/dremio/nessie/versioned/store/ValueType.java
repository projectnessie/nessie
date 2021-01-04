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

import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import com.dremio.nessie.tiered.builder.HasIdConsumer;
import com.dremio.nessie.tiered.builder.Producer;
import com.dremio.nessie.versioned.impl.Fragment;
import com.dremio.nessie.versioned.impl.InternalCommitMetadata;
import com.dremio.nessie.versioned.impl.InternalRef;
import com.dremio.nessie.versioned.impl.InternalValue;
import com.dremio.nessie.versioned.impl.L1;
import com.dremio.nessie.versioned.impl.L2;
import com.dremio.nessie.versioned.impl.L3;
import com.dremio.nessie.versioned.impl.condition.ConditionExpression;
import com.dremio.nessie.versioned.impl.condition.ExpressionFunction;
import com.dremio.nessie.versioned.impl.condition.ExpressionPath;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

public enum ValueType {

  REF(InternalRef.class, InternalRef.SCHEMA, false, "r", "refs", com.dremio.nessie.versioned.impl.InternalRef::builder),
  L1(L1.class, com.dremio.nessie.versioned.impl.L1.SCHEMA, "l1", "l1", com.dremio.nessie.versioned.impl.L1::builder),
  L2(L2.class, com.dremio.nessie.versioned.impl.L2.SCHEMA, "l2", "l2", com.dremio.nessie.versioned.impl.L2::builder),
  L3(L3.class, com.dremio.nessie.versioned.impl.L3.SCHEMA, "l3", "l3", com.dremio.nessie.versioned.impl.L3::builder),
  VALUE(InternalValue.class, InternalValue.SCHEMA, "v", "values", com.dremio.nessie.versioned.impl.InternalValue::builder),
  KEY_FRAGMENT(Fragment.class, Fragment.SCHEMA, "k", "key_lists", com.dremio.nessie.versioned.impl.Fragment::builder),
  COMMIT_METADATA(InternalCommitMetadata.class, InternalCommitMetadata.SCHEMA, "m", "commit_metadata",
      com.dremio.nessie.versioned.impl.InternalCommitMetadata::builder);

  public static String SCHEMA_TYPE = "t";

  private final Class<?> objectClass;
  private final SimpleSchema<?> schema;
  private final boolean immutable;
  private final String valueName;
  private final Entity type;
  private final String defaultTableSuffix;
  private final Supplier<Producer<?, ?>> producerSupplier;

  ValueType(Class<?> objectClass, SimpleSchema<?> schema, String valueName,
      String defaultTableSuffix, Supplier<Producer<?, ?>> producerSupplier) {
    this(objectClass, schema, true, valueName, defaultTableSuffix, producerSupplier);
  }

  ValueType(Class<?> objectClass, SimpleSchema<?> schema, boolean immutable, String valueName,
      String defaultTableSuffix, Supplier<Producer<?, ?>> producerSupplier) {
    this.objectClass = objectClass;
    this.schema = schema;
    this.immutable = immutable;
    this.valueName = valueName;
    this.type = Entity.ofString(valueName);
    this.defaultTableSuffix = defaultTableSuffix;
    this.producerSupplier = producerSupplier;
  }

  /**
   * TODO maybe remove this method once `Entity` has been removed.
   */
  public static ValueType byValueName(String t) {
    for (ValueType value : ValueType.values()) {
      if (value.valueName.equals(t)) {
        return value;
      }
    }
    throw new IllegalArgumentException("No ValueType for table '" + t + "'");
  }

  /**
   * TODO javadoc for checkstyle.
   */
  public String getValueName() {
    return valueName;
  }

  /**
   * Get the object class associated with this {@code ValueType}.
   * @return The object class for this {@code ValueType}.
   */
  public Class<?> getObjectClass() {
    return objectClass;
  }

  /**
   * Get the name of the table for this object optionally added the provided prefix.
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
   * Append this type to the provided attribute value map.
   * @param map The map to append to
   * @return A typed map.
   */
  // TODO Remove once `Entity` is out.
  public Map<String, Entity> addType(Map<String, Entity> map) {
    Preconditions.checkNotNull(map, "map parameter is null");
    return ImmutableMap.<String, Entity>builder()
        .putAll(map)
        .put(ValueType.SCHEMA_TYPE, type).build();
  }

  /**
   * Validate that the provided map includes the expected type.
   * @param map The map to check
   * @return The map passed in (for chaining)
   */
  public Map<String, Entity> checkType(Map<String, Entity> map) {
    Preconditions.checkNotNull(map, "map parameter is null");
    Entity loadedType = map.get(SCHEMA_TYPE);
    Id id = Id.fromEntity(map.get(Store.KEY_NAME));
    Preconditions.checkNotNull(loadedType, "Missing type tag for schema for id %s.", id.getHash());
    Preconditions.checkArgument(type.equals(loadedType),
        "Expected schema for id %s to be of type '%s' but is actually '%s'.", id.getHash(), type.getString(), loadedType.getString());
    return map;
  }

  public ConditionExpression addTypeCheck(Optional<ConditionExpression> possibleExpression) {
    final ExpressionFunction checkType = ExpressionFunction.equals(ExpressionPath.builder(SCHEMA_TYPE).build(), type);
    return possibleExpression.map(ce -> ce.and(checkType)).orElse(ConditionExpression.of(checkType));
  }

  @SuppressWarnings("unchecked")
  public <T> SimpleSchema<T> getSchema() {
    return (SimpleSchema<T>) schema;
  }

  public boolean isImmutable() {
    return immutable;
  }

  /**
   * Create a new consumer for this type.
   */
  // TODO this is currently unused - does it provide any value?
  @SuppressWarnings("unchecked")
  public <E extends HasId, C extends HasIdConsumer<C>> Producer<E, C> newEntityProducer() {
    Producer<?, ?> p = producerSupplier.get();
    return (Producer<E, C>) p;
  }
}
