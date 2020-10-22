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

  REF(InternalRef.class, InternalRef.SCHEMA, false, "r"),
  L1(L1.class, com.dremio.nessie.versioned.impl.L1.SCHEMA, "l1"),
  L2(L2.class, com.dremio.nessie.versioned.impl.L2.SCHEMA, "l2"),
  L3(L3.class, com.dremio.nessie.versioned.impl.L3.SCHEMA, "l3"),
  VALUE(InternalValue.class, InternalValue.SCHEMA, "v"),
  KEY_FRAGMENT(Fragment.class, Fragment.SCHEMA, "k"),
  COMMIT_METADATA(InternalCommitMetadata.class, InternalCommitMetadata.SCHEMA, "m");

  public static String SCHEMA_TYPE = "t";

  private final Class<?> objectClass;
  private final SimpleSchema<?> schema;
  private final boolean immutable;
  private final Entity type;

  ValueType(Class<?> objectClass, SimpleSchema<?> schema, String valueName) {
    this(objectClass, schema, true, valueName);
  }

  ValueType(Class<?> objectClass, SimpleSchema<?> schema, boolean immutable, String valueName) {
    this.objectClass = objectClass;
    this.schema = schema;
    this.immutable = immutable;
    this.type = Entity.s(valueName);
  }

  public Class<?> getObjectClass() {
    return objectClass;
  }

  /**
   * Append this type to the provided attribute value map.
   * @param map The map to append to
   * @return A typed map.
   */
  public Map<String, Entity> addType(Map<String, Entity> map) {
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
    Entity loadedType = map.get(SCHEMA_TYPE);
    Id id = Id.fromEntity(map.get(Store.KEY_NAME));
    Preconditions.checkNotNull(loadedType, "Missing type tag for schema for id %s.", id.getHash());
    Preconditions.checkArgument(type.equals(loadedType),
        "Expected schema for id %s to be of type '%s' but is actually '%s'.", id.getHash(), type.s(), loadedType.s());
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
}
