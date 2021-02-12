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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.projectnessie.versioned.tiered.BaseValue;
import org.projectnessie.versioned.tiered.CommitMetadata;
import org.projectnessie.versioned.tiered.Fragment;
import org.projectnessie.versioned.tiered.L1;
import org.projectnessie.versioned.tiered.L2;
import org.projectnessie.versioned.tiered.L3;
import org.projectnessie.versioned.tiered.Ref;
import org.projectnessie.versioned.tiered.Value;

public final class ValueType<C extends BaseValue<C>> {

  public static final ValueType<Ref> REF = new ValueType<>(Ref.class, "r", "refs");
  public static final ValueType<L1> L1 = new ValueType<>(L1.class, "l1", "l1");
  public static final ValueType<L2> L2 = new ValueType<>(L2.class, "l2", "l2");
  public static final ValueType<L3> L3 = new ValueType<>(L3.class, "l3", "l3");
  public static final ValueType<Value> VALUE = new ValueType<>(Value.class, "v", "values");
  public static final ValueType<Fragment> KEY_FRAGMENT = new ValueType<>(Fragment.class, "k", "key_lists");
  public static final ValueType<CommitMetadata> COMMIT_METADATA = new ValueType<>(CommitMetadata.class, "m", "commit_metadata");

  private static final ValueType<?>[] ALL = new ValueType[]{REF, L1, L2, L3, VALUE, KEY_FRAGMENT, COMMIT_METADATA};

  /**
   * Schema type field name "{@value #SCHEMA_TYPE}".
   */
  public static final String SCHEMA_TYPE = "t";

  private static final Map<String, ValueType<?>> BY_VALUE_NAME = new HashMap<>();

  static {
    for (ValueType<?> type : ALL) {
      BY_VALUE_NAME.put(type.valueName, type);
    }
  }

  private final String valueName;
  private final String defaultTableSuffix;
  private final Class<C> clazz;

  private ValueType(Class<C> clazz, String valueName, String defaultTableSuffix) {
    this.valueName = valueName;
    this.defaultTableSuffix = defaultTableSuffix;
    this.clazz = clazz;
  }

  public Class<C> getValueClass() {
    return clazz;
  }

  public static List<ValueType<?>> values() {
    return Arrays.asList(ALL);
  }

  /**
   * Get the {@link ValueType} by its {@code valueName} as given in the {@link #SCHEMA_TYPE} field.
   *
   * @param t the schema-type value
   * @return the matching value-type
   * @throws IllegalArgumentException if no value-type matches
   */
  public static ValueType<?> byValueName(String t) {
    ValueType<?> type = BY_VALUE_NAME.get(t);
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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ValueType<?> valueType = (ValueType<?>) o;
    return valueName.equals(valueType.valueName);
  }

  @Override
  public int hashCode() {
    return valueName.hashCode();
  }

  public String name() {
    return defaultTableSuffix;
  }

  @Override
  public String toString() {
    return defaultTableSuffix;
  }
}
