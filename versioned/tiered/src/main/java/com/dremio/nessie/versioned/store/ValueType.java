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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.dremio.nessie.tiered.builder.BaseConsumer;
import com.dremio.nessie.tiered.builder.CommitMetadataConsumer;
import com.dremio.nessie.tiered.builder.FragmentConsumer;
import com.dremio.nessie.tiered.builder.L1Consumer;
import com.dremio.nessie.tiered.builder.L2Consumer;
import com.dremio.nessie.tiered.builder.L3Consumer;
import com.dremio.nessie.tiered.builder.RefConsumer;
import com.dremio.nessie.tiered.builder.ValueConsumer;

public final class ValueType<C extends BaseConsumer<C>> {

  public static final ValueType<RefConsumer> REF = new ValueType<>("r", "refs");
  public static final ValueType<L1Consumer> L1 = new ValueType<>("l1", "l1");
  public static final ValueType<L2Consumer> L2 = new ValueType<>("l2", "l2");
  public static final ValueType<L3Consumer> L3 = new ValueType<>("l3", "l3");
  public static final ValueType<ValueConsumer> VALUE = new ValueType<>("v", "values");
  public static final ValueType<FragmentConsumer> KEY_FRAGMENT = new ValueType<>("k", "key_lists");
  public static final ValueType<CommitMetadataConsumer> COMMIT_METADATA = new ValueType<>("m", "commit_metadata");

  private static final ValueType<?>[] ALL = new ValueType[]{REF, L1, L2, L3, VALUE, KEY_FRAGMENT, COMMIT_METADATA};

  /**
   * Schema type field name "{@value #SCHEMA_TYPE}".
   */
  public static final String SCHEMA_TYPE = "t";

  private static final Map<String, ValueType<?>> byValueName = new HashMap<>();

  static {
    for (ValueType<?> type : ALL) {
      byValueName.put(type.valueName, type);
    }
  }

  private final String valueName;
  private final String defaultTableSuffix;

  private ValueType(String valueName, String defaultTableSuffix) {
    this.valueName = valueName;
    this.defaultTableSuffix = defaultTableSuffix;
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
    ValueType<?> type = byValueName.get(t);
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
