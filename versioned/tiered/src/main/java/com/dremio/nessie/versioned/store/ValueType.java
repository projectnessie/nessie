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

public enum ValueType {

  REF("r", "refs"),
  L1("l1", "l1"),
  L2("l2", "l2"),
  L3("l3", "l3"),
  VALUE("v", "values"),
  KEY_FRAGMENT("k", "key_lists"),
  COMMIT_METADATA("m", "commit_metadata");

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

  private final String valueName;
  private final String defaultTableSuffix;

  ValueType(String valueName, String defaultTableSuffix) {
    this.valueName = valueName;
    this.defaultTableSuffix = defaultTableSuffix;
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
}
