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
package com.dremio.nessie.versioned.store.rocksdb;

import com.dremio.nessie.versioned.store.Entity;

/**
 * An expression that is asserted against an Entity.
 */
class Function {
  public static final String EQUALS = "equals";
  public static final String SIZE = "size";

  private final String operator;
  private final String path;
  private final Entity value;

  /**
   * ExpressionFunction representation.
   * @param operator type of assertion to be applied
   * @param path the path to be tested
   * @param value the expected value of path
   */
  Function(String operator, String path, Entity value) {
    this.operator = operator;
    this.path = path;
    this.value = value;
  }

  /**
   * Compares for equality with a provided Function object.
   * @param function object to compare
   * @return true if this is equal to provided object
   */
  boolean equals(Function function) {
    return (this.operator.equals(function.operator)
        && this.path.equals(function.path)
        && this.value.equals(function.value));
  }

  String getOperator() {
    return operator;
  }

  String getPath() {
    return path;
  }

  Entity getValue() {
    return value;
  }
}
