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

import java.util.Objects;

import org.immutables.value.Value.Immutable;

import com.dremio.nessie.versioned.impl.condition.ExpressionPath;
import com.dremio.nessie.versioned.store.Entity;

/**
 * An expression that is asserted against an Entity.
 */
@Immutable
abstract class Function {
  static final String EQUALS = "equals";
  static final String SIZE = "size";

  /**
   * Compares for equality with a provided Function object.
   * @param object  the object to compare
   * @return true if this is equal to provided object
   */
  @Override
  public boolean equals(Object object) {
    if (object == null) {
      return false;
    }

    if (object == this) {
      return true;
    }

    if (!(object instanceof Function)) {
      return false;
    }

    Function function = (Function) object;
    return (getOperator().equals(function.getOperator())
        && getPath().equals(function.getPath())
        && getValue().equals(function.getValue()));
  }

  @Override
  public int hashCode() {
    return Objects.hash(getOperator(), getPath(), getValue());
  }

  abstract String getOperator();

  abstract ExpressionPath getPath();

  abstract Entity getValue();

  /**
   * Evaluates if this expression is for equality.
   * @return true if this function relates to a EQUALS evaluation.
   */
  boolean isEquals() {
    return getOperator().equals(EQUALS);
  }

  /**
   * Evaluates if this expression is for size.
   * @return true if this function relates to a SIZE evaluation.
   */
  boolean isSize() {
    return getOperator().equals(SIZE);
  }

  /**
   * Builds an immutable representation of this class.
   * @return the builder
   */
  public static ImmutableFunction.Builder builder() {
    return ImmutableFunction.builder();
  }
}
