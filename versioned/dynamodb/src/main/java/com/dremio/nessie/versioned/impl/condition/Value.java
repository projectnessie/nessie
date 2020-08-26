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
package com.dremio.nessie.versioned.impl.condition;

import com.dremio.nessie.versioned.impl.condition.AliasCollector.Aliasable;
import com.google.common.base.Preconditions;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * A container class that will contain one of the possible value types in DynamoDB's expression language.
 */
public class Value implements Aliasable<Value> {
  private final AttributeValue value;
  private final ExpressionPath path;
  private final ExpressionFunction func;

  private Value(AttributeValue value, ExpressionPath path, ExpressionFunction func) {
    int i = 0;
    if (value != null) {
      i++;
    }
    if (path != null) {
      i++;
    }

    if (func != null) {
      i++;
    }

    Preconditions.checkArgument(i == 1, "Only one of value, path or function can be defined.");
    this.value = value;
    this.path = path;
    this.func = func;
  }

  public static Value of(AttributeValue value) {
    return new Value(value, null, null);
  }

  public static Value of(ExpressionPath path) {
    return new Value(null, path, null);
  }

  public static Value of(ExpressionFunction func) {
    return new Value(null, null, func);
  }

  @Override
  public Value alias(AliasCollector c) {
    switch (getType()) {
      case VALUE: return new Value(null, ExpressionPath.builder(c.alias(value)).build(), null);
      case PATH: return new Value(null, path.alias(c), null);
      case FUNCTION: return new Value(null, null, func.alias(c));
      default: throw new IllegalStateException();
    }
  }

  /**
   * Return the string representation of this string, if possible.
   * @return A DynamoDb expression fragment.
   */
  public String asString() {
    Preconditions.checkArgument(value == null, "Values must be aliased before conversion to string.");
    if (path != null) {
      return path.asString();
    }

    return func.asString();
  }

  /**
   * Return the value type of this value.
   * @return A value type.
   */
  public Type getType() {
    if (value != null) {
      return Type.VALUE;
    }

    if (path != null) {
      return Type.PATH;
    }

    return Type.FUNCTION;
  }

  public AttributeValue getValue() {
    Preconditions.checkArgument(Type.VALUE == getType());
    return value;
  }

  public ExpressionPath getPath() {
    Preconditions.checkArgument(Type.PATH == getType());
    return path;
  }

  public ExpressionFunction getFunction() {
    Preconditions.checkArgument(Type.FUNCTION == getType());
    return func;
  }

  public static enum Type {
    VALUE, PATH, FUNCTION;
  }
}
