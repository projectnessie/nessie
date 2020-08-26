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

import java.util.List;
import java.util.stream.Collectors;

import com.dremio.nessie.versioned.impl.condition.AliasCollector.Aliasable;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class ExpressionFunction implements Aliasable<ExpressionFunction> {

  private static enum FunctionName {
    LIST_APPEND("list_append", 2),
    IF_NOT_EXISTS("if_not_exists", 2),
    EQUALS("="),
    // Not yet implemented below here.
    //  ATTRIBUTE_EXISTS("attribute_exists", 1),
    //  ATTRIBUTE_NOT_EXISTS("attribute_not_exists", 1),
    //  ATTRIBUTE_TYPE("attribute_type", 1),
    //  BEGINS_WITH("begins_with", 2),
    //  CONTAINS("contains", 2),
    //  SIZE("size", 1),
    //    GT(">"),
    //    LT("<"),
    //    LTE("<="),
    //    GTE(">=")
    ;

    String protocolName;
    int argCount;
    boolean binaryExpression;

    FunctionName(String text, int argCount) {
      this(text, argCount, false);
    }

    FunctionName(String text, int argCount, boolean binaryExpression) {
      this.protocolName = text;
      this.argCount = argCount;
      this.binaryExpression = binaryExpression;
    }

    FunctionName(String text) {
      this(text, 2, true);
    }
  }

  private final FunctionName name;
  private final List<Value> arguments;

  private ExpressionFunction(FunctionName name, ImmutableList<Value> arguments) {
    this.name = name;
    this.arguments = ImmutableList.copyOf(arguments);
    Preconditions.checkArgument(this.arguments.size() == name.argCount, "Unexpected argument count.");
  }

  public static ExpressionFunction appendToList(ExpressionPath initialList, AttributeValue valueToAppend) {
    return new ExpressionFunction(FunctionName.LIST_APPEND, ImmutableList.of(Value.of(initialList), Value.of(valueToAppend)));
  }

  public static ExpressionFunction equals(ExpressionPath path, AttributeValue value) {
    return new ExpressionFunction(FunctionName.EQUALS, ImmutableList.of(Value.of(path), Value.of(value)));
  }

  public static ExpressionFunction ifNotExists(ExpressionPath path, AttributeValue value) {
    return new ExpressionFunction(FunctionName.IF_NOT_EXISTS, ImmutableList.of(Value.of(path), Value.of(value)));
  }

  @Override
  public ExpressionFunction alias(AliasCollector c) {
    return new ExpressionFunction(name, arguments.stream().map(v -> v.alias(c)).collect(ImmutableList.toImmutableList()));
  }

  /**
   * Return this function as a Dynamo expression string.
   * @return The expression string.
   */
  public String asString() {
    if (name.binaryExpression) {
      return String.format("%s %s %s", arguments.get(0).asString(), name.protocolName, arguments.get(1).asString());
    }
    return String.format("%s(%s)", name.protocolName, arguments.stream().map(v -> v.asString()).collect(Collectors.joining(", ")));
  }

}
