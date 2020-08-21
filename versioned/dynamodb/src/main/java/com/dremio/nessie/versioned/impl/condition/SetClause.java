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

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

@org.immutables.value.Value.Immutable
public abstract class SetClause implements UpdateClause {

  public abstract ExpressionPath getPath();

  public abstract Value getValue();

  public static SetClause equals(ExpressionPath path, AttributeValue value) {
    return ImmutableSetClause.builder().path(path).value(Value.of(value)).build();
  }

  /**
   * Create a setClause that sets if the existence path does not exist.
   * @param setPath The path to set the value to.
   * @param existenceCheckPath The path to check for existence.
   * @param valueToSet The value to set.
   * @return The clause
   */
  public static SetClause ifNotExists(ExpressionPath setPath, ExpressionPath existenceCheckPath, AttributeValue valueToSet) {
    return ImmutableSetClause.builder().path(setPath).value(
        Value.of(
            ExpressionFunction.ifNotExists(existenceCheckPath, valueToSet))
        ).build();
  }

  public static SetClause appendToList(ExpressionPath path, AttributeValue value) {
    return ImmutableSetClause.builder().path(path).value(Value.of(ExpressionFunction.appendToList(path, value))).build();
  }

  @Override
  public SetClause alias(AliasCollector c) {
    return ImmutableSetClause.builder().path(getPath().alias(c)).value(getValue().alias(c)).build();
  }

  @Override
  public Type getType() {
    return Type.SET;
  }

  @Override
  public String toClauseString() {
    return String.format("%s = %s", getPath().asString(), getValue().asString());
  }


}
