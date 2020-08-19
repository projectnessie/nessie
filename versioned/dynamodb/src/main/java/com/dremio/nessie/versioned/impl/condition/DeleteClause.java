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

/**
 * Remove items from a set.
 */
@org.immutables.value.Value.Immutable
public abstract class DeleteClause implements UpdateClause {

  abstract ExpressionPath getPath();

  abstract Value getSetToRemove();

  /**
   * Specify a value or set of values to remove from a set.
   * @param path The path to the set to remove values from.
   * @param setToRemove The actual values to remove. Must be a set of values of the same type as those stored at the path above.
   * @return The constructed DeleteClause.
   */
  public static DeleteClause deleteFromSet(ExpressionPath path, AttributeValue setToRemove) {
    return ImmutableDeleteClause.builder().path(path).setToRemove(Value.of(setToRemove)).build();
  }

  @Override
  public DeleteClause alias(AliasCollector c) {
    ExpressionPath newPath = getPath().alias(c);
    Value value = getSetToRemove().alias(c);
    return ImmutableDeleteClause.builder().path(newPath).setToRemove(value).build();
  }

  @Override
  public Type getType() {
    return Type.DELETE;
  }

  @Override
  public String toClauseString() {
    return getPath().asString() + " " + getSetToRemove().asString();
  }



}
