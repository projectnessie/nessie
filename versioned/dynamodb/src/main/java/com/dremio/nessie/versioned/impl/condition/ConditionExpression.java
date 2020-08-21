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

import org.immutables.value.Value;

import com.dremio.nessie.versioned.impl.condition.AliasCollector.Aliasable;
import com.google.common.collect.ImmutableList;

@Value.Immutable
public abstract class ConditionExpression implements Aliasable<ConditionExpression> {

  public abstract List<ExpressionFunction> getFunctions();

  public static ConditionExpression initial() {
    return ImmutableConditionExpression.builder().build();
  }

  public String toConditionExpressionString() {
    return getFunctions().stream().map(ExpressionFunction::asString).collect(Collectors.joining(" AND "));
  }

  @Override
  public ConditionExpression alias(AliasCollector c) {
    return ImmutableConditionExpression.builder()
        .functions(getFunctions().stream().map(f -> f.alias(c)).collect(ImmutableList.toImmutableList()))
        .build();
  }

  public static ConditionExpression of(ExpressionFunction... functions) {
    return ImmutableConditionExpression.builder().addFunctions(functions).build();
  }

  public ConditionExpression and(ExpressionFunction function) {
    return ImmutableConditionExpression.builder()
        .addAllFunctions(getFunctions())
        .addFunctions(function)
        .build();
  }
}
