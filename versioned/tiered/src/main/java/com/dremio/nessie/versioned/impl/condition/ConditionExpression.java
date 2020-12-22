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
import java.util.stream.Collector;
import java.util.stream.Collectors;

import org.immutables.value.Value;

import com.dremio.nessie.versioned.impl.condition.AliasCollector.Aliasable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

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

  /**
   * AND the existing condition with this newly provided condition.
   *
   * @param function The function to AND with.
   * @return The new compound expression.
   */
  public ConditionExpression and(ExpressionFunction function) {
    return ImmutableConditionExpression.builder()
        .addAllFunctions(getFunctions())
        .addFunctions(function)
        .build();
  }

  /**
   * AND the existing condition with this newly provided condition.
   *
   * @param expr The expression to AND with.
   * @return The new compound expression.
   */
  public ConditionExpression and(ConditionExpression expr) {
    return ImmutableConditionExpression.builder()
        .addAllFunctions(getFunctions())
        .addAllFunctions(expr.getFunctions())
        .build();
  }

  /**
   * Collect condition expressions into a single compound condition expression.
   * @return combined update.
   */
  public static final Collector<ConditionExpression, List<ExpressionFunction>, ConditionExpression> toConditionExpression() {
    return Collector.of(
      Lists::newArrayList,
      (o1, l1) -> o1.addAll(l1.getFunctions()),
      (o1, o2) -> {
        o1.addAll(o2);
        return o1;
      },
      o1 -> ImmutableConditionExpression.builder().addAllFunctions(o1).build()
      );
  }

  /**
   * Visit this object given the specific visitor.
   *
   * @param visitor the instance visiting.
   * @param <T> The class to which ConditionExpression is converted
   * @return the converted class
   */
  public <T> T accept(ConditionExpressionVisitor<T> visitor) {
    return visitor.visit(this);
  }
}
