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

@Value.Immutable
public abstract class ProjectionExpression implements Aliasable<ProjectionExpression> {
  public abstract List<ExpressionPath> getPaths();

  static ImmutableProjectionExpression.Builder builder() {
    return ImmutableProjectionExpression.builder();
  }

  @Override
  public String toString() {
    return "ProjectionExpression [" + toProjectionExpression() + "]";
  }

  public String toProjectionExpression() {
    return getPaths().stream().map(p -> p.asString()).collect(Collectors.joining(", "));
  }

  @Override
  public ProjectionExpression alias(AliasCollector c) {
    return ImmutableProjectionExpression.builder().paths(getPaths().stream().map(p -> p.alias(c)).collect(Collectors.toList())).build();
  }

}
