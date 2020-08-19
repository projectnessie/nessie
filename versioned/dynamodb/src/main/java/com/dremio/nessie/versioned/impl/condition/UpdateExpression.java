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
import com.dremio.nessie.versioned.impl.condition.UpdateClause.Type;
import com.google.common.base.Preconditions;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;

@Value.Immutable
public abstract class UpdateExpression implements Aliasable<UpdateExpression> {

  public abstract List<UpdateClause> getClauses();

  @Override
  public UpdateExpression alias(AliasCollector c) {
    return ImmutableUpdateExpression.builder()
        .clauses(getClauses().stream().map(f -> f.alias(c)).collect(Collectors.toList()))
        .build();
  }

  /**
   * Generate the expression string used for an update.
   * @return The update expression of this object.
   */
  public String toUpdateExpressionString() {
    Preconditions.checkArgument(!getClauses().isEmpty(), "At least one clauses must be defined.");
    ListMultimap<UpdateClause.Type, UpdateClause> clauses = Multimaps.index(getClauses(), c -> c.getType());
    StringBuilder sb = new StringBuilder();
    addIfExist(sb, clauses.get(Type.ADD), Type.ADD);
    addIfExist(sb, clauses.get(Type.SET), Type.SET);
    addIfExist(sb, clauses.get(Type.REMOVE), Type.REMOVE);
    addIfExist(sb, clauses.get(Type.DELETE), Type.DELETE);
    return sb.toString();
  }

  private void addIfExist(StringBuilder sb, List<UpdateClause> clauses, UpdateClause.Type type) {
    if (clauses == null || clauses.isEmpty()) {
      return;
    }
    sb.append(" ");
    sb.append(type.name());
    sb.append(" ");
    sb.append(clauses.stream().map(UpdateClause::toClauseString).collect(Collectors.joining(", ")));
  }

  public static UpdateExpression of(UpdateClause... clauses) {
    return ImmutableUpdateExpression.builder().addClauses(clauses).build();
  }

}
