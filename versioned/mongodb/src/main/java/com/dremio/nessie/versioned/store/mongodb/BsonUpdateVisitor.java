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
package com.dremio.nessie.versioned.store.mongodb;

import org.bson.conversions.Bson;

import com.dremio.nessie.versioned.impl.condition.AddClause;
import com.dremio.nessie.versioned.impl.condition.ExpressionPath;
import com.dremio.nessie.versioned.impl.condition.RemoveClause;
import com.dremio.nessie.versioned.impl.condition.SetClause;
import com.dremio.nessie.versioned.impl.condition.UpdateClauseVisitor;
import com.dremio.nessie.versioned.impl.condition.UpdateExpression;
import com.dremio.nessie.versioned.impl.condition.UpdateExpressionVisitor;
import com.dremio.nessie.versioned.impl.condition.Value;
import com.google.common.collect.ImmutableList;
import com.mongodb.BasicDBObject;
import com.mongodb.client.model.Updates;

/**
 * This provides a separation of generation of BSON expressions from @{UpdateExpression} from the object itself.
 */
class BsonUpdateVisitor implements UpdateExpressionVisitor<Bson> {
  private static final class ClauseVisitor implements UpdateClauseVisitor<Bson> {
    @Override
    public Bson visit(AddClause clause) {
      final String path = clause.getPath().getRoot().accept(BsonPathVisitor.INSTANCE, true);
      if (isArrayPath(clause.getPath())) {
        return Updates.push(path, clause.getValue());
      }

      return Updates.set(path, clause.getValue());
    }

    @Override
    public Bson visit(RemoveClause clause) {
      final String path = clause.getPath().getRoot().accept(BsonPathVisitor.INSTANCE, true);
      return Updates.unset(path);
    }

    @Override
    public Bson visit(SetClause clause) {
      final String path = clause.getPath().getRoot().accept(BsonPathVisitor.INSTANCE, true);

      if (Value.Type.VALUE == clause.getValue().getType()) {
        return Updates.set(path, clause.getValue());
      }

      return new BasicDBObject(path, clause.getValue().accept(BsonConditionVisitor.VALUE_VISITOR));
    }

    private boolean isArrayPath(ExpressionPath path) {
      ExpressionPath.PathSegment segment = path.getRoot();
      while (segment.getChild().isPresent()) {
        segment = segment.getChild().get();
      }

      return segment.isPosition();
    }
  }

  private static final ClauseVisitor CLAUSE_VISITOR = new ClauseVisitor();

  @Override
  public Bson visit(UpdateExpression expression) {
    final ImmutableList.Builder<Bson> builder = ImmutableList.builder();
    expression.getClauses().forEach(c -> builder.add(c.accept(CLAUSE_VISITOR)));
    return Updates.combine(builder.build());
  }
}
