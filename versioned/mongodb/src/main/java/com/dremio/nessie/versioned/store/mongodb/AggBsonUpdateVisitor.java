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

import java.util.List;
import java.util.stream.Collectors;

import org.bson.BsonDocument;
import org.bson.conversions.Bson;

import com.dremio.nessie.versioned.impl.condition.ExpressionPath;
import com.dremio.nessie.versioned.impl.condition.RemoveClause;
import com.dremio.nessie.versioned.impl.condition.SetClause;
import com.dremio.nessie.versioned.impl.condition.UpdateClauseVisitor;
import com.dremio.nessie.versioned.impl.condition.UpdateExpression;
import com.dremio.nessie.versioned.impl.condition.UpdateExpressionVisitor;

/**
 * This provides a separation of generation of BSON expressions from @{UpdateExpression} from the object itself.
 */
class AggBsonUpdateVisitor implements UpdateExpressionVisitor<List<Bson>> {
  private static class StageOp {
    final String path;
    final String index;
    final String operation;

    StageOp(String path, String index, String operation) {
      this.path = path;
      this.index = index;
      this.operation = operation;
    }
  }

  private static class ClauseVisitor implements UpdateClauseVisitor<StageOp> {
    @Override
    public StageOp visit(RemoveClause clause) {
      final String path = clause.getPath().getRoot().accept(BsonPathVisitor.INSTANCE_NO_QUOTE, true);
      if (isArrayPath(clause.getPath())) {
        // Mongo has no way of deleting an element from an array, it by default leaves a NULL element. To remove the
        // element in one operation, splice together the array around the index to remove.
        final int lastIndex = path.lastIndexOf(".");
        final String arrayPath = path.substring(0, lastIndex);
        final String arrayIndex = path.substring(lastIndex + 1);
        return new StageOp(arrayPath, arrayIndex, String.format("{$addFields: {\"%1$s\": {$concatArrays: [{$slice:[ \"$%1$s\", %2$s]}, "
            + "{$slice:[ \"$%1$s\", {$add:[1, %2$s]}, {$size:\"$%1$s\"}]}]}}}",
            arrayPath, arrayIndex));
      }

      return new StageOp(path, "", String.format("{$project: {\"%s\": 0}}", path));
    }

    @Override
    public StageOp visit(SetClause clause) {
      throw new UnsupportedOperationException(String.format("Unsupported SetClause type: %s", clause.getValue().getType().name()));
    }
  }

  private static final ClauseVisitor CLAUSE_VISITOR = new ClauseVisitor();

  @Override
  public List<Bson> visit(UpdateExpression expression) {
    // Sort the operations so the array removals occur in reverse order to avoid indexing problems. Since we use a pipeline,
    // if they occur in increasing order then consider the case of: [a, b, c] with a removal of 0, 1. You would expect
    // a return array of [c], however what occurs is actually:
    //  - remove 0: [a, b, c] -> [b, c]
    //  - remove 1: [b, c] -> [b]
    // if instead they occur in reverse order:
    //  - remove 1: [a, b, c] -> [a, c]
    //  - remove 0: [a, c] -> [c]
    // TODO: opportunity to optimize the removals by collapsing sequential removed indexes into ranges.
    return expression.getClauses().stream()
      .map(c -> c.accept(CLAUSE_VISITOR))
      .sorted((o1, o2) -> {
        final int compare = o2.path.compareTo(o1.path);
        return (compare == 0) ? o2.index.compareTo(o1.index) : compare;
      })
      .map(o -> BsonDocument.parse(o.operation))
      .collect(Collectors.toList());
  }

  /**
   * Determine whether or not the given ExpressionPath refers to an array element.
   * @param path the path to check.
   * @return true if the path is for an array element, false otherwise.
   */
  static boolean isArrayPath(ExpressionPath path) {
    ExpressionPath.PathSegment segment = path.getRoot();
    while (segment.getChild().isPresent()) {
      segment = segment.getChild().get();
    }

    return segment.isPosition();
  }
}
