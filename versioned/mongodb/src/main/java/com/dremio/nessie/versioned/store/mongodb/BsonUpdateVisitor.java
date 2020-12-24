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

import org.bson.BsonDocument;
import org.bson.BsonDocumentWriter;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import com.dremio.nessie.versioned.impl.condition.ExpressionFunction;
import com.dremio.nessie.versioned.impl.condition.ExpressionPath;
import com.dremio.nessie.versioned.impl.condition.RemoveClause;
import com.dremio.nessie.versioned.impl.condition.SetClause;
import com.dremio.nessie.versioned.impl.condition.UpdateClauseVisitor;
import com.dremio.nessie.versioned.impl.condition.UpdateExpression;
import com.dremio.nessie.versioned.impl.condition.UpdateExpressionVisitor;
import com.dremio.nessie.versioned.store.Entity;
import com.google.common.collect.ImmutableList;
import com.mongodb.client.model.Updates;

/**
 * This provides a separation of generation of BSON expressions from @{UpdateExpression} from the object itself.
 */
class BsonUpdateVisitor implements UpdateExpressionVisitor<Bson> {
  /**
   * Represent a BSON document for update operations.
   * Intentionally don't use Updates.*() as that will result in issues encoding the value entity,
   * due to codec lookups the MongoDB driver will actually encode the fields of the basic object, not the entity.
   */
  private static class UpdateBson implements Bson {
    private final String operation;
    private final String path;
    private final Entity value;

    UpdateBson(String operation, String path, Entity value) {
      this.operation = operation;
      this.path = path;
      this.value = value;
    }

    @Override
    public <TDocument> BsonDocument toBsonDocument(Class<TDocument> clazz, CodecRegistry codecRegistry) {
      final BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());
      writer.writeStartDocument();
      writer.writeName(operation);
      writer.writeStartDocument();
      CodecProvider.ENTITY_TO_BSON_CONVERTER.writeField(writer, path, value);
      writer.writeEndDocument();
      writer.writeEndDocument();
      return writer.getDocument();
    }
  }

  private static class ClauseVisitor implements UpdateClauseVisitor<Bson> {
    @Override
    public Bson visit(RemoveClause clause) {
      final String path = clause.getPath().getRoot().accept(BsonPathVisitor.INSTANCE_NO_QUOTE, true);
      if (isArrayPath(clause.getPath())) {
        // Mongo has no way of deleting an element from an array in on operation, it leaves a NULL element instead.
        // Use the AggBsonUpdateVisitor instead of the BsonUpdateVisitor to achieve this.
        throw new UnsupportedOperationException("Array element removal is not supported without the aggregation pipeline.");
      }

      return Updates.unset(path);
    }

    @Override
    public Bson visit(SetClause clause) {
      final String path = clause.getPath().getRoot().accept(BsonPathVisitor.INSTANCE_NO_QUOTE, true);

      switch (clause.getValue().getType()) {
        case VALUE:
          return new UpdateBson("$set", path, clause.getValue().getValue());
        case FUNCTION:
          return handleFunction(path, clause.getValue().getFunction());
        default:
          throw new UnsupportedOperationException(String.format("Unsupported SetClause type: %s", clause.getValue().getType().name()));
      }
    }

    private Bson handleFunction(String path, ExpressionFunction function) {
      if (ExpressionFunction.FunctionName.LIST_APPEND == function.getName()) {
        return new UpdateBson("$push", path, function.getArguments().get(1).getValue());
      }

      throw new UnsupportedOperationException(String.format("Unsupported Set function: %s", function.getName()));
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
