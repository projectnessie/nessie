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

package org.projectnessie.versioned.impl.condition;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.projectnessie.versioned.store.Entity;

/**
 * Test basic functionality and show usage of the UpdateClauseVisitor as pertains to UpdateExpression and the UpdateClause
 * hierarchy (ValueOfEntity, ExpressionFunction, ExpressionPath).
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestUpdateClauseVisitor {

  private static final Visitor VISITOR = new Visitor();
  private static final String ID_STR = "id";
  private static final Entity ENTITY_STR = Entity.ofString(ID_STR);
  private static final String PATH_STR = "commits";
  private static final ExpressionPath PATH = ExpressionPath.builder(PATH_STR).build();

  /**
   * A test visitor which builds up an UpdateCommand representation of the passed in UpdateClause.
   */
  static class Visitor implements UpdateClauseVisitor<UpdateCommand> {

    @Override
    public UpdateCommand visit(final AddClause clause) {
      throw new UnsupportedOperationException();
    }

    @Override
    public UpdateCommand visit(final RemoveClause clause) {
      return ImmutableRemoveCommand.builder()
          .operator(UpdateCommand.Operator.REMOVE)
          .path(clause.getPath())
          .build();
    }

    @Override
    public UpdateCommand visit(final SetClause clause) {
      switch (clause.getValue().getType()) {
        case VALUE:
          return ImmutableSetCommand.builder()
              .operator(UpdateCommand.Operator.SET)
              .path(clause.getPath())
              .entity(clause.getValue().getValue())
              .build();
        case FUNCTION:
          return ImmutableSetCommand.builder()
            .operator(UpdateCommand.Operator.SET)
            .path(clause.getPath())
            .entity(handleFunction(clause.getValue().getFunction()))
            .build();
        default:
          throw new UnsupportedOperationException(String.format("Unsupported SetClause type: %s", clause.getValue().getType().name()));
      }
    }

    private Entity handleFunction(ExpressionFunction expressionFunction) {
      if (ExpressionFunction.FunctionName.LIST_APPEND == expressionFunction.getName()) {
        return expressionFunction.getArguments().get(1).getValue();
      }
      throw new UnsupportedOperationException(String.format("Unsupported Set function: %s", expressionFunction.getName()));
    }
  }

  @Test
  void testRemoveClause() {
    final UpdateClause clause = RemoveClause.of(PATH);
    final UpdateCommand command = clause.accept(VISITOR);
    Assertions.assertEquals(UpdateCommand.Operator.REMOVE, command.getOperator());
    Assertions.assertEquals(PATH, command.getPath());
  }

  @Test
  void testSetClauseEquals() {
    testSetClause(SetClause.equals(PATH, ENTITY_STR), PATH, ENTITY_STR);
  }

  @Test
  void testSetClauseAppendToList() {
    testSetClause(SetClause.appendToList(PATH, ENTITY_STR), PATH, ENTITY_STR);
  }

  void testSetClause(UpdateClause clause, ExpressionPath path, Entity entity) {
    final UpdateCommand command = clause.accept(VISITOR);
    Assertions.assertEquals(UpdateCommand.Operator.SET, command.getOperator());
    Assertions.assertEquals(path, command.getPath());
    Assertions.assertEquals(entity, ((SetCommand)command).getEntity());
  }
}
