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
   * Sample interface or class into which UpdateClauses are converted.
   */
  interface UpdateCommand {
    /**
     * An enum encapsulating.
     */
    enum Operator {
      // An operator to remove some part or all of an entity.
      REMOVE,

      // An operator to set some part or all of an entity.
      SET
    }

    Operator getOperator();

    ExpressionPath getPath();

    class SetCommand implements UpdateCommand {
      private final Entity entity;
      private final ExpressionPath path;

      SetCommand(ExpressionPath path, Entity entity) {
        this.path = path;
        this.entity = entity;
      }

      @Override
      public Operator getOperator() {
        return Operator.SET;
      }

      @Override
      public ExpressionPath getPath() {
        return path;
      }

      Entity getEntity() {
        return entity;
      }
    }

    class RemoveCommand implements UpdateCommand {
      private final ExpressionPath path;

      RemoveCommand(ExpressionPath path) {
        this.path = path;
      }

      @Override
      public Operator getOperator() {
        return Operator.REMOVE;
      }

      @Override
      public ExpressionPath getPath() {
        return path;
      }
    }
  }

  /**
   * A test visitor which builds up an UpdateCommand representation of the passed in UpdateClause.
   */
  static class Visitor implements UpdateClauseVisitor<UpdateCommand> {

    @Override
    public UpdateCommand visit(final RemoveClause clause) {
      return new UpdateCommand.RemoveCommand(clause.getPath());
    }

    @Override
    public UpdateCommand visit(final SetClause clause) {
      switch (clause.getValue().getType()) {
        case VALUE:
          return new UpdateCommand.SetCommand(clause.getPath(), clause.getValue().getValue());
        case FUNCTION:
          return new UpdateCommand.SetCommand(clause.getPath(), handleFunction(clause.getValue().getFunction()));
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
    Assertions.assertEquals(entity, ((UpdateCommand.SetCommand)command).getEntity());
  }
}
