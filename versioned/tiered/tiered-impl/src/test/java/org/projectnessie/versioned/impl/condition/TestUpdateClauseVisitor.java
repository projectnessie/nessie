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

import org.immutables.value.Value.Immutable;
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
    testClause(RemoveClause.of(PATH), UpdateCommand.Operator.REMOVE, PATH);
  }

  @Test
  void testSetClauseEquals() {
    final UpdateCommand command = testClause(SetClause.equals(PATH, ENTITY_STR), UpdateCommand.Operator.SET, PATH);
    Assertions.assertEquals(ENTITY_STR, ((UpdateCommand.SetCommand)command).getEntity());
  }

  @Test
  void testSetClauseAppendToList() {
    final UpdateCommand command = testClause(SetClause.appendToList(PATH, ENTITY_STR), UpdateCommand.Operator.SET, PATH);
    Assertions.assertEquals(ENTITY_STR, ((UpdateCommand.SetCommand)command).getEntity());
  }

  UpdateCommand testClause(UpdateClause clause, UpdateCommand.Operator operator, ExpressionPath path) {
    final UpdateCommand command = clause.accept(VISITOR);
    Assertions.assertEquals(operator, command.getOperator());
    Assertions.assertEquals(path, command.getPath());
    return command;
  }

  /**
   * Sample interface or class into which UpdateClauses are converted.
   */
  static interface UpdateCommand {
    /**
     * An enum encapsulating the type of update.
     */
    enum Operator {
      // An operator to remove some part or all of an entity.
      REMOVE,

      // An operator to set some part or all of an entity.
      SET
    }

    Operator getOperator();

    ExpressionPath getPath();

    /**
     * Sample of a specific type of update command into which UpdateClauses are converted.
     */
    @Immutable
    abstract class RemoveCommand implements UpdateCommand {
    }

    /**
     * Sample of a specific type of update command into which UpdateClauses are converted.
     */
    @Immutable
    abstract class SetCommand implements UpdateCommand {

      abstract Entity getEntity();
    }
  }
}
