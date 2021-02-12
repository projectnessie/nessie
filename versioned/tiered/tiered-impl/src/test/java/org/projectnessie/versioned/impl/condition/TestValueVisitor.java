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

import java.util.stream.Collectors;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.projectnessie.versioned.store.Entity;

/**
 * Test basic functionality and show usage of the ValueVisitor as pertains to ConditionExpression and the Value
 * hierarchy (ValueOfEntity, ExpressionFunction, ExpressionPath).
 */
class TestValueVisitor {
  /**
   * A test visitor which builds up a string representation of the passed in Values.
   */
  static class Visitor implements ValueVisitor<String> {
    @Override
    public String visit(Entity entity) {
      return entity.toString();
    }

    @Override
    public String visit(ExpressionFunction value) {
      return value.getName()
        + "[" + value.getArguments().stream().map(a -> a.accept(this)).collect(Collectors.joining(", ")) + "]";
    }

    @Override
    public String visit(ExpressionPath value) {
      return value.asString();
    }
  }

  private static final Visitor VISITOR = new Visitor();

  @Test
  void testEntityValue() {
    final Value numValue = Value.of(Entity.ofNumber(55));
    Assertions.assertEquals("n55", numValue.accept(VISITOR));

    final Value strValue = Value.of(Entity.ofString("myString"));
    Assertions.assertEquals("\"myString\"", strValue.accept(VISITOR));

    final Value boolValue = Value.of(Entity.ofBoolean(true));
    Assertions.assertEquals("true", boolValue.accept(VISITOR));
  }

  @Test
  void testExpressionPathValue() {
    ExpressionPath.PathSegment.Builder builder = ExpressionPath.builder("root");
    Assertions.assertEquals("root", builder.build().accept(VISITOR));

    builder = builder.position(2);
    Assertions.assertEquals("root[2]", builder.build().accept(VISITOR));

    builder = builder.name("notRoot");
    Assertions.assertEquals("root[2].notRoot", builder.build().accept(VISITOR));
  }

  @Test
  void testExpressionFunctionValue() {
    final ExpressionPath.PathSegment.Builder pathBuilder = ExpressionPath.builder("arrayPath");
    final Value value = ExpressionFunction.equals(ExpressionFunction.size(pathBuilder.build()), Entity.ofNumber(10));
    Assertions.assertEquals("EQUALS[SIZE[arrayPath], n10]", value.accept(VISITOR));
  }
}
