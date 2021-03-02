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
package org.projectnessie.versioned.inmem;

import java.security.InvalidParameterException;
import java.util.List;

import org.projectnessie.versioned.impl.condition.ExpressionFunction;
import org.projectnessie.versioned.impl.condition.ExpressionPath;
import org.projectnessie.versioned.impl.condition.Value;
import org.projectnessie.versioned.impl.condition.ValueVisitor;
import org.projectnessie.versioned.store.Entity;

/**
 * This provides a separation of queries on @{ExpressionFunction} from the object itself.
 * This uses the Visitor design pattern to retrieve object attributes.
 */
class InMemValueVisitor implements ValueVisitor<Function> {
  private static class RocksDBExpressionPathVisitor implements ValueVisitor<ExpressionPath> {
    @Override
    public ExpressionPath visit(Entity entity) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ExpressionPath visit(ExpressionFunction value) {
      final ExpressionFunction.FunctionName name = value.getName();
      throw new UnsupportedOperationException(String.format("%s is not a supported top-level RocksDB function.", name));
    }

    @Override
    public ExpressionPath visit(ExpressionPath value) {
      return value;
    }
  }

  private static final RocksDBExpressionPathVisitor EXPRESSION_PATH_VALUE_VISITOR = new RocksDBExpressionPathVisitor();

  @Override
  public Function visit(Entity entity) {
    throw new UnsupportedOperationException(String.format("%s is not supported as a RocksDB Function", entity.toString()));
  }

  @Override
  public Function visit(ExpressionFunction value) {
    final ExpressionFunction.FunctionName name = value.getName();
    final List<Value> arguments = value.getArguments();
    if (arguments.size() != name.getArgCount()) {
      throw new InvalidParameterException(
          String.format("Number of arguments provided [%d] does not match the number expected [%d] for %s.",
              arguments.size(), name.getArgCount(), name));
    }

    switch (name) {
      case EQUALS:
        // Special case SIZE, as the object representation is not contained in one level of ExpressionFunction.
        if (isSize(arguments.get(0))) {
          return ImmutableFunction.builder().operator(Function.Operator.SIZE)
              .path(arguments.get(0).getFunction().getArguments().get(0).accept(EXPRESSION_PATH_VALUE_VISITOR))
              .value(arguments.get(1).getValue()).build();
        }

        return ImmutableFunction.builder().operator(Function.Operator.EQUALS)
            .path(arguments.get(0).accept(EXPRESSION_PATH_VALUE_VISITOR))
            .value(arguments.get(1).getValue()).build();
      default:
        throw new UnsupportedOperationException(String.format("%s is not a supported top-level RocksDB function.", name));
    }
  }

  @Override
  public Function visit(ExpressionPath value) {
    throw new UnsupportedOperationException(String.format("%s is not supported as a RocksDB Function", value.toString()));
  }

  private boolean isSize(Value value) {
    return (value.getType() == Value.Type.FUNCTION) && (((ExpressionFunction)value).getName() == ExpressionFunction.FunctionName.SIZE);
  }
}
