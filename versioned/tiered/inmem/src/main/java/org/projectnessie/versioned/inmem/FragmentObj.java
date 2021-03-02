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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.WithPayload;
import org.projectnessie.versioned.impl.condition.ExpressionFunction;
import org.projectnessie.versioned.impl.condition.ExpressionPath.NameSegment;
import org.projectnessie.versioned.impl.condition.RemoveClause;
import org.projectnessie.versioned.impl.condition.SetClause;
import org.projectnessie.versioned.store.ConditionFailedException;
import org.projectnessie.versioned.store.Entity;
import org.projectnessie.versioned.store.Id;
import org.projectnessie.versioned.tiered.Fragment;

final class FragmentObj extends BaseObj<Fragment> {
  static final String KEY_LIST = "keys";

  private final List<WithPayload<Key>> keys;

  FragmentObj(Id id, long dt, List<WithPayload<Key>> keys) {
    super(id, dt);
    this.keys = keys;
  }

  @Override
  Fragment consume(Fragment consumer) {
    return super.consume(consumer).keys(keys.stream());
  }

  static class FragmentProducer extends BaseObjProducer<Fragment> implements Fragment {
    private List<WithPayload<Key>> keys;

    @Override
    public Fragment keys(Stream<WithPayload<Key>> keys) {
      this.keys = keys.collect(Collectors.toList());
      return this;
    }

    @Override
    BaseObj<Fragment> build() {
      return new FragmentObj(getId(), getDt(), keys);
    }
  }

  @Override
  BaseObj<Fragment> copy() {
    return new FragmentObj(getId(), getDt(),
        keys != null ? new ArrayList<>(keys) : null);
  }

  @Override
  void applyRemove(RemoveClause removeClause, Consumer<DeferredRemove> deferredRemoveConsumer) {
    NameSegment root = removeClause.getPath().getRoot();
    switch (root.getName()) {
      case KEY_LIST:
        int pos = root.getChild().orElseThrow(() -> new IllegalStateException("Position required")).asPosition().getPosition();
        deferredRemoveConsumer.accept(new DeferredRemove(pos, keys::remove));
        break;
      default:
        throw new UnsupportedOperationException();
    }
  }

  @Override
  void applySet(SetClause setClause) {
    NameSegment root = setClause.getPath().getRoot();
    switch (root.getName()) {
      case KEY_LIST:
        switch (setClause.getValue().getType()) {
          case FUNCTION:
            ExpressionFunction expressionFunction = (ExpressionFunction) setClause.getValue();
            switch (expressionFunction.getName()) {
              case LIST_APPEND:
                List<Entity> valueList = expressionFunction.getArguments().get(1).getValue().getList();
                valueList.forEach(value -> {
                  List<Entity> list = value.getList();
                  keys.add(
                      WithPayload.of(Byte.parseByte(list.get(0).getString()),
                      Key.of(list.stream().skip(1).map(Entity::getString).toArray(String[]::new))
                  ));
                });
                break;
              case EQUALS:
              case SIZE:
              case ATTRIBUTE_NOT_EXISTS:
              default:
                throw new UnsupportedOperationException();
            }
            break;
          case VALUE:
          case PATH:
          default:
            throw new UnsupportedOperationException();
        }
        break;
      default:
        throw new UnsupportedOperationException();
    }
  }

  @Override
  public void evaluate(Function function) throws ConditionFailedException {
    final String segment = function.getRootPathAsNameSegment().getName();
    switch (segment) {
      case ID:
        evaluatesId(function);
        break;
      case KEY_LIST:
        if (function.getRootPathAsNameSegment().getChild().isPresent()) {
          throw new ConditionFailedException(invalidOperatorSegmentMessage(function));
        } else if (function.getOperator().equals(Function.Operator.EQUALS)) {
          if (!keysAsEntityList().equals(function.getValue())) {
            throw new ConditionFailedException(conditionNotMatchedMessage(function));
          }
        } else if (function.getOperator().equals(Function.Operator.SIZE)) {
          if (keys.size() != function.getValue().getNumber()) {
            throw new ConditionFailedException(conditionNotMatchedMessage(function));
          }
        } else {
          throw new ConditionFailedException(invalidOperatorSegmentMessage(function));
        }
        break;
      default:
        // NameSegment could not be applied to FunctionExpression.
        throw new ConditionFailedException(invalidOperatorSegmentMessage(function));
    }
  }

  private Entity keysAsEntityList() {
    return Entity.ofList(keys.stream().map(k -> Entity.ofList(
        Stream.concat(Stream.of(k.getPayload().toString()),
            k.getValue().getElements().stream()).map(Entity::ofString)
    )));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    FragmentObj that = (FragmentObj) o;

    return keys.equals(that.keys);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + keys.hashCode();
    return result;
  }
}
