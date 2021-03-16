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
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.stream.Collectors;

import org.projectnessie.versioned.impl.condition.ExpressionPath;
import org.projectnessie.versioned.impl.condition.RemoveClause;
import org.projectnessie.versioned.impl.condition.SetClause;
import org.projectnessie.versioned.impl.condition.UpdateClause;
import org.projectnessie.versioned.impl.condition.UpdateExpression;
import org.projectnessie.versioned.store.ConditionFailedException;
import org.projectnessie.versioned.store.Entity;
import org.projectnessie.versioned.store.Id;
import org.projectnessie.versioned.store.ValueType;
import org.projectnessie.versioned.tiered.BaseValue;

/**
 * Base class of all value-types in of the in-memory {@link org.projectnessie.versioned.store.Store}
 * implementation.
 * <p>All subclasses <em>must</em> implement {@link Object#hashCode()} and
 * {@link Object#equals(Object)}, because the some functionality in the
 * {@link org.projectnessie.versioned.store.Store} implementation require this, for example
 * {@link InMemStore#delete(ValueType, Id, Optional)}.</p>
 * @param <C> value-type
 */
abstract class BaseObj<C extends BaseValue<C>> {
  static final String ID = "id";

  private final Id id;
  private final long dt;

  BaseObj(Id id, long dt) {
    this.id = id;
    this.dt = dt;
  }

  Id getId() {
    return id;
  }

  long getDt() {
    return dt;
  }

  C consume(C consumer) {
    return consumer.id(id).dt(dt);
  }

  void evaluate(List<Function> functions) throws ConditionFailedException {
    for (Function function: functions) {
      if (function.getPath().getRoot().isName()) {
        try {
          evaluate(function);
        } catch (IllegalStateException e) {
          // Catch exceptions raise due to incorrect Entity type in FunctionExpression being compared to the
          // target attribute.
          throw new ConditionFailedException(invalidValueMessage(function));
        }
      }
    }
  }

  void evaluate(Function function) throws ConditionFailedException {
    throw new UnsupportedOperationException("Conditions not supported for " + getClass().getSimpleName());
  }

  boolean evaluate(Function function, List<Id> idList) {
    // EQUALS will either compare a specified position or the whole idList as a List.
    if (function.getOperator().equals(Function.Operator.EQUALS)) {
      final ExpressionPath.PathSegment pathSegment = function.getPath().getRoot().getChild().orElse(null);
      if (pathSegment == null) {
        return toEntity(idList).equals(function.getValue());
      } else if (pathSegment.isPosition()) { // compare individual element of list
        final int position = pathSegment.asPosition().getPosition();
        return toEntity(idList, position).equals(function.getValue());
      }
    } else if (function.getOperator().equals(Function.Operator.SIZE)) {
      return (idList.size() == function.getValue().getNumber());
    }

    return false;
  }

  static Entity toEntity(List<Id> idList) {
    return Entity.ofList(idList.stream().map(Id::toEntity).collect(Collectors.toList()));
  }

  static Entity toEntity(List<Id> idList, int position) {
    return idList.stream().skip(position).findFirst().orElseThrow(NoSuchElementException::new).toEntity();
  }

  void evaluatesId(Function function) throws ConditionFailedException {
    if (!function.isRootNameSegmentChildlessAndEquals()
        || !getId().toEntity().equals(function.getValue())) {
      throw new ConditionFailedException(conditionNotMatchedMessage(function));
    }
  }

  protected static String invalidOperatorSegmentMessage(Function function) {
    return String.format("Operator: %s is not applicable to segment %s",
        function.getOperator(), function.getRootPathAsNameSegment().getName());
  }

  protected static String invalidValueMessage(Function function) {
    return String.format("Not able to apply type: %s to segment: %s", function.getValue().getType(),
        function.getRootPathAsNameSegment().getName());
  }

  protected static String conditionNotMatchedMessage(Function function) {
    return String.format("Condition %s did not match the actual value for %s", function.getValue().getType(),
        function.getRootPathAsNameSegment().getName());
  }

  abstract BaseObj<C> copy();

  static final class DeferredRemove implements Comparable<DeferredRemove> {

    private final int position;
    private final IntConsumer removeOperation;

    protected DeferredRemove(int position, IntConsumer removeOperation) {
      this.position = position;
      this.removeOperation = removeOperation;
    }

    @Override
    public int compareTo(DeferredRemove o) {
      return Integer.compare(o.position, position);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      DeferredRemove that = (DeferredRemove) o;

      if (position != that.position) {
        return false;
      }
      return removeOperation.equals(that.removeOperation);
    }

    @Override
    public int hashCode() {
      int result = position;
      result = 31 * result + removeOperation.hashCode();
      return result;
    }

    void apply() {
      removeOperation.accept(position);
    }
  }

  BaseObj<C> update(UpdateExpression update) {
    BaseObj<C> copy = copy();
    List<DeferredRemove> deferred = new ArrayList<>();
    update.getClauses().forEach(clause -> copy.apply(clause, deferred::add));
    deferred.stream().sorted().forEach(DeferredRemove::apply);
    return copy;
  }

  private void apply(UpdateClause updateClause, Consumer<DeferredRemove> deferredRemoveConsumer) {
    switch (updateClause.getType()) {
      case DELETE:
        applyDelete(updateClause);
        break;
      case REMOVE:
        applyRemove((RemoveClause) updateClause, deferredRemoveConsumer);
        break;
      case SET:
        applySet((SetClause) updateClause);
        break;
      default:
        throw new UnsupportedOperationException(updateClause.toString());
    }
  }

  void applyDelete(UpdateClause updateClause) {
    throw new UnsupportedOperationException(String.format("%s does not support UpdateClause/DELETE", getClass().getSimpleName()));
  }

  void applyRemove(RemoveClause removeClause,
      Consumer<DeferredRemove> deferredRemoveConsumer) {
    throw new UnsupportedOperationException(String.format("%s does not support RemoveClause", getClass().getSimpleName()));
  }

  void applySet(SetClause setClause) {
    throw new UnsupportedOperationException(String.format("%s does not support SetClause", getClass().getSimpleName()));
  }

  abstract static class BaseObjProducer<C extends BaseValue<C>> implements BaseValue<C> {
    private Id id;
    private long dt;

    Id getId() {
      return id;
    }

    long getDt() {
      return dt;
    }

    @SuppressWarnings("unchecked")
    @Override
    public C id(Id id) {
      this.id = id;
      return (C) this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public C dt(long dt) {
      this.dt = dt;
      return (C) this;
    }

    abstract BaseObj<C> build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    BaseObj<?> baseObj = (BaseObj<?>) o;

    if (dt != baseObj.dt) {
      return false;
    }
    return id.equals(baseObj.id);
  }

  @Override
  public int hashCode() {
    int result = id.hashCode();
    result = 31 * result + (int) (dt ^ (dt >>> 32));
    return result;
  }
}
