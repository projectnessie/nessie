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
package org.projectnessie.versioned.rocksdb;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.impl.condition.ExpressionPath;
import org.projectnessie.versioned.store.Entity;
import org.projectnessie.versioned.store.Id;
import org.projectnessie.versioned.tiered.BaseValue;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

/**
 * An implementation of @{BaseValue} used for ConditionExpression and UpdateExpression evaluation.
 * @param <C> Specialization of a specific BaseValue interface.
 */
abstract class RocksBaseValue<C extends BaseValue<C>> implements BaseValue<C>, Evaluator {

  static final String ID = "id";

  private Id id;
  private long datetime;

  RocksBaseValue() {
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
    this.datetime = dt;
    return (C) this;
  }

  Id getId() {
    return id;
  }

  /**
   * Converts a Stream of Ids into a List Entity.
   * @param idStream the stream of Id to convert.
   * @return the List Entity.
   */
  Entity toEntity(Stream<Id> idStream) {
    return Entity.ofList(idStream.map(Id::toEntity).collect(Collectors.toList()));
  }

  /**
   * Retrieves an Id at 'position' in a Stream of Ids as an Entity.
   * @param idStream the stream of Id to convert.
   * @param position the element in the Stream to retrieve.
   * @return the List Entity.
   */
  Entity toEntity(Stream<Id> idStream, int position) {
    return idStream.skip(position).findFirst().orElseThrow(NoSuchElementException::new).toEntity();
  }

  /**
   * Evaluates if the stream of Id meets the Condition Function.
   * @param function the function of the ConditionExpression to evaluate.
   * @param stream The Ids to evaluate against the function
   * @return true if the condition is met
   */
  boolean evaluateStream(Function function, Stream<Id> stream) {
    // EQUALS will either compare a specified position or the whole stream as a List.
    if (function.getOperator().equals(Function.Operator.EQUALS)) {
      final ExpressionPath.PathSegment pathSegment = function.getPath().getRoot().getChild().orElse(null);
      if (pathSegment == null) {
        return toEntity(stream).equals(function.getValue());
      } else if (pathSegment.isPosition()) { // compare individual element of list
        final int position = pathSegment.asPosition().getPosition();
        return toEntity(stream, position).equals(function.getValue());
      }
    } else if (function.getOperator().equals(Function.Operator.SIZE)) {
      return (stream.count() == function.getValue().getNumber());
    }

    return false;
  }

  @Override
  public boolean evaluate(List<Function> functions) {
    for (Function function: functions) {
      if (function.getPath().getRoot().isName()) {
        if (!evaluate(function)) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Checks that a Function is met by the implementing class.
   * @param function the condition to check
   * @return true if the condition is met
   */
  public abstract boolean evaluate(Function function);

  /**
   * Evaluates the id against a function and ensures that the name segment is well formed,
   * ie does not contain a child attribute.
   * @param function the function to evaluate against id.
   * @return true if the id meets the function condition
   */
  boolean evaluatesId(Function function) {
    return (function.isRootNameSegmentChildlessAndEquals()
      && getId().toEntity().equals(function.getValue()));
  }

  /**
   * Serialize the value to protobuf format.
   * @return the value serialized as protobuf.
   */
  abstract byte[] build();

  static ValueProtos.Key buildKey(Key key) {
    return ValueProtos.Key.newBuilder().addAllElements(key.getElements()).build();
  }

  static ValueProtos.KeyMutation buildKeyMutation(Key.Mutation m) {
    return ValueProtos.KeyMutation.newBuilder().setType(getType(m)).setKey(buildKey(m.getKey())).build();
  }

  private static ValueProtos.KeyMutation.MutationType getType(Key.Mutation mutation) {
    switch (mutation.getType()) {
      case ADDITION:
        return ValueProtos.KeyMutation.MutationType.ADDITION;
      case REMOVAL:
        return ValueProtos.KeyMutation.MutationType.REMOVAL;
      default:
        throw new IllegalArgumentException("Unknown mutation type " + mutation.getType());
    }
  }

  static List<ByteString> buildIds(Stream<Id> ids) {
    return ids.map(Id::getValue).collect(Collectors.toList());
  }

  /**
   * Build the base value as a protobuf entity.
   * @return the protobuf entity for serialization.
   */
  ValueProtos.BaseValue buildBase() {
    checkPresent(id, ID);
    return ValueProtos.BaseValue.newBuilder()
        .setId(id.getValue())
        .setDatetime(datetime)
        .build();
  }

  /**
   * Consume the base value attributes.
   * @param consumer the base value consumer.
   * @param base the protobuf base value object.
   */
  static <C extends BaseValue<C>> void setBase(BaseValue<C> consumer, ValueProtos.BaseValue base) {
    consumer.id(Id.of(base.getId()));
    consumer.dt(base.getDatetime());
  }

  static Key createKey(ValueProtos.Key key) {
    return Key.of(key.getElementsList().toArray(new String[0]));
  }

  static Key.Mutation createKeyMutation(ValueProtos.KeyMutation k) {
    switch (k.getType()) {
      case ADDITION:
        return createKey(k.getKey()).asAddition();
      case REMOVAL:
        return createKey(k.getKey()).asRemoval();
      default:
        throw new IllegalArgumentException("Unknown mutation type " + k.getType());
    }
  }

  <E> void checkPresent(E element, String name) {
    Preconditions.checkArgument(
        null != element,
        String.format("Method %s of consumer %s has not been called", name, getClass().getSimpleName()));
  }

  <E> void checkNotPresent(E element, String name) {
    Preconditions.checkArgument(
        null == element,
        String.format("Method %s of consumer %s must not be called", name, getClass().getSimpleName()));
  }
}
