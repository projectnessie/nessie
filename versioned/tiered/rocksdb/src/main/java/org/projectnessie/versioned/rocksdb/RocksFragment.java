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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.store.ConditionFailedException;
import org.projectnessie.versioned.store.Entity;
import org.projectnessie.versioned.store.StoreException;
import org.projectnessie.versioned.tiered.Fragment;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * A RocksDB specific implementation of {@link org.projectnessie.versioned.tiered.Fragment} providing
 * SerDe and Condition evaluation.
 */
class RocksFragment extends RocksBaseValue<Fragment> implements Fragment {
  static final String KEY_LIST = "keys";

  private List<Key> keys;

  RocksFragment() {
    super();
  }

  @Override
  public Fragment keys(Stream<Key> keys) {
    this.keys = keys.collect(Collectors.toList());
    return this;
  }

  @Override
  byte[] build() {
    checkPresent(keys, KEY_LIST);
    return ValueProtos.Fragment.newBuilder()
      .setBase(buildBase())
      .addAllKeys(keys.stream().map(RocksBaseValue::buildKey).collect(Collectors.toList()))
      .build()
      .toByteArray();
  }

  /**
   * Deserialize a RocksDB value into the given consumer.
   *
   * @param value the protobuf formatted value.
   * @param consumer the consumer to put the value into.
   */
  static void toConsumer(byte[] value, Fragment consumer) {
    try {
      final ValueProtos.Fragment fragment = ValueProtos.Fragment.parseFrom(value);
      setBase(consumer, fragment.getBase());
      consumer.keys(fragment.getKeysList().stream().map(RocksBaseValue::createKey));
    } catch (InvalidProtocolBufferException e) {
      throw new StoreException("Corrupt Fragment value encountered when deserializing.", e);
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
          if (!keysAsEntityList(keys).equals(function.getValue())) {
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

  /**
   * Produces an Entity List of Entity Lists for keys.
   * Each key is represented as an entity list of string entities.
   * @param keys stream of keys to convert
   * @return an entity list of keys
   */
  private Entity keysAsEntityList(List<Key> keys) {
    return Entity.ofList(keys.stream().map(k -> Entity.ofList(k.getElements().stream().map(Entity::ofString))));
  }
}
