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

package com.dremio.nessie.versioned.store.rocksdb;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.dremio.nessie.tiered.builder.L3;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.KeyDelta;
import com.dremio.nessie.versioned.store.StoreException;
import com.google.protobuf.InvalidProtocolBufferException;

class RocksL3 extends RocksBaseValue<L3> implements L3, Evaluator {
  private static final String TREE = "tree";

  private Stream<KeyDelta> keyDelta;

  RocksL3() {
    super();
  }

  @Override
  public L3 keyDelta(Stream<KeyDelta> keyDelta) {
    this.keyDelta = keyDelta;
    return this;
  }

  @Override
  public boolean evaluate(Condition condition) {
    boolean result = true;
    for (Function function: condition.functionList) {
      // Retrieve entity at function.path
      final List<String> path = Evaluator.splitPath(function.getPath());
      final String segment = path.get(0);
      if (segment.equals(ID)) {
        result &= ((path.size() == 1)
          && (function.getOperator().equals(Function.EQUALS))
          && (getId().toEntity().equals(function.getValue())));
      } else {
        // TODO: ConditionExpression is currently not supported for TREE.
        // TODO: Implement a case for TREE if serialization of a Stream of KeyDelta to Entity is supported.
        // Invalid Condition Function.
        return false;
      }
    }
    return result;
  }

  @Override
  byte[] build() {
    checkPresent(keyDelta, TREE);
    return ValueProtos.L3.newBuilder()
      .setBase(buildBase())
      .addAllKeyDelta(keyDelta.map(d ->
        ValueProtos.KeyDelta.newBuilder().setId(d.getId().getValue()).setKey(buildKey(d.getKey())).build()).collect(Collectors.toList()))
      .build()
      .toByteArray();
  }

  /**
   * Deserialize a RocksDB value into the given consumer.
   *
   * @param value the protobuf formatted value.
   * @param consumer the consumer to put the value into.
   */
  static void toConsumer(byte[] value, L3 consumer) {
    try {
      final ValueProtos.L3 l3 = ValueProtos.L3.parseFrom(value);
      setBase(consumer, l3.getBase());
      consumer.keyDelta(l3.getKeyDeltaList()
          .stream()
          .map(d -> KeyDelta.of(createKey(d.getKey()), Id.of(d.getId()))));
    } catch (InvalidProtocolBufferException e) {
      throw new StoreException("Corrupt L3 value encountered when deserializing.", e);
    }
  }
}
