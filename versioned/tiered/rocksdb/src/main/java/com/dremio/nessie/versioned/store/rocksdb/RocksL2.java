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

import java.util.stream.Stream;

import com.dremio.nessie.tiered.builder.L2;
import com.dremio.nessie.versioned.impl.condition.ExpressionPath;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.StoreException;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * A RocksDB specific implementation of {@link com.dremio.nessie.tiered.builder.L2} providing
 * SerDe and Condition evaluation.
 */
class RocksL2 extends RocksBaseValue<L2> implements L2 {
  private static final String CHILDREN = "children";

  private Stream<Id> tree; // children

  RocksL2() {
    super();
  }

  @Override
  public L2 children(Stream<Id> ids) {
    this.tree = ids;
    return this;
  }

  @Override
  public boolean evaluateFunction(Function function) {
    final String segment = function.getRootPathAsNameSegment().getName();
    switch (segment) {
      case ID:
        if (!idEvaluates(function)) {
          return false;
        }
        break;
      case CHILDREN:
        if (!evaluateStream(function, tree)) {
          return false;
        }
        break;
      default:
        // Invalid Condition Function.
        return false;
    }
    return true;
  }

  @Override
  byte[] build() {
    checkPresent(tree, CHILDREN);
    return ValueProtos.L2.newBuilder()
      .setBase(buildBase())
      .addAllTree(buildIds(tree))
      .build()
      .toByteArray();
  }

  /**
   * Deserialize a RocksDB value into the given consumer.
   *
   * @param value the protobuf formatted value.
   * @param consumer the consumer to put the value into.
   */
  static void toConsumer(byte[] value, L2 consumer) {
    try {
      final ValueProtos.L2 l2 = ValueProtos.L2.parseFrom(value);
      setBase(consumer, l2.getBase());
      consumer.children(l2.getTreeList().stream().map(Id::of));
    } catch (InvalidProtocolBufferException e) {
      throw new StoreException("Corrupt L2 value encountered when deserializing.", e);
    }
  }
}
