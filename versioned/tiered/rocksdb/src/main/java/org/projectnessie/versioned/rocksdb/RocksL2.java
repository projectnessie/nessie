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

import org.projectnessie.versioned.store.Id;
import org.projectnessie.versioned.store.StoreException;
import org.projectnessie.versioned.tiered.L2;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * A RocksDB specific implementation of {@link org.projectnessie.versioned.tiered.L2} providing
 * SerDe and Condition evaluation.
 */
class RocksL2 extends RocksBaseValue<L2> implements L2 {
  private static final String CHILDREN = "children";

  private List<Id> tree; // children

  RocksL2() {
    super();
  }

  @Override
  public L2 children(Stream<Id> ids) {
    this.tree = ids.collect(Collectors.toList());
    return this;
  }

  @Override
  public boolean evaluate(Function function) {
    final String segment = function.getRootPathAsNameSegment().getName();
    switch (segment) {
      case ID:
        return evaluatesId(function);
      case CHILDREN:
        return evaluate(function, tree);
      default:
        // Invalid Condition Function.
        return false;
    }
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
