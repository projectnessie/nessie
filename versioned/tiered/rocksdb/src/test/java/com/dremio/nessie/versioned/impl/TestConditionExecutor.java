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
package com.dremio.nessie.versioned.impl;

import java.util.Random;

import org.junit.jupiter.api.Test;

import com.dremio.nessie.versioned.impl.condition.ExpressionPath;
import com.dremio.nessie.versioned.store.Entity;
import com.dremio.nessie.versioned.store.Store;
import com.dremio.nessie.versioned.store.ValueType;
import com.dremio.nessie.versioned.store.rocksdb.Condition;
import com.dremio.nessie.versioned.store.rocksdb.Function;
import com.dremio.nessie.versioned.store.rocksdb.RocksDBStore;
import com.dremio.nessie.versioned.store.rocksdb.RocksL1;

public class TestConditionExecutor {
  private static final Random RANDOM = new Random(8612341233543L);
  private static final Entity TRUE_ENTITY = Entity.ofBoolean(true);
  private static final Entity FALSE_ENTITY = Entity.ofBoolean(false);

  @Test
  public void executorL1() {
    ConditionExecutor conditionExecutor = new ConditionExecutor();
    final String path = createPath();

    final Condition expectedCondition = new Condition();
    expectedCondition.add(new Function(Function.EQUALS, path, TRUE_ENTITY));
    Store store = new RocksDBStore();

  }

  @Test
  public void rocksL1PutLoad() {
    ConditionExecutor conditionExecutor = new ConditionExecutor();
    final String path = createPath();

    final Condition expectedCondition = new Condition();
    expectedCondition.add(new Function(Function.EQUALS, path, TRUE_ENTITY));
    Store store = new RocksDBStore();
    RocksL1 rocksL1 = new RocksL1();
    store.loadSingle(ValueType.L1, InternalL1.EMPTY.getId(), rocksL1);
  }

  /**
   * Create a path from a . delimited string.
   * @param path the input string where parts of the path are . delimited.
   * @return the associated ExpressionPath.
   */
  private static ExpressionPath ofPath(String path) {
    ExpressionPath.PathSegment.Builder builder = null;
    for (String part : path.split("\\.")) {
      if (builder == null) {
        builder = ExpressionPath.builder(part);
      } else {
        try {
          builder = builder.position(Integer.parseInt(part));
        } catch (NumberFormatException e) {
          builder = builder.name(part);
        }
      }
    }

    return builder.build();
  }

  private static String createPath() {
    return SampleEntities.createString(RANDOM, RANDOM.nextInt(15) + 1);
  }
}
