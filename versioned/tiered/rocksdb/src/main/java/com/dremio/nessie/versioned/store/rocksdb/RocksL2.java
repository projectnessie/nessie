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

import static com.dremio.nessie.versioned.store.rocksdb.RocksL1.CHILDREN;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import com.dremio.nessie.tiered.builder.L2;
import com.dremio.nessie.versioned.store.Id;

public class RocksL2 extends RocksBaseValue<L2> implements L2, Evaluator {
  static RocksL2 EMPTY = new RocksL2(Id.EMPTY, 0L);

  static Id EMPTY_ID = EMPTY.getId();

  private Stream<Id> tree; // children

  public RocksL2() {
    this(EMPTY_ID, 0L);
  }

  private RocksL2(Id id, long dt) {
    super(id, dt);
  }

  @Override
  public L2 children(Stream<Id> ids) {
    this.tree = ids;
    return this;
  }

  public Stream<Id> getChildren() {
    return tree;
  }

  @Override
  public boolean evaluate(Condition condition) {
    boolean result = true;
    for (Function function: condition.functionList) {
      // Retrieve entity at function.path
      List<String> path = Arrays.asList(function.getPath().split(Pattern.quote(".")));
      String segment = path.get(0);
      if (segment.equals(ID)) {
        result &= ((path.size() == 1)
          && (function.getOperator().equals(Function.EQUALS))
          && (getId().toEntity().equals(function.getValue())));
      } else if (segment.startsWith(CHILDREN)) {
        result &= evaluateStream(function, tree);
      } else {
        // Invalid Condition Function.
        return false;
      }
    }
    return result;
  }
}
