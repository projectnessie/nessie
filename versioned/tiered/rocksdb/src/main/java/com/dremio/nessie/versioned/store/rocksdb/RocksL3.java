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

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import com.dremio.nessie.tiered.builder.L3;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.KeyDelta;

public class RocksL3 extends RocksBaseValue<L3> implements L3, Evaluator {
  private static final String TREE = "tree";

  private Stream<KeyDelta> keyDelta;

  static RocksL3 EMPTY = new RocksL3(Id.EMPTY, 0L);

  static Id EMPTY_ID = EMPTY.getId();

  public RocksL3() {
    this(EMPTY_ID, 0L);
  }

  RocksL3(Id id, long dt) {
    super(id, dt);
  }

  @Override
  public L3 keyDelta(Stream<KeyDelta> keyDelta) {
    this.keyDelta = keyDelta;
    return this;
  }

  public Stream<KeyDelta> getKeyDelta() {
    return this.keyDelta;
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
      } else if (segment.startsWith(TREE)) {
        // TODO: Does serialization of KeyDelta to Entity needs to be supported?
        // TODO: If not, then this should return false.
        return false;
      } else {
        // Invalid Condition Function.
        return false;
      }
    }
    return result;
  }
}
