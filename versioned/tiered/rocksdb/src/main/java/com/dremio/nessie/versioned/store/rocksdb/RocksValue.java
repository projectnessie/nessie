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

import com.dremio.nessie.tiered.builder.Value;
import com.dremio.nessie.versioned.impl.condition.ExpressionPath;
import com.dremio.nessie.versioned.store.Id;
import com.google.protobuf.ByteString;

/**
 * A RocksDB specific implementation of {@link com.dremio.nessie.tiered.builder.Value} providing
 * SerDe and Condition evaluation.
 */
class RocksValue extends RocksWrappedValue<Value> implements Evaluator, Value {
  static Value of(Id id, long dt, ByteString value) {
    return new RocksValue().id(id).dt(dt).value(value);
  }

  RocksValue() {
    super();
  }

  @Override
  public boolean evaluate(Condition condition) {
    for (Function function: condition.getFunctions()) {
      // Retrieve entity at function.path
      if (function.getPath().getRoot().isName()) {
        final ExpressionPath.NameSegment nameSegment = function.getPath().getRoot().asName();
        final String segment = nameSegment.getName();
        switch (segment) {
          case ID:
            if (!idEvaluates(nameSegment, function)) {
              return false;
            }
            break;
          case VALUE:
            if  (!nameSegmentChildlessAndEquals(nameSegment, function)
                || !byteValue.toStringUtf8().equals(function.getValue().getString())) {
              return false;
            }
            break;
          default:
            // Invalid Condition Function.
            return false;
        }
      }
    }
    return true;
  }
}
