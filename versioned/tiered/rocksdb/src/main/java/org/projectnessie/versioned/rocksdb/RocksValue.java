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

import org.projectnessie.versioned.tiered.Value;
import org.projectnessie.versioned.store.Id;
import com.google.protobuf.ByteString;

/**
 * A RocksDB specific implementation of {@link org.projectnessie.versioned.tiered.Value} providing
 * SerDe and Condition evaluation.
 */
class RocksValue extends RocksWrappedValue<Value> implements Value {
  static Value of(Id id, long dt, ByteString value) {
    return new RocksValue().id(id).dt(dt).value(value);
  }

  RocksValue() {
    super();
  }

  @Override
  public boolean evaluateFunction(Function function) {
    final String segment = function.getRootPathAsNameSegment().getName();
    switch (segment) {
      case ID:
        return idEvaluates(function);
      case VALUE:
        return (function.isRootNameSegmentChildlessAndEquals()
            && byteValue.toStringUtf8().equals(function.getValue().getString()));
      default:
        // Invalid Condition Function.
        return false;
    }
  }
}
