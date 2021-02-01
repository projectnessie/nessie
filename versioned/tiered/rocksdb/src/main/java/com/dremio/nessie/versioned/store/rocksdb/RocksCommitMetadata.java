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

import com.dremio.nessie.tiered.builder.CommitMetadata;
import com.dremio.nessie.versioned.store.Id;
import com.google.protobuf.ByteString;

class RocksCommitMetadata extends RocksWrappedValue<CommitMetadata> implements Evaluator, CommitMetadata {
  static CommitMetadata of(Id id, long dt, ByteString value) {
    return new RocksCommitMetadata().id(id).dt(dt).value(value);
  }

  RocksCommitMetadata() {
    super();
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
      } else if (segment.equals(VALUE)) {
        result &= ((path.size() == 1)
          && (function.getOperator().equals(Function.EQUALS))
          && (byteValue.toStringUtf8().equals(function.getValue().getString())));
      } else {
        // Invalid Condition Function.
        return false;
      }
    }
    return result;
  }
}
