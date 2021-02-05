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

import com.dremio.nessie.tiered.builder.CommitMetadata;
import com.dremio.nessie.versioned.impl.condition.ExpressionPath;
import com.dremio.nessie.versioned.store.Id;
import com.google.protobuf.ByteString;

/**
 * A RocksDB specific implementation of {@link com.dremio.nessie.tiered.builder.CommitMetadata} providing
 * SerDe and Condition evaluation.
 */
class RocksCommitMetadata extends RocksWrappedValue<CommitMetadata> implements Evaluator, CommitMetadata {
  static CommitMetadata of(Id id, long dt, ByteString value) {
    return new RocksCommitMetadata().id(id).dt(dt).value(value);
  }

  RocksCommitMetadata() {
    super();
  }

  @Override
  public boolean evaluate(Condition condition) {
    for (Function function: condition.getFunctions()) {
      if (function.getPath().getRoot().isName()) {
        ExpressionPath.NameSegment nameSegment = function.getPath().getRoot().asName();
        final String segment = nameSegment.getName();

        switch (segment) {
          case ID:
            // ID is considered a leaf attribute, ie no children. Ensure this is the case
            // in the ExpressionPath.
            if (!idEvaluates(nameSegment, function)) {
              return false;
            }
            break;
          case VALUE:
            // VALUE is considered a leaf attribute, ie no children. Ensure this is the case
            // in the ExpressionPath.
            if (!(nameSegmentChildlessAndEquals(nameSegment, function)
                && byteValue.toStringUtf8().equals(function.getValue().getString()))) {
              return false;
            }
            break;
          default:
            // Invalid Condition Function.
            return false;
        }
      }
    }
    // All functions have passed the test.
    return true;
  }
}
