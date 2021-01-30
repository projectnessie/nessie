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

import com.dremio.nessie.tiered.builder.CommitMetadata;
import com.dremio.nessie.versioned.store.Id;
import com.google.protobuf.ByteString;

public class RocksCommitMetadata extends RocksBaseValue<CommitMetadata> implements CommitMetadata, Evaluator {
  public static final String ID = "id";
  public static final String VALUE = "value";

  static Id EMPTY_ID = Id.EMPTY;

  static CommitMetadata of(Id id, long dt, ByteString value) {
    return new RocksCommitMetadata(id, dt).value(value);
  }

  private ByteString value; // The value of CommitMetadata serialized as a ByteString

  private RocksCommitMetadata(Id id, long dt) {
    super(id, dt);
  }

  @Override
  public CommitMetadata value(ByteString value) {
    this.value = value;
    return this;
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
      } else if (segment.equals(VALUE)) {
        result &= ((path.size() == 1)
          && (function.getOperator().equals(Function.EQUALS))
          && (value.toStringUtf8().equals(function.getValue().getString())));
      } else {
        // Invalid Condition Function.
        return false;
      }
    }
    return result;
  }
}
