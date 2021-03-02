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
package org.projectnessie.versioned.inmem;

import org.projectnessie.versioned.store.Id;
import org.projectnessie.versioned.tiered.Value;

import com.google.protobuf.ByteString;

final class ValueObj extends WrappedValueObj<Value> {

  ValueObj(Id id, long dt, ByteString value) {
    super(id, dt, value);
  }

  @Override
  BaseObj<Value> copy() {
    return new ValueObj(getId(), getDt(), getValue());
  }

  static class ValueProducer extends BaseWrappedValueObjProducer<Value> implements Value {

    @Override
    BaseObj<Value> build() {
      return new ValueObj(getId(), getDt(), getValue());
    }
  }
}
