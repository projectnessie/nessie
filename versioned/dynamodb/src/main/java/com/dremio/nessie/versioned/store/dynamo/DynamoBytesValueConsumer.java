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

package com.dremio.nessie.versioned.store.dynamo;

import com.dremio.nessie.tiered.builder.ValueConsumer;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.ValueType;
import com.google.protobuf.ByteString;

class DynamoBytesValueConsumer<T extends DynamoBytesValueConsumer<T>> extends DynamoConsumer<T> implements
    ValueConsumer<T> {

  DynamoBytesValueConsumer(ValueType valueType) {
    super(valueType);
  }

  @Override
  public T id(Id id) {
    addEntitySafe(ID, idBuilder(id));
    return (T) this;
  }

  @Override
  public T value(ByteString value) {
    addEntitySafe(VALUE, bytes(value));
    return (T) this;
  }
}
