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

import java.util.Map;

import com.dremio.nessie.tiered.builder.ValueConsumer;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.ValueType;
import com.google.protobuf.ByteString;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

class DynamoValueConsumer extends DynamoConsumer<ValueConsumer> implements ValueConsumer {

  static final String VALUE = "value";

  DynamoValueConsumer() {
    super(ValueType.VALUE);
  }

  @Override
  public DynamoValueConsumer id(Id id) {
    addEntitySafe(ID, idBuilder(id));
    return this;
  }

  @Override
  public boolean canHandleType(ValueType valueType) {
    return valueType == ValueType.VALUE;
  }

  @Override
  public DynamoValueConsumer value(ByteString value) {
    addEntitySafe(VALUE, bytes(value));
    return this;
  }

  static class Producer extends DynamoProducer<ValueConsumer> {
    public Producer(Map<String, AttributeValue> entity) {
      super(entity);
    }

    @Override
    public void applyToConsumer(ValueConsumer consumer) {
      consumer.id(deserializeId(entity))
          .value(deserializeBytes(entity.get(VALUE)));
    }
  }

}
