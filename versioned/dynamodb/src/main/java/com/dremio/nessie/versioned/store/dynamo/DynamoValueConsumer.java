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
import com.dremio.nessie.versioned.impl.InternalValue;
import com.dremio.nessie.versioned.store.ValueType;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

class DynamoValueConsumer extends DynamoBytesValueConsumer<DynamoValueConsumer>
    implements ValueConsumer<DynamoValueConsumer> {

  DynamoValueConsumer() {
    super(ValueType.VALUE);
  }

  static class Producer implements DynamoProducer<InternalValue> {
    @Override
    public InternalValue deserialize(Map<String, AttributeValue> entity) {
      return InternalValue.builder()
          .id(deserializeId(entity))
          .value(deserializeBytes(entity.get(VALUE)))
          .build();
    }
  }

}
