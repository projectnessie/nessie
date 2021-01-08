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

import static com.dremio.nessie.versioned.store.dynamo.AttributeValueUtil.bytes;
import static com.dremio.nessie.versioned.store.dynamo.AttributeValueUtil.deserializeBytes;
import static com.dremio.nessie.versioned.store.dynamo.AttributeValueUtil.deserializeId;

import java.util.Map;

import com.dremio.nessie.tiered.builder.WrappedValueConsumer;
import com.dremio.nessie.versioned.store.ValueType;
import com.google.protobuf.ByteString;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

abstract class DynamoWrappedValueConsumer<C extends WrappedValueConsumer<C>> extends DynamoConsumer<C> implements WrappedValueConsumer<C> {

  static final String VALUE = "value";

  DynamoWrappedValueConsumer(ValueType valueType) {
    super(valueType);
  }

  @Override
  public C value(ByteString value) {
    return addEntitySafe(VALUE, bytes(value));
  }

  @Override
  Map<String, AttributeValue> build() {
    checkPresent(VALUE, "value");

    return super.build();
  }

  /**
   * Deserialize a DynamoDB entity into the given consumer.
   */
  static <C extends WrappedValueConsumer<C>> void produceToConsumer(Map<String, AttributeValue> entity, C consumer) {
    consumer.id(deserializeId(entity, ID))
        .value(deserializeBytes(entity, VALUE));
  }

}
