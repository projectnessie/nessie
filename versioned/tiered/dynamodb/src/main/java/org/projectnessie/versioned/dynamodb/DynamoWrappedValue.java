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
package org.projectnessie.versioned.dynamodb;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.projectnessie.versioned.dynamodb.AttributeValueUtil.attributeValue;
import static org.projectnessie.versioned.dynamodb.AttributeValueUtil.bytes;

import java.util.Map;

import org.projectnessie.versioned.store.ValueType;
import org.projectnessie.versioned.tiered.BaseWrappedValue;

import com.google.protobuf.ByteString;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

class DynamoWrappedValue<C extends BaseWrappedValue<C>> extends DynamoBaseValue<C> implements BaseWrappedValue<C> {

  static final String VALUE = "value";

  DynamoWrappedValue(ValueType<C> valueType) {
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
  static <C extends BaseWrappedValue<C>> void produceToConsumer(Map<String, AttributeValue> entity, C consumer) {
    SdkBytes b = checkNotNull(attributeValue(entity, VALUE).b(), "mandatory binary value is null");
    baseToConsumer(entity, consumer)
        .value(ByteString.copyFrom(b.asByteArrayUnsafe()));
  }
}
