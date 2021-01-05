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

import static com.dremio.nessie.versioned.store.dynamo.AttributeValueUtil.attributeValue;
import static com.dremio.nessie.versioned.store.dynamo.AttributeValueUtil.deserializeId;
import static com.dremio.nessie.versioned.store.dynamo.AttributeValueUtil.list;
import static com.dremio.nessie.versioned.store.dynamo.AttributeValueUtil.mandatoryList;

import java.util.Map;
import java.util.stream.Stream;

import com.dremio.nessie.tiered.builder.FragmentConsumer;
import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.store.ValueType;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

class DynamoFragmentConsumer extends DynamoConsumer<FragmentConsumer> implements FragmentConsumer {

  static final String KEY_LIST = "keys";

  DynamoFragmentConsumer() {
    super(ValueType.KEY_FRAGMENT);
  }

  @Override
  public boolean canHandleType(ValueType valueType) {
    return valueType == ValueType.KEY_FRAGMENT;
  }

  @Override
  public FragmentConsumer keys(Stream<Key> keys) {
    return addEntitySafe(
        KEY_LIST,
        list(keys.map(AttributeValueUtil::keyElements)));
  }

  @Override
  public Map<String, AttributeValue> build() {
    checkPresent(KEY_LIST, "keys");

    return super.build();
  }

  /**
   * Deserialize a DynamoDB entity into the given consumer.
   */
  static void produceToConsumer(Map<String, AttributeValue> entity, FragmentConsumer consumer) {
    consumer.id(deserializeId(entity, ID))
        .keys(mandatoryList(attributeValue(entity, KEY_LIST)).stream()
            .map(AttributeValueUtil::deserializeKey)
        );
  }
}
