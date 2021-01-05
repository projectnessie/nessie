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

import static com.dremio.nessie.versioned.store.dynamo.AttributeValueUtil.deserializeId;
import static com.dremio.nessie.versioned.store.dynamo.AttributeValueUtil.deserializeIdStream;

import java.util.Map;
import java.util.stream.Stream;

import com.dremio.nessie.tiered.builder.L2Consumer;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.ValueType;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

class DynamoL2Consumer extends DynamoConsumer<L2Consumer> implements L2Consumer {

  static final String TREE = "tree";

  DynamoL2Consumer() {
    super(ValueType.L2);
  }

  @Override
  public L2Consumer children(Stream<Id> ids) {
    return addIdList(TREE, ids);
  }

  @Override
  public boolean canHandleType(ValueType valueType) {
    return valueType == ValueType.L2;
  }

  @Override
  public Map<String, AttributeValue> build() {
    checkPresent(TREE, "children");

    return super.build();
  }

  /**
   * Deserialize a DynamoDB entity into the given consumer.
   */
  static void produceToConsumer(Map<String, AttributeValue> entity, L2Consumer consumer) {
    consumer.id(deserializeId(entity, ID));

    if (entity.containsKey(TREE)) {
      consumer.children(deserializeIdStream(entity, TREE));
    }
  }
}
