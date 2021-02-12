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

import static org.projectnessie.versioned.dynamodb.AttributeValueUtil.deserializeIdStream;

import java.util.Map;
import java.util.stream.Stream;

import org.projectnessie.versioned.store.Id;
import org.projectnessie.versioned.store.ValueType;
import org.projectnessie.versioned.tiered.L2;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

class DynamoL2 extends DynamoBaseValue<L2> implements L2 {

  static final String TREE = "tree";

  DynamoL2() {
    super(ValueType.L2);
  }

  @Override
  public L2 children(Stream<Id> ids) {
    return addIdList(TREE, ids);
  }

  @Override
  Map<String, AttributeValue> build() {
    checkPresent(TREE, "children");

    return super.build();
  }

  /**
   * Deserialize a DynamoDB entity into the given consumer.
   */
  static void toConsumer(Map<String, AttributeValue> entity, L2 consumer) {
    baseToConsumer(entity, consumer);

    if (entity.containsKey(TREE)) {
      consumer.children(deserializeIdStream(entity, TREE));
    }
  }
}
