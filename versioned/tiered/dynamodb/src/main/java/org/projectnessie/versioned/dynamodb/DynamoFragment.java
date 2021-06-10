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

import static org.projectnessie.versioned.dynamodb.AttributeValueUtil.attributeValue;
import static org.projectnessie.versioned.dynamodb.AttributeValueUtil.list;

import java.util.Map;
import java.util.stream.Stream;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.WithPayload;
import org.projectnessie.versioned.store.ValueType;
import org.projectnessie.versioned.tiered.Fragment;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

class DynamoFragment extends DynamoBaseValue<Fragment> implements Fragment {

  static final String KEY_LIST = "keys";

  DynamoFragment() {
    super(ValueType.KEY_FRAGMENT);
  }

  @Override
  public Fragment keys(Stream<WithPayload<Key>> keys) {
    return addEntitySafe(KEY_LIST, list(keys.map(AttributeValueUtil::keyElementsWithPayload)));
  }

  @Override
  Map<String, AttributeValue> build() {
    checkPresent(KEY_LIST, "keys");
    return super.build();
  }

  /** Deserialize a DynamoDB entity into the given consumer. */
  static void toConsumer(Map<String, AttributeValue> entity, Fragment consumer) {
    baseToConsumer(entity, consumer)
        .keys(
            attributeValue(entity, KEY_LIST).l().stream()
                .map(AttributeValueUtil::deserializeKeyWithPayload));
  }
}
