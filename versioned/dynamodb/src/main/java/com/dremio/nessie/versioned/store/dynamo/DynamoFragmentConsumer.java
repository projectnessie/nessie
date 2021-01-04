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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.dremio.nessie.tiered.builder.FragmentConsumer;
import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.impl.Fragment;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.ValueType;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

class DynamoFragmentConsumer extends DynamoConsumer<DynamoFragmentConsumer> implements
    FragmentConsumer<DynamoFragmentConsumer> {

  DynamoFragmentConsumer() {
    super(ValueType.KEY_FRAGMENT);
  }

  @Override
  public DynamoFragmentConsumer id(Id id) {
    addEntitySafe(ID, idBuilder(id));
    return this;
  }

  @Override
  public DynamoFragmentConsumer keys(Stream<Key> keys) {
    addEntitySafe(
        KEY_LIST,
        AttributeValue.builder().l(keys
            .map(DynamoConsumer::keyList)
            .collect(Collectors.toList())
        ));
    return this;
  }

  static class Producer implements DynamoProducer<Fragment> {
    @Override
    public Fragment deserialize(Map<String, AttributeValue> entity) {
      Fragment.Builder builder = Fragment.builder();
      builder.id(deserializeId(entity));
      builder.keys(entity.get(KEY_LIST).l().stream()
          .map(DynamoConsumer::deserializeKey)
      );
      return builder.build();
    }
  }
}
