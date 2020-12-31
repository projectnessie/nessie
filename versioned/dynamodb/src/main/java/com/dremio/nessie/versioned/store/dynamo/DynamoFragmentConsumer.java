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
import static com.dremio.nessie.versioned.store.dynamo.AttributeValueUtil.deserializeKey;
import static com.dremio.nessie.versioned.store.dynamo.DynamoConstants.ID;
import static com.dremio.nessie.versioned.store.dynamo.DynamoConstants.KEY_LIST;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.dremio.nessie.tiered.builder.FragmentConsumer;
import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.impl.Fragment;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.ValueType;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class DynamoFragmentConsumer extends DynamoConsumer<DynamoFragmentConsumer> implements
    FragmentConsumer<DynamoFragmentConsumer> {

  private Id id;
  private List<Key> keys = new ArrayList<>();

  DynamoFragmentConsumer() {
    super(ValueType.KEY_FRAGMENT);
  }

  @Override
  public DynamoFragmentConsumer id(Id id) {
    addEntitySafe(ID, idBuilder(id));
    return this;
  }

  @Override
  public DynamoFragmentConsumer addKey(Key key) {
    keys.add(key);
    return this;
  }

  @Override
  Map<String, AttributeValue> getEntity() {
    addEntitySafe(
        KEY_LIST,
        AttributeValue.builder().l(keys.stream()
            .map(DynamoConsumer::keyList)
            .collect(Collectors.toList())
        ));

    return buildValuesMap(entity);
  }

  static class Producer implements DynamoProducer<Fragment> {
    @Override
    public Fragment deserialize(Map<String, AttributeValue> entity) {
      Fragment.Builder builder = Fragment.builder();
      builder.id(deserializeId(entity));
      for (AttributeValue key : entity.get(KEY_LIST).l()) {
        builder.addKey(deserializeKey(key));
      }
      return builder.build();
    }
  }
}
