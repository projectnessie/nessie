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

import com.dremio.nessie.versioned.impl.Persistent;
import com.dremio.nessie.versioned.store.HasId;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.Store;
import com.dremio.nessie.versioned.store.ValueType;
import com.google.common.base.Preconditions;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class DynamoSerDe {

  /**
   * TODO javadoc for checkstyle.
   */
  public static <C extends DynamoConsumer<C>, V> Map<String, AttributeValue> serialize(
      ValueType type,
      V value) {

    @SuppressWarnings("unchecked") Persistent<DynamoConsumer<C>> persistent = (Persistent<DynamoConsumer<C>>) value;

    DynamoConsumer<C> consumer = DynamoConsumer.newConsumer(persistent.type());
    persistent.applyToConsumer(consumer);
    Map<String, AttributeValue> map = consumer.getEntity();

    if (false) {
      AttributeValueUtil.sanityCheckFromSaveOp(type, persistent, map);
    }
    return map;
  }

  /**
   * Deserialize the given {code map} as the given {@link ValueType type}.
   */
  @SuppressWarnings("unchecked")
  public static <V> V deserialize(ValueType valueType, Map<String, AttributeValue> map) {
    Preconditions.checkNotNull(map, "map parameter is null");
    String loadedType = map.get(ValueType.SCHEMA_TYPE).s();
    Id id = DynamoConsumer.deserializeId(map.get(Store.KEY_NAME));
    Preconditions.checkNotNull(loadedType, "Missing type tag for schema for id %s.", id.getHash());
    Preconditions.checkArgument(valueType.getValueName().equals(loadedType),
        "Expected schema for id %s to be of type '%s' but is actually '%s'.", id.getHash(), valueType.getValueName(), loadedType);

    V item = (V) DynamoConsumer.newProducer(valueType)
        .deserialize(map);

    if (false) {
      AttributeValueUtil.sanityCheckToConsumer(map, valueType, (HasId) item);
    }

    return item;
  }
}
