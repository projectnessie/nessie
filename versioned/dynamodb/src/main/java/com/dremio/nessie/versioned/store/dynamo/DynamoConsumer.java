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

import static com.dremio.nessie.versioned.store.dynamo.AttributeValueUtil.idValue;
import static com.dremio.nessie.versioned.store.dynamo.AttributeValueUtil.idsList;
import static com.dremio.nessie.versioned.store.dynamo.AttributeValueUtil.string;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import com.dremio.nessie.tiered.builder.HasIdConsumer;
import com.dremio.nessie.tiered.builder.Producer;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.Store;
import com.dremio.nessie.versioned.store.ValueType;
import com.google.common.base.Preconditions;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

abstract class DynamoConsumer<C extends HasIdConsumer<C>>
    implements HasIdConsumer<C>, Producer<Map<String, AttributeValue>, C> {

  static final String ID = Store.KEY_NAME;

  static final String KEY_ADDITION = "a";
  static final String KEY_REMOVAL = "d";

  final Map<String, AttributeValue> entity = new HashMap<>();

  DynamoConsumer(ValueType valueType) {
    entity.put(ValueType.SCHEMA_TYPE, string(valueType.getValueName()));
  }

  @SuppressWarnings("unchecked")
  @Override
  public C id(Id id) {
    addEntitySafe(ID, idValue(id));
    return (C) this;
  }

  /**
   * Adds an {@link AttributeValue} that consists of a list of {@link Id}s.
   */
  @SuppressWarnings("unchecked")
  C addIdList(String key, Stream<Id> ids) {
    addEntitySafe(key, idsList(ids));
    return (C) this;
  }

  /**
   * Adds the {@link AttributeValue} for the given key for the final entity.
   */
  @SuppressWarnings("unchecked")
  C addEntitySafe(String key, AttributeValue value) {
    AttributeValue old = entity.put(key, value);
    if (old != null) {
      throw new IllegalStateException("Duplicate '" + key + "' in 'entity' map. Old={" + old + "} current={" + value + "}");
    }
    return (C) this;
  }

  @Override
  public Map<String, AttributeValue> build() {
    checkPresent(ID, "id");

    return entity;
  }

  void checkPresent(String id, String name) {
    Preconditions.checkArgument(
        entity.containsKey(id),
        String.format("Method %s of consumer %s has not been called", name, getClass().getSimpleName()));
  }

  void checkNotPresent(String id, String name) {
    Preconditions.checkArgument(
        !entity.containsKey(id),
        String.format("Method %s of consumer %s must not be called", name, getClass().getSimpleName()));
  }

}
