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
package com.dremio.nessie.versioned.impl;

import java.util.Map;

import com.dremio.nessie.versioned.impl.DynamoStore.ValueType;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

class SaveOp<V extends HasId> {
  private final ValueType type;
  private final V value;

  public SaveOp(ValueType type, V value) {
    this.type = type;
    this.value = value;
  }

  public ValueType getType() {
    return type;
  }

  public V getValue() {
    return value;
  }

  public Map<String, AttributeValue> toAttributeValues() {
    SimpleSchema<V> schema = type.getSchema();
    return schema.itemToMap(value, true);
  }

}
