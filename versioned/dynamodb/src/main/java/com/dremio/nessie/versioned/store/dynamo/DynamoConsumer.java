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

import com.dremio.nessie.tiered.builder.HasIdConsumer;
import com.dremio.nessie.versioned.store.HasId;
import com.dremio.nessie.versioned.store.ValueType;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public interface DynamoConsumer<C extends HasIdConsumer<C>> extends HasIdConsumer<C> {
  Map<String, AttributeValue> getEntity();

  /**
   * TODO add some javadoc.
   */
  @SuppressWarnings("unchecked")
  static <C extends DynamoConsumer<C>> DynamoConsumer<C> newConsumer(ValueType type) {
    switch (type) {
      case L1:
        return (DynamoConsumer<C>) new DynamoL1Consumer();
      case L2:
        return (DynamoConsumer<C>) new DynamoL2Consumer();
      case L3:
        return (DynamoConsumer<C>) new DynamoL3Consumer();
      default:
        throw new IllegalArgumentException("No DynamoConsumer implementation for " + type);
    }
  }

  /**
   * TODO add some javadoc.
   */
  @SuppressWarnings("unchecked")
  static <E extends HasId, P extends DynamoProducer<E>> P newProducer(ValueType type) {
    switch (type) {
      case L1:
        return (P) new DynamoL1Consumer.Producer();
      case L2:
        return (P) new DynamoL2Consumer.Producer();
      case L3:
        return (P) new DynamoL3Consumer.Producer();
      default:
        throw new IllegalArgumentException("No DynamoConsumer implementation for " + type);
    }
  }
}
