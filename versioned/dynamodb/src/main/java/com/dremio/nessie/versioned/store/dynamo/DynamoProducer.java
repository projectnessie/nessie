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

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * A producer implements the deserialization part and uses the consumers to build the
 * deserialized objects.
 */
public abstract class DynamoProducer<C extends HasIdConsumer<C>> {
  protected final Map<String, AttributeValue> entity;

  public DynamoProducer(Map<String, AttributeValue> entity) {
    this.entity = entity;
  }

  public abstract void applyToConsumer(C consumer);
}
