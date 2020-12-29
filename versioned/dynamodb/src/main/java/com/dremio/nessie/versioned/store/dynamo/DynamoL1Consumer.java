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

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.dremio.nessie.tiered.builder.L1Consumer;
import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.store.Id;

class DynamoL1Consumer implements L1Consumer<DynamoL1Consumer> {
  private final Map<String, AttributeValue> entity = new HashMap<>();

  @Override
  public DynamoL1Consumer commitMetadataId(Id id) {
    return null;
  }

  @Override
  public DynamoL1Consumer addAncestors(List<Id> ids) {
    return null;
  }

  @Override
  public DynamoL1Consumer children(List<Id> ids) {
    return null;
  }

  @Override
  public DynamoL1Consumer id(Id id) {
    return null;
  }

  @Override
  public DynamoL1Consumer addKeyAddition(Key key) {
    return null;
  }

  @Override
  public DynamoL1Consumer addKeyRemoval(Key key) {
    return null;
  }

  @Override
  public DynamoL1Consumer incrementalKeyList(Id checkpointId, int distanceFromCheckpoint) {
    return null;
  }

  @Override
  public DynamoL1Consumer completeKeyList(List<Id> fragmentIds) {
    return null;
  }

  public Map<String, AttributeValue> getEntity() {
    return entity;
  }
}
