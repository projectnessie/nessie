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
import static com.dremio.nessie.versioned.store.dynamo.AttributeValueUtil.deserializeIdList;
import static com.dremio.nessie.versioned.store.dynamo.DynamoConstants.ID;
import static com.dremio.nessie.versioned.store.dynamo.DynamoConstants.TREE;

import java.util.List;
import java.util.Map;

import com.dremio.nessie.tiered.builder.L2Consumer;
import com.dremio.nessie.versioned.impl.L2;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.ValueType;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

class DynamoL2Consumer extends DynamoConsumer<DynamoL2Consumer> implements L2Consumer<DynamoL2Consumer> {

  DynamoL2Consumer() {
    super(ValueType.L2);
  }

  @Override
  public DynamoL2Consumer children(List<Id> ids) {
    addIdList(TREE, ids);
    return this;
  }

  @Override
  public DynamoL2Consumer id(Id id) {
    addEntitySafe(ID, bytes(id.getValue()));
    return this;
  }

  @Override
  Map<String, AttributeValue> getEntity() {
    return buildValuesMap(entity);
  }

  static class Producer implements DynamoProducer<L2> {
    @Override
    public L2 deserialize(Map<String, AttributeValue> entity) {
      L2.Builder builder = L2.builder()
          .id(deserializeId(entity));

      if (entity.containsKey(TREE)) {
        builder = builder.children(deserializeIdList(entity.get(TREE)));
      }

      return builder.build();
    }
  }
}
