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

import static com.dremio.nessie.versioned.store.dynamo.AttributeValueUtil.deserializeBytes;
import static com.dremio.nessie.versioned.store.dynamo.AttributeValueUtil.deserializeId;
import static com.dremio.nessie.versioned.store.dynamo.DynamoConstants.VALUE;

import java.util.Map;

import com.dremio.nessie.tiered.builder.CommitMetadataConsumer;
import com.dremio.nessie.versioned.impl.InternalCommitMetadata;
import com.dremio.nessie.versioned.store.ValueType;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class DynamoCommitMetadataConsumer extends DynamoBytesValueConsumer<DynamoCommitMetadataConsumer>
    implements CommitMetadataConsumer<DynamoCommitMetadataConsumer> {

  DynamoCommitMetadataConsumer() {
    super(ValueType.COMMIT_METADATA);
  }

  static class Producer implements DynamoProducer<InternalCommitMetadata> {
    @Override
    public InternalCommitMetadata deserialize(Map<String, AttributeValue> entity) {
      return InternalCommitMetadata.builder()
          .id(deserializeId(entity))
          .value(deserializeBytes(entity.get(VALUE)))
          .build();
    }
  }
}
