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

package com.dremio.nessie.backend.dynamodb;

import java.util.HashMap;
import java.util.Map;

import com.dremio.nessie.backend.BranchControllerReference;
import com.dremio.nessie.backend.ImmutableBranchControllerReference;
import com.dremio.nessie.backend.VersionedWrapper;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

@Deprecated
public class GitRefDynamoDbBackend extends
                                   AbstractEntityDynamoDbBackend<BranchControllerReference> {


  public GitRefDynamoDbBackend(DynamoDbClient client) {
    super(client, "NessieGitRefDatabase", true);
  }

  @Override
  protected Map<String, AttributeValue> toDynamoDB(
      VersionedWrapper<BranchControllerReference> from) {
    Map<String, AttributeValue> item = new HashMap<>();
    String uuid = from.getObj().getId();
    String refId = String.valueOf(from.getObj().getRefId());
    String updateTime = String.valueOf(from.getObj().getUpdateTime());

    item.put("uuid", AttributeValue.builder().s(uuid).build());
    item.put("ref", AttributeValue.builder().s(refId).build());
    item.put("updateTime", AttributeValue.builder().n(updateTime).build());
    return item;
  }

  @Override
  protected VersionedWrapper<BranchControllerReference> fromDynamoDB(
      Map<String, AttributeValue> from) {
    ImmutableBranchControllerReference.Builder builder = ImmutableBranchControllerReference.builder();
    builder.refId(from.get("ref").s())
           .id(from.get("uuid").s())
           .updateTime(Long.parseLong(from.get("updateTime").n()));
    Long version = Long.parseLong(from.get("version").n());
    return new VersionedWrapper<>(builder.build(), version);
  }

}
