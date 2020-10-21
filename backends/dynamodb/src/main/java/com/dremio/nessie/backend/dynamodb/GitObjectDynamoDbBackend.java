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


import com.dremio.nessie.backend.BranchControllerObject;
import com.dremio.nessie.backend.ImmutableBranchControllerObject;
import com.dremio.nessie.backend.VersionedWrapper;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

@Deprecated
public class GitObjectDynamoDbBackend extends
                                      AbstractEntityDynamoDbBackend<BranchControllerObject> {


  public GitObjectDynamoDbBackend(DynamoDbClient client) {
    super(client, "NessieGitObjectDatabase", false);
  }

  @Override
  protected Map<String, AttributeValue> toDynamoDB(VersionedWrapper<BranchControllerObject> from) {
    Map<String, AttributeValue> item = new HashMap<>();
    String uuid = from.getObj().getId();
    String type = String.valueOf(from.getObj().getType());
    //Note: dynamo doesn't accept 0 length byte arrays so we put a dummy here.
    //the git protocol means any 'real' object will have to be greater than 1 bytes so the
    //only time a 0 byte array can happen is when creating an empty branch.
    byte[] bytes = from.getObj().getData();
    SdkBytes data = SdkBytes.fromByteArray(bytes.length == 0 ? new byte[]{-1} : bytes);
    String updateTime = String.valueOf(from.getObj().getUpdateTime());

    item.put("uuid", AttributeValue.builder().s(uuid).build());
    item.put("type", AttributeValue.builder().n(type).build());
    item.put("data", AttributeValue.builder().b(data).build());
    item.put("updateTime", AttributeValue.builder().n(updateTime).build());
    return item;
  }

  @Override
  protected VersionedWrapper<BranchControllerObject> fromDynamoDB(
      Map<String, AttributeValue> from) {
    ImmutableBranchControllerObject.Builder builder = ImmutableBranchControllerObject.builder();
    byte[] data = from.get("data").b().asByteArray();
    builder.data(data.length == 1 && data[0] == ((byte) -1) ? new byte[0] : data)
           .id(from.get("uuid").s())
           .type(Integer.parseInt(from.get("type").n()))
           .updateTime(Long.parseLong(from.get("updateTime").n()));
    return new VersionedWrapper<>(builder.build());
  }
}
