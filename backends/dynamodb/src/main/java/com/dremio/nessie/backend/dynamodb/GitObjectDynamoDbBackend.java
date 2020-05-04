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

import com.dremio.nessie.backend.dynamodb.model.GitObject;
import com.dremio.nessie.model.BranchControllerObject;
import com.dremio.nessie.model.ImmutableBranchControllerObject;
import com.dremio.nessie.model.VersionedWrapper;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

public class GitObjectDynamoDbBackend extends AbstractEntityDynamoDbBackend<GitObject,
    BranchControllerObject> {


  public GitObjectDynamoDbBackend(DynamoDbClient client,
                                  DynamoDbEnhancedClient mapper) {
    super(client, mapper, GitObject.class, "NessieGitObjectDatabase");
  }

  @Override
  protected GitObject toDynamoDB(VersionedWrapper<BranchControllerObject> from) {
    return new GitObject(
      from.getObj().getId(),
      from.getObj().getType(),
      //Note: dynamo doesn't accept 0 length byte arrays so we put a dummy here.
      //the git protocol means any 'real' object will have to be greater than 1 bytes so the
      //only time a 0 byte array can happen is when creating an empty branch.
      from.getObj().getData().length == 0 ? new byte[]{-1} : from.getObj().getData(),
      from.getObj().getUpdateTime()
    );
  }

  @Override
  protected VersionedWrapper<BranchControllerObject> fromDynamoDB(GitObject from) {
    return new VersionedWrapper<>(ImmutableBranchControllerObject.builder()
                                                                 .data(
                                                                       from.getData().length == 1
                                                          && from.getData()[0] == ((byte) -1)
                                                            ? new byte[0] : from.getData())
                                                                 .id(from.getUuid())
                                                                 .type(from.getType())
                                                                 .updateTime(
                                                                       from.getUpdateTime())
                                                                 .build());
  }
}
