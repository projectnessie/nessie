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
import com.dremio.nessie.model.ImmutableGitObject;
import com.dremio.nessie.model.VersionedWrapper;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

public class GitObjectDynamoDbBackend extends AbstractEntityDynamoDbBackend<GitObject,
    com.dremio.nessie.model.GitObject> {


  public GitObjectDynamoDbBackend(DynamoDbClient client,
                                  DynamoDbEnhancedClient mapper) {
    super(client, mapper, GitObject.class, "NessieGitObjectDatabase", null);
  }

  @Override
  protected GitObject toDynamoDB(VersionedWrapper<com.dremio.nessie.model.GitObject> from) {
    return new GitObject(
      from.getObj().getId(),
      from.getObj().getType(),
      from.getObj().getData().length == 0 ? new byte[]{-1} : from.getObj().getData(),
      from.getObj().isDeleted(),
      from.getObj().getUpdateTime(),
      from.getVersion().isPresent() ? from.getVersion().getAsLong() : null
    );
  }

  @Override
  protected VersionedWrapper<com.dremio.nessie.model.GitObject> fromDynamoDB(GitObject from) {
    return new VersionedWrapper<>(ImmutableGitObject.builder()
                                                    .data(from.getData().length == 1
                                                          && from.getData()[0] == ((byte) -1)
                                                            ? new byte[0] : from.getData())
                                                    .isDeleted(from.isDeleted())
                                                    .id(from.getUuid())
                                                    .type(from.getType())
                                                    .updateTime(from.getUpdateTime())
                                                    .build(), from.getVersion());
  }
}
