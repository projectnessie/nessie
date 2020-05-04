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

import com.dremio.nessie.backend.dynamodb.model.GitRef;
import com.dremio.nessie.model.ImmutableGitRef;
import com.dremio.nessie.model.VersionedWrapper;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

public class GitRefDynamoDbBackend extends AbstractEntityDynamoDbBackend<GitRef,
    com.dremio.nessie.model.GitRef> {


  public GitRefDynamoDbBackend(DynamoDbClient client,
                               DynamoDbEnhancedClient mapper) {
    super(client, mapper, GitRef.class, "NessieGitRefDatabase", null);
  }

  @Override
  protected GitRef toDynamoDB(VersionedWrapper<com.dremio.nessie.model.GitRef> from) {
    return new GitRef(
      from.getObj().getId(),
      from.getObj().isDeleted(),
      from.getObj().getUpdateTime(),
      from.getObj().getRefId(),
      from.getVersion().isPresent() ? from.getVersion().getAsLong() : null
    );
  }

  @Override
  protected VersionedWrapper<com.dremio.nessie.model.GitRef> fromDynamoDB(GitRef from) {
    return new VersionedWrapper<>(ImmutableGitRef.builder()
                                                 .isDeleted(from.isDeleted())
                                                 .updateTime(from.getUpdateTime())
                                                 .refId(from.getName())
                                                 .id(from.getUuid())
                                                 .build(),
                                  from.getVersion());
  }
}
