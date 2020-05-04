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

package com.dremio.iceberg.backend.dynamodb;

import com.dremio.iceberg.backend.dynamodb.model.GitObject;
import com.dremio.iceberg.model.ImmutableGitObject;
import com.dremio.iceberg.model.ImmutableGitRef;
import com.dremio.iceberg.model.VersionedWrapper;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectIdRef.Unpeeled;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Ref.Storage;
import org.eclipse.jgit.lib.SymbolicRef;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

public class GitObjectDynamoDbBackend extends AbstractEntityDynamoDbBackend<GitObject,
    com.dremio.iceberg.model.GitContainer> {


  public GitObjectDynamoDbBackend(DynamoDbClient client,
                                  DynamoDbEnhancedClient mapper) {
    super(client, mapper, GitObject.class, "NessieGitObjectDatabase", "refType");
  }

  @Override
  protected GitObject toDynamoDB(VersionedWrapper<com.dremio.iceberg.model.GitContainer> from) {
    if (from.getObj() instanceof com.dremio.iceberg.model.GitObject) {
      return new GitObject(
        from.getObj().getId(),
        from.getObj().getType(),
        from.getObj().getData().length == 0 ? new byte[]{-1} : from.getObj().getData(),
        from.getObj().isDeleted(),
        from.getObj().getUpdateTime(),
        null,
        "obj",
        null,
        from.getVersion().isPresent() ? from.getVersion().getAsLong() : null
      );
    } else {
      return new GitObject(
        from.getObj().getId(),
        from.getObj().getType(),
        from.getObj().getData(),
        from.getObj().isDeleted(),
        from.getObj().getUpdateTime(),
        from.getObj().getRef().isSymbolic() ? from.getObj().getRef().getTarget().getName()
          : from.getObj().getRef().getObjectId().name(),
        "ref",
        from.getObj().getTargetRef() != null ? from.getObj().getTargetRef().getName() : null,
        from.getVersion().isPresent() ? from.getVersion().getAsLong() : null
      );
    }
  }

  @Override
  protected VersionedWrapper<com.dremio.iceberg.model.GitContainer> fromDynamoDB(GitObject from) {
    if (from.getRefType().equals("ref")) {
      Ref ref;
      Ref targetRef = null;
      if (from.getTargetRef() != null) {
        targetRef = new Unpeeled(Storage.NETWORK,
                                 from.getTargetRef(),
                                 ObjectId.fromString(from.getName()));
        ref = new SymbolicRef(from.getUuid(), targetRef);
      } else {
        ref = new Unpeeled(Storage.NETWORK, from.getUuid(), ObjectId.fromString(from.getName()));
      }
      return new VersionedWrapper<>(ImmutableGitRef.builder()
                                                   .isDeleted(from.isDeleted())
                                                   .updateTime(from.getUpdateTime())
                                                   .ref(ref)
                                                   .targetRef(targetRef)
                                                   .build(),
                                    from.getVersion());
    }
    return new VersionedWrapper<>(ImmutableGitObject.builder()
                                                    .data(from.getData().length == 1
                                                          && from.getData()[0] == ((byte) -1)
                                                            ? new byte[0] : from.getData())
                                                    .isDeleted(from.isDeleted())
                                                    .objectId(ObjectId.fromString(from.getUuid()))
                                                    .type(from.getType())
                                                    .updateTime(from.getUpdateTime())
                                                    .build(), from.getVersion());
  }
}
