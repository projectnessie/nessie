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

import com.dremio.iceberg.backend.dynamodb.model.User;
import com.dremio.iceberg.model.VersionedWrapper;
import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/**
 * dynamodb backend for Users.
 */
public class UserDynamoDbBackend
    extends AbstractEntityDynamoDbBackend<User, com.dremio.iceberg.model.User> {
  private static final Joiner JOINER = Joiner.on(",");

  public UserDynamoDbBackend(DynamoDbClient client, DynamoDbEnhancedClient mapper) {
    super(client, mapper, User.class, "IcebergAlleyUser");
  }


  @Override
  protected User toDynamoDB(VersionedWrapper<com.dremio.iceberg.model.User> wrappedFrom) {
    com.dremio.iceberg.model.User from = wrappedFrom.getObj();
    return new User(
      from.getId(),
      from.getCreateMillis(),
      from.getPassword(),
      JOINER.join(from.getRoles()),
      from.isActive(),
      wrappedFrom.getVersion().isPresent() ? wrappedFrom.getVersion().getAsLong() : null,
      from.getUpdateMillis()
    );
  }

  @Override
  protected VersionedWrapper<com.dremio.iceberg.model.User> fromDynamoDB(User from) {
    return new VersionedWrapper<>(new com.dremio.iceberg.model.User(
      from.getUuid(),
      from.getPassword(),
      from.getCreateMillis(),
      from.isDeleted(),
      null,
      Sets.newHashSet(from.getRoles().split(",")),
      from.getUpdateTime()
    ), from.getVersion());
  }
}
