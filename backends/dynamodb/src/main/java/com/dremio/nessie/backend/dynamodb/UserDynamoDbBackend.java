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

import com.dremio.nessie.backend.dynamodb.model.User;
import com.dremio.nessie.model.ImmutableUser;
import com.dremio.nessie.model.VersionedWrapper;
import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/**
 * dynamodb backend for Users.
 */
public class UserDynamoDbBackend
    extends AbstractEntityDynamoDbBackend<User, com.dremio.nessie.model.User> {

  private static final Joiner JOINER = Joiner.on(",");

  public UserDynamoDbBackend(DynamoDbClient client, DynamoDbEnhancedClient mapper) {
    super(client, mapper, User.class, "NessieUsers", "namespace");
  }


  @Override
  protected User toDynamoDB(VersionedWrapper<com.dremio.nessie.model.User> wrappedFrom) {
    com.dremio.nessie.model.User from = wrappedFrom.getObj();
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
  protected VersionedWrapper<com.dremio.nessie.model.User> fromDynamoDB(User from) {
    return new VersionedWrapper<>(ImmutableUser.builder()
                                               .id(from.getUuid())
                                               .password(from.getPassword())
                                               .createMillis(from.getCreateMillis())
                                               .isActive(!from.isDeleted())
                                               .email(null)
                                               .roles(Sets.newHashSet(from.getRoles().split(",")))
                                               .updateMillis(from.getUpdateTime())
                                               .build(),
                                  from.getVersion());
  }
}
