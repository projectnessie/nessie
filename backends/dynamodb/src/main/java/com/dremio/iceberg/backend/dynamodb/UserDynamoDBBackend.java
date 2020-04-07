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

import java.util.Map;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.dremio.iceberg.backend.dynamodb.model.User;
import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class UserDynamoDBBackend extends AbstractEntityDynamoDBBackend<User, com.dremio.iceberg.model.User> {
  private static final Joiner JOINER = Joiner.on(",");

  public UserDynamoDBBackend(AmazonDynamoDB client, DynamoDBMapperConfig config, DynamoDBMapper mapper) {
    super(client, config, mapper, User.class);
  }

  @Override
  protected User toDynamoDB(com.dremio.iceberg.model.User from) {
    return new User(
      from.getUsername(),
      from.getCreateMillis(),
      from.getPassword(),
      JOINER.join(from.getRoles()),
      from.isActive(),
      Long.parseLong(from.getExtraAttrs().get("version"))
    );
  }

  @Override
  protected com.dremio.iceberg.model.User fromDynamoDB(User from) {
    Map<String, String> extraAttrs = Maps.newHashMap();
    extraAttrs.put("version", Long.toString(from.getVersion()));
    return new com.dremio.iceberg.model.User(
      from.getUuid(),
      from.getPassword(),
      from.getCreateMillis(),
      from.isDeleted(),
      null,
      Sets.newHashSet(from.getRoles().split(",")),
      extraAttrs
    );
  }
}
