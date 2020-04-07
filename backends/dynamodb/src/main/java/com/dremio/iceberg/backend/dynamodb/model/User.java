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
package com.dremio.iceberg.backend.dynamodb.model;


import software.amazon.awssdk.enhanced.dynamodb.extensions.annotations.DynamoDbVersionAttribute;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;

/**
 * Dynamodb table for User
 */
@DynamoDbBean
public class User implements Base {
  private String username;
  private long createMillis;
  private String password;
  private String roles;
  private boolean active;
  private Long version;
  private long updateTime;

  public User() {

  }

  public User(String username,
              long createMillis,
              String password,
              String roles,
              boolean active,
              Long version,
              long updateTime) {
    this.username = username;
    this.createMillis = createMillis;
    this.password = password;
    this.roles = roles;
    this.active = active;
    this.version = version;
    this.updateTime = updateTime;
  }


  public long getCreateMillis() {
    return createMillis;
  }

  public void setCreateMillis(long createMillis) {
    this.createMillis = createMillis;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public String getRoles() {
    return roles;
  }

  public void setRoles(String roles) {
    this.roles = roles;
  }

  @Override
  public boolean isDeleted() {
    return !active;
  }

  @Override
  @DynamoDbVersionAttribute
  public Long getVersion() {
    return version;
  }

  @Override
  @DynamoDbPartitionKey
  public String getUuid() {
    return username;
  }

  public long getUpdateTime() {
    return updateTime;
  }

  public void setUpdateTime(long updateTime) {
    this.updateTime = updateTime;
  }

  public void setActive(boolean active) {
    this.active = active;
  }

  public void setUuid(String uuid) {
    this.username = uuid;
  }

  public void setVersion(Long version) {
    this.version = version;
  }
}
