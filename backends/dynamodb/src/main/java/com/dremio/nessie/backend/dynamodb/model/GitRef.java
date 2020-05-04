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

package com.dremio.nessie.backend.dynamodb.model;

import java.util.Objects;
import software.amazon.awssdk.enhanced.dynamodb.extensions.annotations.DynamoDbVersionAttribute;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;

@DynamoDbBean
public class GitRef implements Base {

  private String uuid;
  private long updateTime;
  private String ref;
  private Long version;

  public GitRef() {

  }

  /**
   * Mutable Bean object to be used w/ DynamoDb's object mapper. Used with git refs.
   */
  public GitRef(String uuid,
                long updateTime,
                String ref,
                Long version) {
    this.uuid = uuid;
    this.updateTime = updateTime;
    this.ref = ref;
    this.version = version;
  }

  @DynamoDbPartitionKey
  public String getUuid() {
    return uuid;
  }

  @DynamoDbVersionAttribute
  public Long getVersion() {
    return version;
  }

  @Override
  public long getUpdateTime() {
    return updateTime;
  }

  @Override
  public String getName() {
    return ref;
  }

  public void setUuid(String uuid) {
    this.uuid = uuid;
  }

  public void setUpdateTime(long updateTime) {
    this.updateTime = updateTime;
  }

  public void setRef(String ref) {
    this.ref = ref;
  }

  public void setVersion(Long version) {
    this.version = version;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GitRef gitObject = (GitRef) o;
    return Objects.equals(uuid, gitObject.uuid);
  }

  @Override
  public int hashCode() {
    return Objects.hash(uuid);
  }

  public String getRef() {
    return ref;
  }

}
