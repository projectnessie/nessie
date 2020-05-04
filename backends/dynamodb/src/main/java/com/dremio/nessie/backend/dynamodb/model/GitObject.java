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
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.enhanced.dynamodb.AttributeConverter;
import software.amazon.awssdk.enhanced.dynamodb.AttributeValueType;
import software.amazon.awssdk.enhanced.dynamodb.EnhancedType;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbConvertedBy;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

@DynamoDbBean
public class GitObject implements Base {

  private String uuid;
  private int type;
  private byte[] data;
  private long updateTime;

  public GitObject() {

  }

  public GitObject(String uuid,
                   int type,
                   byte[] data,
                   long updateTime) {
    this.uuid = uuid;
    this.type = type;
    this.data = data;
    this.updateTime = updateTime;
  }

  @DynamoDbPartitionKey
  public String getUuid() {
    return uuid;
  }


  @Override
  public long getUpdateTime() {
    return updateTime;
  }

  @Override
  public String getName() {
    return uuid;
  }

  public int getType() {
    return type;
  }

  public byte[] getData() {
    return data;
  }

  public void setUuid(String uuid) {
    this.uuid = uuid;
  }

  public void setType(int type) {
    this.type = type;
  }

  @DynamoDbConvertedBy(ByteArrayConverter.class)
  public void setData(byte[] data) {
    this.data = data;
  }

  public void setUpdateTime(long updateTime) {
    this.updateTime = updateTime;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GitObject gitObject = (GitObject) o;
    return Objects.equals(uuid, gitObject.uuid);
  }

  @Override
  public int hashCode() {
    return Objects.hash(uuid);
  }

  /**
   * Convert Snapshot to String for DynamoDb.
   */
  public static class ByteArrayConverter implements AttributeConverter<byte[]> {

    @Override
    public AttributeValue transformFrom(byte[] data) {
      return AttributeValue.builder().b(SdkBytes.fromByteArray(data)).build();
    }

    @Override
    public byte[] transformTo(AttributeValue attributeValue) {
      return attributeValue.b().asByteArray();
    }

    @Override
    public EnhancedType<byte[]> type() {
      return EnhancedType.of(byte[].class);
    }

    @Override
    public AttributeValueType attributeValueType() {
      return AttributeValueType.B;
    }
  }
}
