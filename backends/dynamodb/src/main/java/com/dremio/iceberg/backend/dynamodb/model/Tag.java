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

import java.util.Map;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBIgnore;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBVersionAttribute;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;

@DynamoDBTable(tableName="IcebergAlleyTags")
public class Tag implements Base {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private String name;
  private long createMillis;
  private Long expireMillis;
  private String uuid;
  private boolean deleted;
  private Map<String, String> tableSnapshots;
  private Long version;

  @DynamoDBAttribute(attributeName = "name")
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  @DynamoDBAttribute(attributeName = "createMillis")
  public long getCreateMillis() {
    return createMillis;
  }

  public void setCreateMillis(long createMillis) {
    this.createMillis = createMillis;
  }

  @DynamoDBAttribute(attributeName = "expireMillis")
  public Long getExpireMillis() {
    return expireMillis;
  }

  public void setExpireMillis(Long expireMillis) {
    this.expireMillis = expireMillis;
  }

  @DynamoDBAttribute(attributeName = "tableSnapshots")
  public Map<String, String> getTableSnapshots() {
    return tableSnapshots;
  }

  public void setTableSnapshots(Map<String, String> tableSnapshots) {
    this.tableSnapshots = tableSnapshots;
  }

  @DynamoDBHashKey(attributeName = "uuid")
  public String getUuid() {
    return uuid;
  }

  public void setUuid(String uuid) {
    this.uuid = uuid;
  }

  @DynamoDBAttribute(attributeName = "deleted")
  public boolean isDeleted() {
    return deleted;
  }

  public void setDeleted(boolean deleted) {
    this.deleted = deleted;
  }

  @DynamoDBVersionAttribute()
  public Long getVersion() {
    return version;
  }

  public void setVersion(Long version) {
    this.version = version;
  }

  public static Tag fromModelTag(com.dremio.iceberg.model.Tag oldTag) {
    Tag table = new Tag();
    table.setDeleted(oldTag.isDeleted());
    table.setUuid(oldTag.getUuid());
    table.setName(oldTag.getName());
    table.setCreateMillis(oldTag.getCreateMillis());
    table.setExpireMillis(oldTag.getExpireMillis());
    table.setTableSnapshots(oldTag.getTableSnapshots());
    table.setVersion(oldTag.getVersion());
    return table;
  }

  @DynamoDBIgnore
  public com.dremio.iceberg.model.Tag toModelTag() {
    Map<String, String> extraAttrs = Maps.newHashMap();
    extraAttrs.put("version", version == null ? null : version.toString());
    return new com.dremio.iceberg.model.Tag(
      name,
      uuid,
      createMillis,
      expireMillis,
      tableSnapshots,
      deleted,
      version
    );
  }
}
