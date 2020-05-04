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

import com.dremio.nessie.model.ImmutableTag;
import com.dremio.nessie.model.TableVersion;
import com.dremio.nessie.model.VersionedWrapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import software.amazon.awssdk.enhanced.dynamodb.extensions.annotations.DynamoDbVersionAttribute;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbConvertedBy;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbIgnore;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;

/**
 * Dynamodb table for Tag.
 */
@DynamoDbBean
public class Tag implements Base {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private String name;
  private long createMillis;
  private Long expireMillis;
  private String uuid;
  private boolean deleted;
  private Map<String, TableVersion> tableSnapshots;
  private String baseTag;
  private Long version;
  private long updateTime;

  public Tag() {

  }

  public Tag(String name, long createMillis, Long expireMillis, String uuid, boolean deleted,
             Map<String, TableVersion> tableSnapshots, String baseTag, Long version,
             long updateTime) {
    this.name = name;
    this.createMillis = createMillis;
    this.expireMillis = expireMillis;
    this.uuid = uuid;
    this.deleted = deleted;
    this.tableSnapshots = tableSnapshots;
    this.baseTag = baseTag;
    this.version = version;
    this.updateTime = updateTime;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public long getCreateMillis() {
    return createMillis;
  }

  public void setCreateMillis(long createMillis) {
    this.createMillis = createMillis;
  }

  public Long getExpireMillis() {
    return expireMillis;
  }

  public void setExpireMillis(Long expireMillis) {
    this.expireMillis = expireMillis;
  }

  @DynamoDbConvertedBy(Table.VersionTypeConverter.class)
  public Map<String, TableVersion> getTableSnapshots() {
    return tableSnapshots;
  }

  public void setTableSnapshots(Map<String, TableVersion> tableSnapshots) {
    this.tableSnapshots = tableSnapshots;
  }

  @DynamoDbPartitionKey
  public String getUuid() {
    return uuid;
  }

  public long getUpdateTime() {
    return updateTime;
  }

  public void setUpdateTime(long updateTime) {
    this.updateTime = updateTime;
  }

  public void setUuid(String uuid) {
    this.uuid = uuid;
  }

  public boolean isDeleted() {
    return deleted;
  }

  public void setDeleted(boolean deleted) {
    this.deleted = deleted;
  }

  @DynamoDbVersionAttribute
  public Long getVersion() {
    return version;
  }

  public void setVersion(Long version) {
    this.version = version;
  }

  public String getBaseTag() {
    return baseTag;
  }

  public void setBaseTag(String baseTag) {
    this.baseTag = baseTag;
  }

  public static Tag fromModelTag(VersionedWrapper<com.dremio.nessie.model.Tag> oldVersionedTag) {
    com.dremio.nessie.model.Tag oldTag = oldVersionedTag.getObj();
    Tag tag = new Tag(oldTag.getName(),
                      oldTag.getCreateMillis(),
                      oldTag.getExpireMillis().isPresent() ? oldTag.getExpireMillis().getAsLong()
                        : null,
                      oldTag.getId(),
                      oldTag.isDeleted(),
                      oldTag.getTableVersions(),
                      oldTag.getBaseTag(),
                      oldVersionedTag.getVersion().isPresent() ? oldVersionedTag.getVersion()
                                                                                .getAsLong() : null,
                      oldTag.getUpdateTime());
    return tag;
  }

  @DynamoDbIgnore
  public VersionedWrapper<com.dremio.nessie.model.Tag> toModelTag() {
    return new VersionedWrapper<>(ImmutableTag.builder().name(name)
                                              .id(uuid)
                                              .createMillis(createMillis)
                                              .expireMillis(expireMillis)
                                              .tableVersions(tableSnapshots)
                                              .baseTag(baseTag)
                                              .isDeleted(deleted)
                                              .updateTime(updateTime)
                                              .build(),
                                  version);
  }
}
