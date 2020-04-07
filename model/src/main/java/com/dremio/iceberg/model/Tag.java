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
package com.dremio.iceberg.model;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.Maps;

public class Tag implements Base {

  private String name;
  private String uuid;
  private long createMillis;
  private Long expireMillis;
  private Map<String, TableVersion> tableSnapshots;
  private String baseTag;
  private boolean deleted;
  private Long version;
  private long updateTime;

  @JsonCreator
  public Tag(
    @JsonProperty("name") String name,
    @JsonProperty("uuid") String uuid,
    @JsonProperty("createMillis") long createMillis,
    @JsonProperty("expireMillis") Long expireMillis,
    @JsonProperty("tableSnapshots") Map<String, TableVersion> tableSnapshots,
    @JsonProperty("baseTag") String baseTag,
    @JsonProperty("deleted") boolean deleted,
    @JsonProperty("version") Long version,
    @JsonProperty("updateMillis") long updateMillis
    ) {
    this.name = name;
    this.uuid = uuid;
    this.createMillis = createMillis;
    this.expireMillis = expireMillis;
    this.tableSnapshots = tableSnapshots;
    this.baseTag = baseTag;
    this.deleted = deleted;
    this.version = version;
    this.updateTime = updateMillis;
  }

  public Tag(String name) {
    this.name = name;
  }

  public Tag(String name, String baseTag) {
    this.name = name;
    this.baseTag = baseTag;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getUuid() {
    return uuid;
  }

  public void setUuid(String uuid) {
    this.uuid = uuid;
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

  public Map<String, TableVersion> getTableSnapshots() {
    return tableSnapshots;
  }

  public void setTableSnapshots(Map<String, TableVersion> tableSnapshots) {
    this.tableSnapshots = tableSnapshots;
  }

  public boolean isDeleted() {
    return deleted;
  }

  public void setDeleted(boolean deleted) {
    this.deleted = deleted;
  }

  public Long getVersion() {
    return version;
  }

  public void setVersion(Long version) {
    this.version = version;
  }

  public long getUpdateTime() {
    return updateTime;
  }

  public void setUpdateTime(long updateTime) {
    this.updateTime = updateTime;
  }

  public String getBaseTag() {
    return baseTag;
  }

  public void setBaseTag(String baseTag) {
    this.baseTag = baseTag;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Tag tag = (Tag) o;
    return createMillis == tag.createMillis &&
      deleted == tag.deleted &&
      updateTime == tag.updateTime &&
      Objects.equal(name, tag.name) &&
      Objects.equal(uuid, tag.uuid) &&
      Objects.equal(expireMillis, tag.expireMillis) &&
      Objects.equal(tableSnapshots, tag.tableSnapshots) &&
      Objects.equal(baseTag, tag.baseTag) &&
      Objects.equal(version, tag.version);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(name, uuid, createMillis, expireMillis, tableSnapshots, baseTag, deleted, version, updateTime);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
      .add("name", name)
      .add("uuid", uuid)
      .add("createMillis", createMillis)
      .add("expireMillis", expireMillis)
      .add("tableSnapshots", tableSnapshots)
      .add("baseTag", baseTag)
      .add("deleted", deleted)
      .add("version", version)
      .add("updateTime", updateTime)
      .toString();
  }

  public Tag rename(String name) {
    return new Tag(
      name,
      uuid,
      createMillis,
      expireMillis,
      tableSnapshots,
      baseTag,
      deleted,
      version,
      updateTime
    );
  }
}
