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

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.Maps;

public class Tag {

  private String name;
  private String uuid;
  private long createMillis;
  private Long expireMillis;
  private Map<String, String> tableSnapshots = Maps.newHashMap();
  private boolean deleted;
  private Map<String, String> extraAttrs = Maps.newHashMap();

  @JsonIgnore
  private String etag;

  @JsonCreator
  public Tag(
    @JsonProperty("name") String name,
    @JsonProperty("uuid") String uuid,
    @JsonProperty("createMillis") long createMillis,
    @JsonProperty("expireMillis") Long expireMillis,
    @JsonProperty("snapshots") Map<String, String> tableSnapshots,
    @JsonProperty("deleted") boolean deleted,
    @JsonProperty("extraAttrs") Map<String, String> extraAttrs
    ) {
    this.name = name;
    this.uuid = uuid;
    this.createMillis = createMillis;
    this.expireMillis = expireMillis;
    this.tableSnapshots = tableSnapshots;
    this.deleted = deleted;
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

  public Map<String, String> getTableSnapshots() {
    return tableSnapshots;
  }

  public void setTableSnapshots(Map<String, String> tableSnapshots) {
    this.tableSnapshots = tableSnapshots;
  }

  public boolean isDeleted() {
    return deleted;
  }

  public void setDeleted(boolean deleted) {
    this.deleted = deleted;
  }

  public String getEtag() {
    return etag;
  }

  public void setEtag(String etag) {
    this.etag = etag;
  }

  public Map<String, String> getExtraAttrs() {
    return extraAttrs;
  }

  public void setExtraAttrs(Map<String, String> extraAttrs) {
    this.extraAttrs = extraAttrs;
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
      Objects.equal(name, tag.name) &&
      Objects.equal(uuid, tag.uuid) &&
      Objects.equal(expireMillis, tag.expireMillis) &&
      Objects.equal(tableSnapshots, tag.tableSnapshots);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(name, uuid, createMillis, expireMillis, tableSnapshots);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
      .add("name", name)
      .add("uuid", uuid)
      .add("createMillis", createMillis)
      .add("expireMillis", expireMillis)
      .add("tableSnapshots", tableSnapshots)
      .add("deleted", deleted)
      .add("extraAttrs", extraAttrs)
      .toString();
  }
}
