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
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.StringJoiner;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;

/**
 * api representation of an Iceberg Alley Tag/Branch
 */
public final class Tag implements Base {

  private final String name;
  private final String id;
  private final long createMillis;
  private final Long expireMillis;
  @JsonProperty("tableSnapshots")
  private final Map<String, TableVersion> tableSnapshots;
  private final String baseTag;
  private final boolean deleted;
  private final Long version;
  private final long updateTime;

  public Tag() {
    this(
      null,
      null,
      0,
      null,
      Maps.newHashMap(),
      null,
      false,
      null,
      0L
    );
  }

  public Tag(
    String name,
    String uuid,
    long createMillis,
    Long expireMillis,
    Map<String, TableVersion> tableSnapshots,
    String baseTag,
    boolean deleted,
    Long version,
    long updateMillis
  ) {
    this.name = name;
    this.id = uuid;
    this.createMillis = createMillis;
    this.expireMillis = expireMillis;
    this.tableSnapshots = tableSnapshots;
    this.baseTag = baseTag;
    this.deleted = deleted;
    this.version = version;
    this.updateTime = updateMillis;
  }

  public static Tag tag(String name) {
    return tag(name, null);
  }

  public static Tag tag(String name, String baseTag) {
    return new Tag(
      name,
      null,
      0L,
      null,
      Maps.newHashMap(),
      baseTag,
      false,
      null,
      0L
    );
  }

  public String getName() {
    return name;
  }

  public String getId() {
    return id;
  }

  public long getCreateMillis() {
    return createMillis;
  }

  public OptionalLong getExpireMillis() {
    return expireMillis == null ? OptionalLong.empty() : OptionalLong.of(expireMillis);
  }

  public Set<String> tableSnapshotKeys() {
    return tableSnapshots.keySet();
  }

  public TableVersion getTableSnapshot(String key) {
    return tableSnapshots.get(key);
  }

  public boolean isDeleted() {
    return deleted;
  }

  public OptionalLong getVersion() {
    return version == null ? OptionalLong.empty() : OptionalLong.of(version);
  }

  public long getUpdateTime() {
    return updateTime;
  }

  public String getBaseTag() {
    return baseTag;
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
      Objects.equals(name, tag.name) &&
      Objects.equals(id, tag.id) &&
      Objects.equals(expireMillis, tag.expireMillis) &&
      Objects.equals(tableSnapshots, tag.tableSnapshots) &&
      Objects.equals(baseTag, tag.baseTag) &&
      Objects.equals(version, tag.version);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, id, createMillis, expireMillis, tableSnapshots, baseTag, deleted, version, updateTime);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", Tag.class.getSimpleName() + "[", "]")
      .add("name='" + name + "'")
      .add("uuid='" + id + "'")
      .add("createMillis=" + createMillis)
      .add("expireMillis=" + expireMillis)
      .add("tableSnapshots=" + tableSnapshots)
      .add("baseTag='" + baseTag + "'")
      .add("deleted=" + deleted)
      .add("version=" + version)
      .add("updateTime=" + updateTime)
      .toString();
  }

  public Tag incrementVersion() {
    return new Tag(
      name,
      id,
      createMillis,
      expireMillis,
      tableSnapshots,
      baseTag,
      deleted,
      version == null ? 1 : version+1,
      updateTime
    );
  }

  public Tag rename(String name) {
    return new Tag(
      name,
      id,
      createMillis,
      expireMillis,
      tableSnapshots,
      baseTag,
      deleted,
      version,
      updateTime
    );
  }

  public Tag withSnapshots(List<TableVersion> tableVersion) {
    tableVersion.forEach(t->tableSnapshots.put(t.getUuid(), t));
    return new Tag(
      name,
      id,
      createMillis,
      expireMillis,
      tableSnapshots,
      baseTag,
      deleted,
      version,
      updateTime
    );
  }

  public Tag withSnapshot(TableVersion tableVersion) {
    tableSnapshots.put(tableVersion.getUuid(), tableVersion);
    return new Tag(
      name,
      id,
      createMillis,
      expireMillis,
      tableSnapshots,
      baseTag,
      deleted,
      version,
      updateTime
    );
  }

  public Tag withUpdateTime(long updateTime) {
    return new Tag(
      name,
      id,
      createMillis,
      expireMillis,
      tableSnapshots,
      baseTag,
      deleted,
      version,
      updateTime
    );
  }

  public Tag withId(String uuid) {
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

  public Tag withDeleted(boolean deleted) {
    return new Tag(
      name,
      id,
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
