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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.StringJoiner;

/**
 * api representation of an Iceberg Alley Tag/Branch.
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
  private final long updateTime;

  public Tag() {
    this(null,
         null,
         0,
         null,
         Maps.newHashMap(),
         null,
         false,
         0L
    );
  }

  public Tag(String name,
             String uuid,
             long createMillis,
             Long expireMillis,
             Map<String, TableVersion> tableSnapshots,
             String baseTag,
             boolean deleted,
             long updateMillis
  ) {
    this.name = name;
    this.id = uuid;
    this.createMillis = createMillis;
    this.expireMillis = expireMillis;
    this.tableSnapshots = tableSnapshots;
    this.baseTag = baseTag;
    this.deleted = deleted;
    this.updateTime = updateMillis;
  }

  public static TagBuilder builder() {
    return new TagBuilder();
  }

  public static TagBuilder copyOf(Tag tag) {
    return new TagBuilder(tag);
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
    return name.equals(tag.name) && Objects.equals(id, tag.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, id);
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
      .add("updateTime=" + updateTime)
      .toString();
  }


  /**
   * builder for Tag object.
   */
  public static class TagBuilder {

    private String name;
    private String id;
    private long createMillis;
    private Long expireMillis;
    private Map<String, TableVersion> tableSnapshots;
    private String baseTag;
    private boolean deleted;
    private long updateTime;

    TagBuilder() {
    }

    TagBuilder(Tag tag) {
      this.name = tag.name;
      this.id = tag.id;
      this.createMillis = tag.createMillis;
      this.expireMillis = tag.expireMillis;
      this.tableSnapshots = tag.tableSnapshots;
      this.baseTag = tag.baseTag;
      this.deleted = tag.deleted;
      this.updateTime = tag.updateTime;
    }

    public TagBuilder name(String name) {
      this.name = name;
      return this;
    }

    public TagBuilder id(String id) {
      this.id = id;
      return this;
    }

    public TagBuilder createMillis(long createMillis) {
      this.createMillis = createMillis;
      return this;
    }

    public TagBuilder expireMillis(Long expireMillis) {
      this.expireMillis = expireMillis;
      return this;
    }

    public TagBuilder tableSnapshots(Map<String, TableVersion> tableSnapshots) {
      this.tableSnapshots = tableSnapshots;
      return this;
    }

    public String baseTag() {
      return baseTag;
    }

    public TagBuilder baseTag(String baseTag) {
      this.baseTag = baseTag;
      return this;
    }

    public TagBuilder deleted(boolean deleted) {
      this.deleted = deleted;
      return this;
    }

    public TagBuilder updateTime(long updateTime) {
      this.updateTime = updateTime;
      return this;
    }

    public Tag build() {
      return new Tag(name, id, createMillis, expireMillis, tableSnapshots, baseTag, deleted,
                     updateTime);
    }

    @Override
    public String toString() {
      return new StringJoiner(", ", TagBuilder.class.getSimpleName() + "[", "]")
        .add("name='" + name + "'")
        .add("id='" + id + "'")
        .add("createMillis=" + createMillis)
        .add("expireMillis=" + expireMillis)
        .add("tableSnapshots=" + tableSnapshots)
        .add("baseTag='" + baseTag + "'")
        .add("deleted=" + deleted)
        .add("updateTime=" + updateTime)
        .toString();
    }
  }
}
