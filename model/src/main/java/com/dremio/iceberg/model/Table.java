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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;

/**
 * API representation of an iceberg table.
 */
public final class Table implements Base {

  private final String tableName;
  private final String baseLocation;
  private final String namespace;
  private final String metadataLocation;
  private final String id;
  private final boolean deleted;
  private final String sourceId;
  private final List<Snapshot> snapshots;
  private final String schema;
  private final long updateTime;
  @JsonProperty("versionList")
  private final Map<String, TableVersion> versionList;

  public Table() {
    this(null,
         null,
         null,
         null,
         null,
         false,
         null,
         Lists.newArrayList(),
         null,
         Long.MIN_VALUE,
         Maps.newHashMap()
    );
  }

  public Table(String tableName,
               String baseLocation,
               String namespace,
               String metadataLocation,
               String id,
               boolean deleted,
               String sourceId,
               List<Snapshot> snapshots,
               String schema,
               long updateTime,
               Map<String, TableVersion> versionList) {
    this.tableName = tableName;
    this.baseLocation = baseLocation;
    this.namespace = namespace;
    this.metadataLocation = metadataLocation;
    this.id = id;
    this.deleted = deleted;
    this.sourceId = sourceId;
    this.snapshots = snapshots;
    this.schema = schema;
    this.updateTime = updateTime;
    this.versionList = versionList;
  }

  public static TableBuilder builder() {
    return new TableBuilder();
  }

  public static TableBuilder copyTable(Table table) {
    return new TableBuilder(table);
  }

  public static long now() {
    return ZonedDateTime.now(ZoneId.of("UTC")).toInstant().toEpochMilli();
  }

  public String getTableName() {
    return this.tableName;
  }

  public String getBaseLocation() {
    return this.baseLocation;
  }

  public String getNamespace() {
    return this.namespace;
  }

  public String getMetadataLocation() {
    return this.metadataLocation;
  }

  public String getId() {
    return this.id;
  }

  public boolean isDeleted() {
    return this.deleted;
  }

  public String getSourceId() {
    return this.sourceId;
  }

  public List<Snapshot> getSnapshots() {
    return ImmutableList.copyOf(this.snapshots);
  }

  public String getSchema() {
    return this.schema;
  }

  public long getUpdateTime() {
    return this.updateTime;
  }

  public Set<String> tableVersions() {
    return this.versionList.keySet();
  }

  public TableVersion getTableVersion(String key) {
    return this.versionList.get(key);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Table table = (Table) o;
    return Objects.equals(tableName, table.tableName)
           && Objects.equals(namespace, table.namespace)
           && Objects.equals(metadataLocation, table.metadataLocation)
           && Objects.equals(id, table.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableName, namespace, metadataLocation, id);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", Table.class.getSimpleName() + "[", "]")
      .add("tableName='" + tableName + "'")
      .add("baseLocation='" + baseLocation + "'")
      .add("namespace='" + namespace + "'")
      .add("metadataLocation='" + metadataLocation + "'")
      .add("id='" + id + "'")
      .add("deleted=" + deleted)
      .add("sourceId='" + sourceId + "'")
      .add("snapshots=" + snapshots)
      .add("schema='" + schema + "'")
      .add("updateTime=" + updateTime)
      .add("versionList=" + versionList)
      .toString();
  }

  @JsonIgnore
  public TableBuilder withMetadataLocation(String metadataLocation) {
    TableBuilder builder = Table.copyTable(this);
    if (builder.metadataLocation != null
        && builder.versionList.containsKey(this.metadataLocation)) {
      TableVersion currentVersion = versionList.get(this.metadataLocation);
      currentVersion = new TableVersion(
        currentVersion.getUuid(),
        currentVersion.getMetadataLocation(),
        currentVersion.getCreateTime(),
        now(),
        currentVersion.getSnapshotId().isPresent() ? currentVersion.getSnapshotId().getAsLong() :
          null);
      versionList.put(metadataLocation, currentVersion);
    }
    TableVersion newVersion = new TableVersion(id,
                                               metadataLocation,
                                               updateTime,
                                               null,
                                               snapshots.isEmpty() ? null
                                                 : snapshots.get(snapshots.size() - 1)
                                                            .getSnapshotId()
    );
    builder.versionList.put(metadataLocation, newVersion);
    return builder;
  }

  /**
   * builder utility for table.
   */
  public static class TableBuilder {

    private String tableName;
    private String baseLocation;
    private String namespace;
    private String metadataLocation;
    private String id;
    private boolean deleted;
    private String sourceId;
    private List<Snapshot> snapshots;
    private String schema;
    private long updateTime;
    private Map<String, TableVersion> versionList;

    TableBuilder() {
    }

    TableBuilder(Table table) {
      this.tableName = table.tableName;
      this.baseLocation = table.baseLocation;
      this.namespace = table.namespace;
      this.metadataLocation = table.metadataLocation;
      this.id = table.id;
      this.deleted = table.deleted;
      this.sourceId = table.sourceId;
      this.snapshots = table.snapshots;
      this.schema = table.schema;
      this.updateTime = table.updateTime;
      this.versionList = table.versionList;
    }

    public TableBuilder tableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    public TableBuilder baseLocation(String baseLocation) {
      this.baseLocation = baseLocation;
      return this;
    }

    public TableBuilder namespace(String namespace) {
      this.namespace = namespace;
      return this;
    }

    public TableBuilder metadataLocation(String metadataLocation) {
      this.metadataLocation = metadataLocation;
      return this;
    }

    public TableBuilder id(String id) {
      this.id = id;
      return this;
    }

    public TableBuilder deleted(boolean deleted) {
      this.deleted = deleted;
      return this;
    }

    public TableBuilder sourceId(String sourceId) {
      this.sourceId = sourceId;
      return this;
    }

    public TableBuilder snapshots(List<Snapshot> snapshots) {
      this.snapshots = snapshots;
      return this;
    }

    public TableBuilder snapshot(Snapshot snapshot) {
      if (this.snapshots == null) {
        this.snapshots = Lists.newArrayList();
      }
      this.snapshots.add(snapshot);
      return this;
    }

    public TableBuilder schema(String schema) {
      this.schema = schema;
      return this;
    }

    public TableBuilder updateTime(long updateTime) {
      this.updateTime = updateTime;
      return this;
    }

    public TableBuilder versionList(Map<String, TableVersion> versionList) {
      this.versionList = versionList;
      return this;
    }

    public Table build() {
      return new Table(tableName,
                       baseLocation,
                       namespace,
                       metadataLocation,
                       id,
                       deleted,
                       sourceId,
                       snapshots == null ? Lists.newArrayList() : snapshots,
                       schema,
                       updateTime,
                       versionList == null ? Maps.newHashMap() : versionList);
    }

    @Override
    public String toString() {
      return new StringJoiner(", ", TableBuilder.class.getSimpleName() + "[", "]")
        .add("tableName='" + tableName + "'")
        .add("baseLocation='" + baseLocation + "'")
        .add("namespace='" + namespace + "'")
        .add("metadataLocation='" + metadataLocation + "'")
        .add("id='" + id + "'")
        .add("deleted=" + deleted)
        .add("sourceId='" + sourceId + "'")
        .add("snapshots=" + snapshots)
        .add("schema='" + schema + "'")
        .add("updateTime=" + updateTime)
        .add("versionList=" + versionList)
        .toString();
    }
  }

}
