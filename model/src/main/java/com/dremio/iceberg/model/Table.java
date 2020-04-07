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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * API representation of an iceberg table
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
  private final Long version;
  private final long updateTime;
  @JsonProperty("versionList")
  private final Map<String, TableVersion> versionList;

  public Table() {
    this(null,
      null,
      null,
      null,
      null,
      null,
      null,
      Lists.newArrayList(),
      false,
      null,
      0L,
      Maps.newHashMap());
  }

  public Table(
    String uuid,
    String tableName,
    String namespace,
    String baseLocation,
    String metadataLocation,
    String sourceId,
    String schema,
    List<Snapshot> snapshots,
    boolean deleted,
    Long version,
    long updateTime,
    Map<String, TableVersion> versionList) {
    this.id = uuid;
    this.tableName = tableName;
    this.namespace = namespace;
    this.metadataLocation = metadataLocation;
    this.sourceId = sourceId;
    this.schema = schema;
    this.snapshots = snapshots == null ? Lists.newArrayList() : snapshots;
    this.deleted = deleted;
    this.baseLocation = baseLocation;
    this.version = version;
    this.updateTime = updateTime;
    this.versionList = versionList == null ? Maps.newHashMap() : versionList;
  }

  public static Table table(String tableName, String baseLocation) {
    return table(tableName, null, baseLocation);
  }

  public static Table table(String tableName, String namespace, String baseLocation) {
    return new Table(
      null,
      tableName,
      namespace,
      baseLocation,
      null,
      null,
      null,
      Lists.newArrayList(),
      false,
      null,
      0L,
      Maps.newHashMap()
    );
  }

  public String getTableName() {
    return tableName;
  }

  public String getBaseLocation() {
    return baseLocation;
  }

  public boolean isDeleted() {
    return deleted;
  }

  public String getId() {
    return id;
  }

  public String getNamespace() {
    return namespace;
  }

  public String getSourceId() {
    return sourceId;
  }

  public List<Snapshot> getSnapshots() {
    return ImmutableList.copyOf(snapshots);
  }

  public void addSnapshot(Snapshot snapshot) {
    if (snapshot == null) {
      return;
    }
    snapshots.add(snapshot);
  }

  public String getMetadataLocation() {
    return metadataLocation;
  }

  public String getSchema() {
    return schema;
  }

  public OptionalLong getVersion() {
    return version == null ? OptionalLong.empty() : OptionalLong.of(version);
  }

  public long getUpdateTime() {
    return updateTime;
  }

  public TableVersion getTableVersion(String tableVersion) {
    return versionList.get(tableVersion);
  }

  public Set<String> tableVersions() {
    return versionList.keySet();
  }

  @JsonIgnore
  public Table incrementVersion() {
    if (metadataLocation == null) {
      return this;
    }
    TableVersion currentVersion = versionList.get(metadataLocation);
    if (currentVersion != null) {
      currentVersion = new TableVersion(
        currentVersion.getUuid(),
        currentVersion.getMetadataLocation(),
        currentVersion.getVersion().isPresent() ? currentVersion.getVersion().getAsLong() : null,
        currentVersion.getCreateTime(),
        updateTime,
        currentVersion.getSnapshotId().isPresent() ? currentVersion.getSnapshotId().getAsLong() : null);
      versionList.put(metadataLocation, currentVersion);
    }
    TableVersion newVersion = new TableVersion(
      id,
      metadataLocation,
      version,
      updateTime,
      null,
      snapshots.isEmpty() ? null : snapshots.get(snapshots.size() - 1).getSnapshotId()
    );
    versionList.put(metadataLocation, newVersion);
    return new Table(
      id,
      tableName,
      namespace,
      baseLocation,
      metadataLocation,
      sourceId,
      schema,
      snapshots,
      deleted,
      version == null ? 1L : version+1L,
      updateTime,
      versionList
    );
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
    return deleted == table.deleted &&
      updateTime == table.updateTime &&
      Objects.equals(tableName, table.tableName) &&
      Objects.equals(baseLocation, table.baseLocation) &&
      Objects.equals(namespace, table.namespace) &&
      Objects.equals(metadataLocation, table.metadataLocation) &&
      Objects.equals(id, table.id) &&
      Objects.equals(sourceId, table.sourceId) &&
      Objects.equals(schema, table.schema) &&
      Objects.equals(version, table.version);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableName, baseLocation, namespace, metadataLocation, id, deleted, sourceId, schema, version, updateTime);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", Table.class.getSimpleName() + "[", "]")
      .add("tableName='" + tableName + "'")
      .add("baseLocation='" + baseLocation + "'")
      .add("namespace='" + namespace + "'")
      .add("metadataLocation='" + metadataLocation + "'")
      .add("uuid='" + id + "'")
      .add("deleted=" + deleted)
      .add("sourceId='" + sourceId + "'")
      .add("snapshots=" + snapshots)
      .add("schema='" + schema + "'")
      .add("version=" + version)
      .add("updateTime=" + updateTime)
      .add("versionList=" + versionList)
      .toString();
  }

  @JsonIgnore
  public Table rename(String name, String namespace) {
    return new Table(id,
      name,
      namespace,
      baseLocation,
      metadataLocation,
      sourceId,
      schema,
      snapshots,
      deleted,
      version,
      updateTime,
      versionList);
  }

  @JsonIgnore
  public Table withMetadataLocation(String metadataLoc) {
    return new Table(
      id,
      tableName,
      namespace,
      baseLocation,
      metadataLoc,
      sourceId,
      schema,
      snapshots,
      deleted,
      version,
      updateTime,
      versionList);
  }

  @JsonIgnore
  public Table updateTime(long updateTime) {
    return new Table(
      id,
      tableName,
      namespace,
      baseLocation,
      metadataLocation,
      sourceId,
      schema,
      snapshots,
      deleted,
      version,
      updateTime,
      versionList
    );
  }

  @JsonIgnore
  public Table withId(String uuid) {
    return new Table(
      uuid,
      tableName,
      namespace,
      baseLocation,
      metadataLocation,
      sourceId,
      schema,
      snapshots,
      deleted,
      version,
      updateTime,
      versionList
    );
  }

  @JsonIgnore
  public Table withDeleted(boolean deleted) {
    return new Table(
      id,
      tableName,
      namespace,
      baseLocation,
      metadataLocation,
      sourceId,
      schema,
      snapshots,
      deleted,
      version,
      updateTime,
      versionList
    );
  }

  @JsonIgnore
  public Table withUpdateTime(long updateTime) {
    return new Table(
      id,
      tableName,
      namespace,
      baseLocation,
      metadataLocation,
      sourceId,
      schema,
      snapshots,
      deleted,
      version,
      updateTime,
      versionList
    );
  }

  @JsonIgnore
  public Table withSchema(String schema) {
    return new Table(
      id,
      tableName,
      namespace,
      baseLocation,
      metadataLocation,
      sourceId,
      schema,
      snapshots,
      deleted,
      version,
      updateTime,
      versionList
    );
  }

  @JsonIgnore
  public Table withSourceId(String sourceId) {
    return new Table(
      id,
      tableName,
      namespace,
      baseLocation,
      metadataLocation,
      sourceId,
      schema,
      snapshots,
      deleted,
      version,
      updateTime,
      versionList
    );
  }
}
