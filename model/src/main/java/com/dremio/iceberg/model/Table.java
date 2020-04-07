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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class Table {
  private final String tableName;
  private final String baseLocation;
  private final String namespace;
  private String metadataLocation = null;
  private String uuid;
  private boolean deleted = false;
  private String sourceId;
  private List<Snapshot> snapshots = Lists.newArrayList();
  private String schema;
  private Long version;

  @JsonCreator
  public Table(
      @JsonProperty("uuid") String uuid,
      @JsonProperty("tableName") String tableName,
      @JsonProperty("namespace") String namespace,
      @JsonProperty("baseLocation") String baseLocation,
      @JsonProperty("metadataLocation") String metadataLocation,
      @JsonProperty("sourceId") String sourceId,
      @JsonProperty("schema") String schema,
      @JsonProperty("snapshots") List<Snapshot> snapshots,
      @JsonProperty("deleted") boolean deleted,
      @JsonProperty("version") Long version) {
    this.uuid = uuid;
    this.tableName = tableName;
    this.namespace = namespace;
    this.metadataLocation = metadataLocation;
    this.sourceId = sourceId;
    this.schema = schema;
    this.snapshots = snapshots == null ? this.snapshots : snapshots;
    this.deleted = deleted;
    this.baseLocation = baseLocation;
    this.version = version;
  }

  public Table(String tableName, String baseLocation) {
    this(tableName, null, baseLocation);
  }

  public Table(String tableName, String namespace, String baseLocation) {
    this.tableName = tableName;
    this.namespace = namespace;
    this.baseLocation = baseLocation;
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

  public void setDeleted(boolean deleted) {
    this.deleted = deleted;
  }

  public String getMetadataLocation() {
    return metadataLocation;
  }

  public void setMetadataLocation(String metadataLocation) {
    this.metadataLocation = metadataLocation;
  }

  public String getUuid() {
    return uuid;
  }

  public void setUuid(String uuid) {
    this.uuid = uuid;
  }

  public String getNamespace() {
    return namespace;
  }

  public String getSourceId() {
    return sourceId;
  }

  public void setSourceId(String sourceId) {
    this.sourceId = sourceId;
  }

  public List<Snapshot> getSnapshots() {
    return snapshots;
  }

  public void setSnapshots(List<Snapshot> snapshots) {
    this.snapshots = snapshots;
  }

  public void addSnapshot(Snapshot snapshot) {
    if (snapshot == null) {
      return;
    }
    snapshots.add(snapshot);
  }

  public String getSchema() {
    return schema;
  }

  public void setSchema(String schema) {
    this.schema = schema;
  }

  public Long getVersion() {
    return version;
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
    Table table = (Table) o;
    return Objects.equal(tableName, table.tableName) &&
      Objects.equal(baseLocation, table.baseLocation) &&
      Objects.equal(namespace, table.namespace) &&
      Objects.equal(metadataLocation, table.metadataLocation) &&
      Objects.equal(uuid, table.uuid) &&
      Objects.equal(version, table.version) &&
      Objects.equal(schema, table.schema);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(tableName, baseLocation, namespace, metadataLocation, uuid, schema, version);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
      .add("tableName", tableName)
      .add("baseLocation", baseLocation)
      .add("namespace", namespace)
      .add("metadataLocation", metadataLocation)
      .add("uuid", uuid)
      .add("deleted", deleted)
      .add("sourceId", sourceId)
      .add("snapshots", snapshots)
      .add("schema", schema)
      .add("version", version)
      .toString();
  }

  public Table rename(String name, String namespace) {
    return new Table(this.uuid, name, namespace, this.getBaseLocation(), this.getMetadataLocation(),
      this.getSourceId(), this.getSchema(), this.getSnapshots(), this.isDeleted(), this.getVersion());
  }

  public Table newMetadataLocation(String metadataLoc) {
    return new Table(this.uuid, this.getTableName(), this.namespace, this.getBaseLocation(), metadataLoc,
      this.getSourceId(), this.getSchema(), this.getSnapshots(), this.isDeleted(), this.getVersion());
  }
}
