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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

public class Table {
  private final String tableName;
  private final String baseLocation;
  private final String namespace;
  private String metadataLocation = null;
  private String uuid;
  private boolean deleted = false;
  @JsonIgnore
  private String etag;

  @JsonCreator
  public Table(
      @JsonProperty("uuid") String uuid,
      @JsonProperty("tableName") String tableName,
      @JsonProperty("namespace") String namespace,
      @JsonProperty("baseLocation") String baseLocation,
      @JsonProperty("metadataLocation") String metadataLocation,
      @JsonProperty("deleted") boolean deleted) {
    this.uuid = uuid;
    this.tableName = tableName;
    this.namespace = namespace;
    this.metadataLocation = metadataLocation;
    this.deleted = deleted;
    this.baseLocation = baseLocation;
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

  public String getEtag() {
    return etag;
  }

  public void setEtag(String etag) {
    this.etag = etag;
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
      Objects.equal(tableName, table.tableName) &&
      Objects.equal(baseLocation, table.baseLocation) &&
      Objects.equal(namespace, table.namespace) &&
      Objects.equal(metadataLocation, table.metadataLocation) &&
      Objects.equal(uuid, table.uuid) &&
      Objects.equal(etag, table.etag);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(tableName, baseLocation, namespace, metadataLocation, uuid, deleted, etag);
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
      .add("etag", etag)
      .toString();
  }

  public Table rename(String name, String namespace) {
    Table table = new Table(this.uuid, name, namespace, this.getBaseLocation(), this.getMetadataLocation(),
      this.isDeleted());
    table.setEtag(this.getEtag());
    return table;
  }

  public Table newMetadataLocation(String metadataLoc) {
    Table table = new Table(this.uuid, this.getTableName(), this.namespace, this.getBaseLocation(), metadataLoc,
      this.isDeleted());
    table.setEtag(this.getEtag());
    return table;
  }
}
