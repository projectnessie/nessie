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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBIgnore;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBVersionAttribute;
import com.dremio.iceberg.model.Snapshot;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;

@DynamoDBTable(tableName="IcebergAlleyTables")
public class Table implements Base{
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private String tableName;
  private String baseLocation;
  private String namespace;
  private String metadataLocation;
  private String uuid;
  private boolean deleted;
  private String sourceId;
  private List<String> snapshots;
  private String schema;
  private Long version;

  @DynamoDBAttribute(attributeName = "tableName")
  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  @DynamoDBAttribute(attributeName = "baseLocation")
  public String getBaseLocation() {
    return baseLocation;
  }

  public void setBaseLocation(String baseLocation) {
    this.baseLocation = baseLocation;
  }

  @DynamoDBAttribute(attributeName = "namespace")
  public String getNamespace() {
    return namespace;
  }

  public void setNamespace(String namespace) {
    this.namespace = namespace;
  }

  @DynamoDBAttribute(attributeName = "metadataLocation")
  public String getMetadataLocation() {
    return metadataLocation;
  }

  public void setMetadataLocation(String metadataLocation) {
    this.metadataLocation = metadataLocation;
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

  @DynamoDBAttribute(attributeName = "sourceId")
  public String getSourceId() {
    return sourceId;
  }

  public void setSourceId(String sourceId) {
    this.sourceId = sourceId;
  }

  @DynamoDBAttribute(attributeName = "snapshots")
  public List<String> getSnapshots() {
    return snapshots;
  }

  public void setSnapshots(List<String> snapshots) {
    this.snapshots = snapshots;
  }

  @DynamoDBAttribute(attributeName = "schema")
  public String getSchema() {
    return schema;
  }

  public void setSchema(String schema) {
    this.schema = schema;
  }

  @DynamoDBVersionAttribute()
  public Long getVersion() {
    return version;
  }

  public void setVersion(Long version) {
    this.version = version;
  }

  public static Table fromModelTable(com.dremio.iceberg.model.Table oldTable) {
    Table table = new Table();
    table.setBaseLocation(oldTable.getBaseLocation());
    table.setDeleted(oldTable.isDeleted());
    table.setMetadataLocation(oldTable.getMetadataLocation());
    table.setNamespace(oldTable.getNamespace());
    table.setSchema(oldTable.getSchema());
    table.setSnapshots(oldTable.getSnapshots().stream().map(value -> {
      try {
        return MAPPER.writeValueAsString(value);
      } catch (JsonProcessingException e) {
        return null;
      }
    }).filter(Objects::nonNull).collect(Collectors.toList()));
    table.setSourceId(oldTable.getSourceId());
    table.setTableName(oldTable.getTableName());
    table.setUuid(oldTable.getUuid());
    table.setVersion(oldTable.getVersion());
    return table;
  }

  @DynamoDBIgnore
  public com.dremio.iceberg.model.Table toModelTable() {
    return new com.dremio.iceberg.model.Table(
      uuid,
      tableName,
      namespace,
      baseLocation,
      metadataLocation,
      sourceId,
      schema,
      snapshots.stream().map(s -> {
        try {
          return MAPPER.readValue(s, Snapshot.class);
        } catch (IOException e) {
          return null;
        }
      }).filter(Objects::nonNull).collect(Collectors.toList()),
      deleted,
     version
    );
  }
}
