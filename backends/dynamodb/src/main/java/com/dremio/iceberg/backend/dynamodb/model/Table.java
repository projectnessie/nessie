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

import com.dremio.iceberg.model.Snapshot;
import com.dremio.iceberg.model.TableVersion;
import com.dremio.iceberg.model.VersionedWrapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import software.amazon.awssdk.enhanced.dynamodb.AttributeConverter;
import software.amazon.awssdk.enhanced.dynamodb.AttributeValueType;
import software.amazon.awssdk.enhanced.dynamodb.EnhancedType;
import software.amazon.awssdk.enhanced.dynamodb.extensions.annotations.DynamoDbVersionAttribute;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbConvertedBy;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbIgnore;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Dynamodb table for Table.
 */
@DynamoDbBean
public class Table implements Base {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private String tableName;
  private String baseLocation;
  private String namespace;
  private String metadataLocation;
  private String uuid;
  private boolean deleted;
  private String sourceId;
  private List<Snapshot> snapshots;
  private String schema;
  private Long version;
  private long updateTime;
  private Map<String, TableVersion> versionList;

  public Table() {
  }

  public Table(String tableName,
               String baseLocation,
               String namespace,
               String metadataLocation,
               String uuid,
               boolean deleted,
               String sourceId,
               List<Snapshot> snapshots,
               String schema,
               Long version,
               long updateTime,
               Map<String, TableVersion> versionList) {
    this.tableName = tableName;
    this.baseLocation = baseLocation;
    this.namespace = namespace;
    this.metadataLocation = metadataLocation;
    this.uuid = uuid;
    this.deleted = deleted;
    this.sourceId = sourceId;
    this.snapshots = snapshots;
    this.schema = schema;
    this.version = version;
    this.updateTime = updateTime;
    this.versionList = versionList;
  }

  public String getTableName() {
    return tableName;
  }

  public String getBaseLocation() {
    return baseLocation;
  }

  public String getNamespace() {
    return namespace;
  }

  public String getMetadataLocation() {
    return metadataLocation;
  }

  @DynamoDbPartitionKey
  public String getUuid() {
    return uuid;
  }

  public long getUpdateTime() {
    return updateTime;
  }

  public boolean isDeleted() {
    return deleted;
  }

  public String getSourceId() {
    return sourceId;
  }

  @DynamoDbConvertedBy(SnapshotTypeConverter.class)
  public List<Snapshot> getSnapshots() {
    return snapshots;
  }

  public String getSchema() {
    return schema;
  }

  @DynamoDbVersionAttribute
  public Long getVersion() {
    return version;
  }

  @DynamoDbConvertedBy(VersionTypeConverter.class)
  public Map<String, TableVersion> getVersionList() {
    return versionList;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public void setBaseLocation(String baseLocation) {
    this.baseLocation = baseLocation;
  }

  public void setNamespace(String namespace) {
    this.namespace = namespace;
  }

  public void setMetadataLocation(String metadataLocation) {
    this.metadataLocation = metadataLocation;
  }

  public void setUuid(String uuid) {
    this.uuid = uuid;
  }

  public void setDeleted(boolean deleted) {
    this.deleted = deleted;
  }

  public void setSourceId(String sourceId) {
    this.sourceId = sourceId;
  }

  public void setSnapshots(List<Snapshot> snapshots) {
    this.snapshots = snapshots;
  }

  public void setSchema(String schema) {
    this.schema = schema;
  }

  public void setVersion(Long version) {
    this.version = version;
  }

  public void setUpdateTime(long updateTime) {
    this.updateTime = updateTime;
  }

  public void setVersionList(Map<String, TableVersion> versionList) {
    this.versionList = versionList;
  }

  public static Table fromModelTable(VersionedWrapper<com.dremio.iceberg.model.Table>
                                       oldVersionedTable) {
    com.dremio.iceberg.model.Table oldTable = oldVersionedTable.getObj();
    Table table = new Table(oldTable.getTableName(),
                            oldTable.getBaseLocation(),
                            oldTable.getNamespace(),
                            oldTable.getMetadataLocation(),
                            oldTable.getId(),
                            oldTable.isDeleted(),
                            oldTable.getSourceId(),
                            oldTable.getSnapshots(),
                            oldTable.getSchema(),
                            oldVersionedTable.getVersion().isPresent()
                              ? oldVersionedTable.getVersion().getAsLong() :
                              null,
                            oldTable.getUpdateTime(),
                            oldTable.tableVersions().stream()
                                    .collect(Collectors.toMap(Function.identity(),
                                                              oldTable::getTableVersion)));
    return table;
  }

  @DynamoDbIgnore
  public VersionedWrapper<com.dremio.iceberg.model.Table> toModelTable() {
    return new VersionedWrapper<>(new com.dremio.iceberg.model.Table(
      tableName,
      baseLocation,
      namespace,
      metadataLocation,
      uuid,
      deleted,
      sourceId,
      snapshots,
      schema,
      updateTime,
      versionList
    ),
                                  version);
  }

  /**
   * Convert TableVersion to Dynamodb string.
   */
  public static class VersionTypeConverter implements
                                           AttributeConverter<Map<String, TableVersion>> {

    @Override
    public AttributeValue transformFrom(Map<String, TableVersion> stringTableVersionMap) {
      Map<String, AttributeValue> attributeMap = Maps.newHashMap();
      for (Map.Entry<String, TableVersion> e : stringTableVersionMap.entrySet()) {
        try {
          attributeMap.put(e.getKey(),
                           AttributeValue.builder()
                                         .s(MAPPER.writeValueAsString(e.getValue()))
                                         .build());
        } catch (JsonProcessingException ex) {
          //pass
        }
      }
      return AttributeValue.builder().m(attributeMap).build();
    }

    @Override
    public Map<String, TableVersion> transformTo(AttributeValue attributeValue) {
      Map<String, TableVersion> attributeMap = Maps.newHashMap();
      for (Map.Entry<String, AttributeValue> e : attributeValue.m().entrySet()) {
        try {
          attributeMap.put(e.getKey(), MAPPER.readValue(e.getValue().s(), TableVersion.class));
        } catch (IOException ex) {
          //pass
        }
      }
      return attributeMap;
    }

    @Override
    public EnhancedType<Map<String, TableVersion>> type() {
      return EnhancedType.mapOf(String.class, TableVersion.class);
    }

    @Override
    public AttributeValueType attributeValueType() {
      return AttributeValueType.M;
    }
  }

  /**
   * Convert Snapshot to String for DynamoDb.
   */
  public static class SnapshotTypeConverter implements AttributeConverter<List<Snapshot>> {

    @Override
    public AttributeValue transformFrom(List<Snapshot> snapshots) {
      List<AttributeValue> ss = snapshots.stream().map(s -> {
        try {
          return AttributeValue.builder().s(MAPPER.writeValueAsString(s)).build();
        } catch (JsonProcessingException e) {
          return null;
        }
      }).filter(Objects::nonNull).collect(Collectors.toList());
      if (ss.isEmpty()) {
        ss.add(AttributeValue.builder().s("xx").build());
      }
      return AttributeValue.builder().l(ss).build();
    }

    @Override
    public List<Snapshot> transformTo(AttributeValue attributeValue) {
      return attributeValue.l().stream().map(s -> {
        try {
          return MAPPER.readValue(s.s(), Snapshot.class);
        } catch (IOException e) {
          return null;
        }
      }).filter(Objects::nonNull).collect(Collectors.toList());
    }

    @Override
    public EnhancedType<List<Snapshot>> type() {
      return EnhancedType.listOf(Snapshot.class);
    }

    @Override
    public AttributeValueType attributeValueType() {
      return AttributeValueType.L;
    }
  }
}
