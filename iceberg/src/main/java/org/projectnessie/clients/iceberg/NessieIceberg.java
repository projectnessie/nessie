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
package org.projectnessie.clients.iceberg;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.NessieIcebergBridge;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.util.JsonUtil;
import org.apache.iceberg.view.BaseVersion;
import org.apache.iceberg.view.HistoryEntry;
import org.apache.iceberg.view.Version;
import org.apache.iceberg.view.VersionSummary;
import org.apache.iceberg.view.ViewDefinition;
import org.apache.iceberg.view.ViewVersionMetadata;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;
import org.projectnessie.model.ImmutableIcebergTable;
import org.projectnessie.model.ImmutableIcebergView;
import org.projectnessie.model.iceberg.IcebergMetadataUpdate;
import org.projectnessie.model.iceberg.IcebergPartitionSpec;
import org.projectnessie.model.iceberg.IcebergSchema;
import org.projectnessie.model.iceberg.IcebergSnapshot;
import org.projectnessie.model.iceberg.IcebergSortOrder;
import org.projectnessie.model.iceberg.IcebergViewDefinition;
import org.projectnessie.model.iceberg.IcebergViewVersion;
import org.projectnessie.model.iceberg.IcebergViewVersionSummary;
import org.projectnessie.model.iceberg.ImmutableIcebergPartitionSpec;
import org.projectnessie.model.iceberg.ImmutableIcebergSchema;
import org.projectnessie.model.iceberg.ImmutableIcebergSortOrder;
import org.projectnessie.model.iceberg.ImmutableIcebergViewDefinition;
import org.projectnessie.model.iceberg.ImmutableIcebergViewVersion;
import org.projectnessie.model.iceberg.ImmutableIcebergViewVersionSummary;

/**
 * Converts between Iceberg's {@link TableMetadata} and Nessie's {@link IcebergTable} and related
 * classes.
 */
public final class NessieIceberg extends NessieIcebergBridge {
  private NessieIceberg() {}

  public static IcebergView toNessie(ViewVersionMetadata view) {
    ImmutableIcebergView.Builder builder = IcebergView.builder();
    builder
        .formatVersion(1) // Iceberg doesn't keep the format-version around
        .versionId(view.currentVersionId())
        .properties(view.properties())
        .metadataLocation(view.location())
        .schemaId(view.definition().schema().schemaId())
        // .dialect() // TODO this is currently undefined in Iceberg
        .sqlText(view.definition().sql())
        .versionId(view.currentVersionId())
        .viewDefinition(toNessie(view.definition()));
    view.versions().stream().map(NessieIceberg::toNessie).forEach(builder::addVersions);
    return builder.build();
  }

  private static IcebergViewVersion toNessie(Version version) {
    ImmutableIcebergViewVersion.Builder builder = IcebergViewVersion.builder();
    builder
        .versionId(version.versionId())
        .timestampMillis(version.timestampMillis())
        .summary(toNessie(version.summary()))
        .viewDefinition(toNessie(version.viewDefinition()));
    return builder.build();
  }

  private static IcebergViewDefinition toNessie(ViewDefinition viewDefinition) {
    ImmutableIcebergViewDefinition.Builder builder = IcebergViewDefinition.builder();
    builder
        .schema(toNessie(viewDefinition.schema()))
        .sql(viewDefinition.sql())
        .sessionCatalog(viewDefinition.sessionCatalog())
        .sessionNamespace(viewDefinition.sessionNamespace());
    return builder.build();
  }

  private static IcebergViewVersionSummary toNessie(VersionSummary summary) {
    ImmutableIcebergViewVersionSummary.Builder builder = IcebergViewVersionSummary.builder();
    builder.properties(summary.properties());
    return builder.build();
  }

  public static IcebergTable toNessie(TableMetadata tableMetadata) {
    ImmutableIcebergTable.Builder builder = IcebergTable.builder();
    builder
        .uuid(tableMetadata.uuid())
        .location(tableMetadata.location())
        .metadataLocation(tableMetadata.metadataFileLocation())
        .formatVersion(tableMetadata.formatVersion())
        .schemaId(tableMetadata.currentSchemaId())
        .snapshotId(
            tableMetadata.currentSnapshot() != null
                ? tableMetadata.currentSnapshot().snapshotId()
                : IcebergTable.DEFAULT_SNAPSHOT_ID)
        .properties(tableMetadata.properties())
        .sortOrderId(tableMetadata.defaultSortOrderId())
        .specId(tableMetadata.defaultSpecId())
        .lastAssignedPartitionId(lastAssignedPartitionId(tableMetadata))
        .lastColumnId(tableMetadata.lastColumnId())
        .lastSequenceNumber(tableMetadata.lastSequenceNumber())
        .lastUpdatedMillis(tableMetadata.lastUpdatedMillis());
    tableMetadata.schemas().stream().map(NessieIceberg::toNessie).forEach(builder::addSchemas);
    tableMetadata.snapshots().stream().map(NessieIceberg::toNessie).forEach(builder::addSnapshots);
    tableMetadata.specs().stream().map(NessieIceberg::toNessie).forEach(builder::addSpecs);
    tableMetadata.sortOrders().stream()
        .map(NessieIceberg::toNessie)
        .forEach(builder::addSortOrders);
    tableMetadata.changes().stream().map(NessieIceberg::toNessie).forEach(builder::addChanges);
    return builder.build();
  }

  private static IcebergPartitionSpec toNessie(PartitionSpec partitionSpec) {
    if (partitionSpec == null) {
      return null;
    }
    ImmutableIcebergPartitionSpec.Builder builder =
        IcebergPartitionSpec.builder()
            .specId(partitionSpec.specId())
            .schemaId(partitionSpec.schema().schemaId());
    partitionSpec.fields().stream()
        .map(NessieIceberg::toNessie)
        .filter(Objects::nonNull)
        .forEach(builder::addFields);
    return builder.build();
  }

  private static IcebergPartitionSpec.PartitionField toNessie(PartitionField partitionField) {
    if (partitionField == null) {
      return null;
    }
    return IcebergPartitionSpec.PartitionField.builder()
        .fieldId(partitionField.fieldId())
        .name(partitionField.name())
        .sourceId(partitionField.sourceId())
        .transform(partitionField.transform().toString())
        .build();
  }

  private static IcebergSortOrder toNessie(SortOrder sortOrder) {
    if (sortOrder == null) {
      return null;
    }
    ImmutableIcebergSortOrder.Builder builder =
        IcebergSortOrder.builder()
            .orderId(sortOrder.orderId())
            .schemaId(sortOrder.schema().schemaId());
    sortOrder.fields().stream()
        .map(NessieIceberg::toNessie)
        .filter(Objects::nonNull)
        .forEach(builder::addFields);
    return builder.build();
  }

  private static IcebergSortOrder.SortField toNessie(SortField sortField) {
    if (sortField == null) {
      return null;
    }
    return IcebergSortOrder.SortField.builder()
        .direction(toNessie(sortField.direction()))
        .sourceId(sortField.sourceId())
        .nullOrder(toNessie(sortField.nullOrder()))
        .transform(sortField.transform().toString())
        .build();
  }

  private static IcebergSortOrder.SortField.SortDirection toNessie(SortDirection direction) {
    return IcebergSortOrder.SortField.SortDirection.valueOf(direction.name());
  }

  private static IcebergSortOrder.SortField.NullOrder toNessie(NullOrder nullOrder) {
    return IcebergSortOrder.SortField.NullOrder.valueOf(nullOrder.name());
  }

  private static IcebergSnapshot toNessie(Snapshot snapshot) {
    if (snapshot == null) {
      return null;
    }
    return IcebergSnapshot.builder()
        .schemaId(snapshot.schemaId())
        .snapshotId(snapshot.snapshotId())
        .parentId(snapshot.parentId())
        .manifestListLocation(snapshot.manifestListLocation())
        .operation(snapshot.operation())
        .putAllSummary(snapshot.summary())
        .sequenceNumber(snapshot.sequenceNumber())
        .timestampMillis(snapshot.timestampMillis())
        .build();
  }

  private static IcebergSchema toNessie(Schema schema) {
    if (schema == null) {
      return null;
    }

    JsonNode schemaFieldsArray;
    try {
      schemaFieldsArray = JsonUtil.mapper().readTree(SchemaParser.toJson(schema)).get(FIELDS);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }

    ImmutableIcebergSchema.Builder builder =
        IcebergSchema.builder().schemaId(schema.schemaId()).struct(schemaFieldsArray);
    if (schema.identifierFieldIds() != null) {
      builder.identifierFieldIds(
          schema.identifierFieldIds().stream().mapToInt(Integer::intValue).toArray());
    }
    return builder.build();
  }

  private static IcebergMetadataUpdate toNessie(MetadataUpdate metadataUpdate) {
    if (metadataUpdate == null) {
      return null;
    } else if (metadataUpdate instanceof MetadataUpdate.AssignUUID) {
      MetadataUpdate.AssignUUID assignUUID = (MetadataUpdate.AssignUUID) metadataUpdate;
      return IcebergMetadataUpdate.AssignUUID.builder().uuid(assignUUID.uuid()).build();
    } else if (metadataUpdate instanceof MetadataUpdate.AddPartitionSpec) {
      MetadataUpdate.AddPartitionSpec addPartitionSpec =
          (MetadataUpdate.AddPartitionSpec) metadataUpdate;
      return IcebergMetadataUpdate.AddPartitionSpec.builder()
          .specId(addPartitionSpec.spec().specId())
          .build();
    } else if (metadataUpdate instanceof MetadataUpdate.AddSchema) {
      MetadataUpdate.AddSchema addSchema = (MetadataUpdate.AddSchema) metadataUpdate;
      return IcebergMetadataUpdate.AddSchema.builder()
          .lastColumnId(addSchema.lastColumnId())
          .schemaId(addSchema.schema().schemaId())
          .build();
    } else if (metadataUpdate instanceof MetadataUpdate.AddSnapshot) {
      MetadataUpdate.AddSnapshot addSnapshot = (MetadataUpdate.AddSnapshot) metadataUpdate;
      return IcebergMetadataUpdate.AddSnapshot.builder()
          .snapshotId(addSnapshot.snapshot().snapshotId())
          .build();
    } else if (metadataUpdate instanceof MetadataUpdate.AddSortOrder) {
      MetadataUpdate.AddSortOrder addSortOrder = (MetadataUpdate.AddSortOrder) metadataUpdate;
      return IcebergMetadataUpdate.AddSortOrder.builder()
          .sortOrderId(addSortOrder.spec().orderId())
          .build();
    } else if (metadataUpdate instanceof MetadataUpdate.RemoveProperties) {
      MetadataUpdate.RemoveProperties removeProperties =
          (MetadataUpdate.RemoveProperties) metadataUpdate;
      return IcebergMetadataUpdate.RemoveProperties.builder()
          .removed(removeProperties.removed())
          .build();
    } else if (metadataUpdate instanceof MetadataUpdate.RemoveSnapshot) {
      MetadataUpdate.RemoveSnapshot removeSnapshot = (MetadataUpdate.RemoveSnapshot) metadataUpdate;
      return IcebergMetadataUpdate.RemoveSnapshot.builder()
          .snapshotId(removeSnapshot.snapshotId())
          .build();
    } else if (metadataUpdate instanceof MetadataUpdate.SetCurrentSchema) {
      MetadataUpdate.SetCurrentSchema setCurrentSchema =
          (MetadataUpdate.SetCurrentSchema) metadataUpdate;
      return IcebergMetadataUpdate.SetCurrentSchema.builder()
          .schemaId(setCurrentSchema.schemaId())
          .build();
    } else if (metadataUpdate instanceof MetadataUpdate.SetCurrentSnapshot) {
      MetadataUpdate.SetCurrentSnapshot setCurrentSnapshot =
          (MetadataUpdate.SetCurrentSnapshot) metadataUpdate;
      return IcebergMetadataUpdate.SetCurrentSnapshot.builder()
          .snapshotId(setCurrentSnapshot.snapshotId())
          .build();
    } else if (metadataUpdate instanceof MetadataUpdate.SetDefaultPartitionSpec) {
      MetadataUpdate.SetDefaultPartitionSpec setDefaultPartitionSpec =
          (MetadataUpdate.SetDefaultPartitionSpec) metadataUpdate;
      return IcebergMetadataUpdate.SetDefaultPartitionSpec.builder()
          .specId(setDefaultPartitionSpec.specId())
          .build();
    } else if (metadataUpdate instanceof MetadataUpdate.SetDefaultSortOrder) {
      MetadataUpdate.SetDefaultSortOrder setDefaultSortOrder =
          (MetadataUpdate.SetDefaultSortOrder) metadataUpdate;
      return IcebergMetadataUpdate.SetDefaultSortOrder.builder()
          .sortOrderId(setDefaultSortOrder.sortOrderId())
          .build();
    } else if (metadataUpdate instanceof MetadataUpdate.SetLocation) {
      MetadataUpdate.SetLocation setLocation = (MetadataUpdate.SetLocation) metadataUpdate;
      return IcebergMetadataUpdate.SetLocation.builder().location(setLocation.location()).build();
    } else if (metadataUpdate instanceof MetadataUpdate.SetProperties) {
      MetadataUpdate.SetProperties setProperties = (MetadataUpdate.SetProperties) metadataUpdate;
      return IcebergMetadataUpdate.SetProperties.builder().updated(setProperties.updated()).build();
    } else if (metadataUpdate instanceof MetadataUpdate.UpgradeFormatVersion) {
      MetadataUpdate.UpgradeFormatVersion upgradeFormatVersion =
          (MetadataUpdate.UpgradeFormatVersion) metadataUpdate;
      return IcebergMetadataUpdate.UpgradeFormatVersion.builder()
          .formatVersion(upgradeFormatVersion.formatVersion())
          .build();
    } else {
      throw new IllegalArgumentException(
          "Unsupported MetadataUpdate type " + metadataUpdate.getClass().getName());
    }
  }

  public static TableMetadata toIceberg(FileIO io, IcebergTable nessieTableMetadata) {
    return toIcebergInternal(io, nessieTableMetadata);
  }

  public static ViewVersionMetadata toIceberg(IcebergView nessieViewMetadata) {
    String location = nessieViewMetadata.getMetadataLocation();
    ViewDefinition definition = toIceberg(nessieViewMetadata.getViewDefinition());
    Map<String, String> properties = nessieViewMetadata.getProperties();
    List<Version> versions =
        nessieViewMetadata.getVersions().stream()
            .map(NessieIceberg::toIceberg)
            .collect(Collectors.toList());
    int currentVersionId = nessieViewMetadata.getVersionId();
    long currentVersionTimestamp =
        versions.stream()
            .filter(v -> v.versionId() == currentVersionId)
            .mapToLong(Version::timestampMillis)
            .findFirst()
            .orElse(0L);
    List<HistoryEntry> versionLog =
        Collections.singletonList(HistoryEntry.of(currentVersionTimestamp, currentVersionId));
    return new ViewVersionMetadata(
        location, definition, properties, currentVersionId, versions, versionLog);
  }

  private static Version toIceberg(IcebergViewVersion viewVersion) {
    if (viewVersion == null) {
      return null;
    }
    return new BaseVersion(
        viewVersion.getVersionId(),
        null,
        viewVersion.getTimestampMillis(),
        toIceberg(viewVersion.getSummary()),
        toIceberg(viewVersion.getViewDefinition()));
  }

  private static VersionSummary toIceberg(IcebergViewVersionSummary viewVersionSummary) {
    if (viewVersionSummary == null) {
      return null;
    }
    return new VersionSummary(viewVersionSummary.getProperties());
  }

  private static ViewDefinition toIceberg(IcebergViewDefinition viewDefinition) {
    if (viewDefinition == null) {
      return null;
    }
    return ViewDefinition.of(
        viewDefinition.getSql(),
        toIceberg(viewDefinition.getSchema()),
        viewDefinition.getSessionCatalog(),
        viewDefinition.getSessionNamespace());
  }
}
