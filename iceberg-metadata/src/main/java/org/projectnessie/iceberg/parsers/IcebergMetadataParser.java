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
package org.projectnessie.iceberg.parsers;

import java.util.stream.Collectors;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.io.FileIO;
import org.projectnessie.model.IcebergTableMetadata;
import org.projectnessie.model.ImmutableIcebergTableMetadata;
import org.projectnessie.model.ImmutablePartitionField;
import org.projectnessie.model.ImmutableSnapshot;
import org.projectnessie.model.ImmutableSortField;
import org.projectnessie.model.ImmutableSortOrder;
import org.projectnessie.model.PartitionField;
import org.projectnessie.model.Snapshot;
import org.projectnessie.model.SortField;
import org.projectnessie.model.SortField.NullOrder;
import org.projectnessie.model.SortField.SortDirection;
import org.projectnessie.model.SortOrder;

public final class IcebergMetadataParser {

  private IcebergMetadataParser() {}

  public static IcebergTableMetadata icebergToNessie(TableMetadata metadata) {
    return ImmutableIcebergTableMetadata.builder()
        .metadataLocation(metadata.metadataFileLocation())
        .formatVersion(metadata.formatVersion())
        .uuid(metadata.uuid())
        .location(metadata.location())
        .lastUpdatedMillis(metadata.lastUpdatedMillis())
        .lastColumnId(metadata.lastColumnId())
        .spec(org.apache.iceberg.IcebergMetadataParser.icebergToNessie(metadata.spec()))
        .putAllProperties(metadata.properties())
        .lastAssignedPartitionId(metadata.defaultSpecId())
        .snapshotId(metadata.currentSnapshot().snapshotId())
        .schemaId(metadata.currentSchemaId())
        .specId(metadata.defaultSpecId())
        .sortOrderId(metadata.defaultSortOrderId())
        .schema(SchemaParser.toJson(metadata.schema()))
        .sortOrder(icebergToNessie(metadata.sortOrder()))
        .snapshot(icebergToNessie(metadata.currentSnapshot()))
        .lastSequenceNumber(metadata.lastSequenceNumber())
        .build();
  }

  private static Snapshot icebergToNessie(org.apache.iceberg.Snapshot currentSnapshot) {
    if (currentSnapshot == null) {
      return null;
    }
    return ImmutableSnapshot.builder()
        .snapshotId(currentSnapshot.snapshotId())
        .schemaId(currentSnapshot.schemaId())
        .manifestList(currentSnapshot.manifestListLocation())
        .operation(currentSnapshot.operation())
        .parentSnapshotId(currentSnapshot.parentId())
        .putAllSummary(currentSnapshot.summary())
        .sequenceNumber(currentSnapshot.sequenceNumber())
        .timestampMillis(currentSnapshot.timestampMillis())
        .build();
  }

  private static SortOrder icebergToNessie(org.apache.iceberg.SortOrder sortOrder) {
    return ImmutableSortOrder.builder()
        .fields(
            sortOrder.fields().stream()
                .map(IcebergMetadataParser::icebergToNessie)
                .collect(Collectors.toList()))
        .schema(SchemaParser.toJson(sortOrder.schema()))
        .orderId(sortOrder.orderId())
        .build();
  }

  private static SortField icebergToNessie(org.apache.iceberg.SortField x) {
    return ImmutableSortField.builder()
        .sourceId(x.sourceId())
        .direction(SortDirection.values()[x.direction().ordinal()])
        .nullOrder(NullOrder.values()[x.nullOrder().ordinal()])
        .transform(x.transform().toString())
        .build();
  }

  public static PartitionField icebergToNessie(org.apache.iceberg.PartitionField partitionField) {
    return ImmutablePartitionField.builder()
        .sourceId(partitionField.sourceId())
        .fieldId(partitionField.fieldId())
        .name(partitionField.name())
        .transform(partitionField.transform().toString())
        .build();
  }

  public static TableMetadata nessieToIceberg(FileIO fileIO, IcebergTableMetadata metadata) {
    return org.apache.iceberg.IcebergMetadataParser.nessieToIceberg(fileIO, metadata);
  }
}
