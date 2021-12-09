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
package org.apache.iceberg;

import java.util.stream.Collectors;
import org.apache.iceberg.PartitionSpec.Builder;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.projectnessie.model.IcebergTableMetadata;
import org.projectnessie.model.ImmutablePartitionSpec;
import org.projectnessie.model.Snapshot;
import org.projectnessie.model.SortOrder;

public final class IcebergMetadataParser {

  private IcebergMetadataParser() {}

  public static org.projectnessie.model.PartitionSpec icebergToNessie(
      org.apache.iceberg.PartitionSpec spec) {
    return ImmutablePartitionSpec.builder()
        .fields(
            spec.fields().stream()
                .map(org.projectnessie.iceberg.parsers.IcebergMetadataParser::icebergToNessie)
                .collect(Collectors.toList()))
        .specId(spec.specId())
        .lastAssignedFieldId(spec.lastAssignedFieldId())
        .schema(SchemaParser.toJson(spec.schema()))
        .build();
  }

  public static TableMetadata nessieToIceberg(FileIO fileIo, IcebergTableMetadata metadata) {
    return new TableMetadata(
        null,
        metadata.getFormatVersion(),
        metadata.getUuid(),
        metadata.getLocation(),
        metadata.getLastSequenceNumber(),
        metadata.getLastUpdatedMillis(),
        metadata.getLastColumnId(),
        metadata.getSchemaId(),
        ImmutableList.of(SchemaParser.fromJson(metadata.getSchema())),
        metadata.getSpecId(),
        ImmutableList.of(nessieToIceberg(metadata.getSpec())),
        metadata.getLastAssignedPartitionId(),
        metadata.getSortOrderId(),
        ImmutableList.of(nessieToIceberg(metadata.getSortOrder())),
        metadata.getProperties(),
        metadata.getSnapshotId(),
        ImmutableList.of(nessieToIceberg(fileIo, metadata.getSnapshot())),
        ImmutableList.of(),
        ImmutableList.of());
  }

  private static org.apache.iceberg.Snapshot nessieToIceberg(FileIO fileIO, Snapshot snapshot) {
    return new BaseSnapshot(
        fileIO,
        snapshot.getSequenceNumber(),
        snapshot.getSnapshotId(),
        snapshot.getParentSnapshotId(),
        snapshot.getTimestampMillis(),
        snapshot.getOperation(),
        snapshot.getSummary(),
        snapshot.getSchemaId(),
        snapshot.getManifestList());
  }

  private static org.apache.iceberg.SortOrder nessieToIceberg(SortOrder sortOrder) {
    org.apache.iceberg.SortOrder.Builder builder =
        org.apache.iceberg.SortOrder.builderFor(SchemaParser.fromJson(sortOrder.getSchema()));
    sortOrder
        .getFields()
        .forEach(
            x ->
                builder.addSortField(
                    x.getTransform(),
                    x.getSourceId(),
                    SortDirection.values()[x.getDirection().ordinal()],
                    NullOrder.values()[x.getNullOrder().ordinal()]));
    return builder.withOrderId(sortOrder.getOrderId()).build();
  }

  private static PartitionSpec nessieToIceberg(org.projectnessie.model.PartitionSpec spec) {
    Builder builder = PartitionSpec.builderFor(SchemaParser.fromJson(spec.getSchema()));
    spec.getFields()
        .forEach(x -> builder.add(x.getSourceId(), x.getFieldId(), x.getName(), x.getTransform()));
    Preconditions.checkArgument(spec.getLastAssignedFieldId() == spec.getLastAssignedFieldId());
    return builder.withSpecId(spec.getSpecId()).build();
  }
}
