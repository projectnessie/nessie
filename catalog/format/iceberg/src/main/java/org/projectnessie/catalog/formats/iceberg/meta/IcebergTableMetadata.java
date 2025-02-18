/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.catalog.formats.iceberg.meta;

import static org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionSpec.MIN_PARTITION_ID;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonView;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.projectnessie.catalog.formats.iceberg.IcebergSpec;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@JsonSerialize(as = ImmutableIcebergTableMetadata.class)
@JsonDeserialize(as = ImmutableIcebergTableMetadata.class)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonIgnoreProperties(ignoreUnknown = true)
public interface IcebergTableMetadata {

  long NO_SNAPSHOT_ID = -1;
  long INITIAL_SEQUENCE_NUMBER = 0;
  int INITIAL_PARTITION_ID = MIN_PARTITION_ID - 1;

  String STAGED_PROPERTY = "nessie.staged";
  String GC_ENABLED = "gc.enabled";

  int formatVersion();

  @Nullable
  @jakarta.annotation.Nullable
  String tableUuid();

  /**
   * The Iceberg table base location, usually something like {@code
   * s3://bucket1/warehouse/ns/table_<uuid>}.
   */
  String location();

  @JsonView(IcebergSpec.IcebergSpecV2.class)
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  @jakarta.annotation.Nullable
  Long lastSequenceNumber();

  long lastUpdatedMs();

  int lastColumnId();

  @JsonView(IcebergSpec.IcebergSpecV1.class)
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  @jakarta.annotation.Nullable
  IcebergSchema schema();

  List<IcebergSchema> schemas();

  // Optional in V1
  @Nullable
  @jakarta.annotation.Nullable
  Integer currentSchemaId();

  @JsonView(IcebergSpec.IcebergSpecV1.class)
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  List<IcebergPartitionField> partitionSpec();

  List<IcebergPartitionSpec> partitionSpecs();

  // Optional in V1
  @Nullable
  @jakarta.annotation.Nullable
  Integer defaultSpecId();

  // Optional in V1
  @Nullable
  @jakarta.annotation.Nullable
  Integer lastPartitionId();

  // Optional in V1
  @Nullable
  @jakarta.annotation.Nullable
  Integer defaultSortOrderId();

  List<IcebergSortOrder> sortOrders();

  Map<String, String> properties();

  @Nullable
  @jakarta.annotation.Nullable
  Long currentSnapshotId();

  @JsonIgnore
  default long currentSnapshotIdAsLong() {
    var snapshotId = currentSnapshotId();
    return snapshotId != null ? snapshotId : NO_SNAPSHOT_ID;
  }

  @JsonView(IcebergSpec.IcebergSpecV2.class)
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  @jakarta.annotation.Nullable
  Map<String, IcebergSnapshotRef> refs();

  List<IcebergSnapshot> snapshots();

  List<IcebergStatisticsFile> statistics();

  List<IcebergPartitionStatisticsFile> partitionStatistics();

  List<IcebergSnapshotLogEntry> snapshotLog();

  List<IcebergHistoryEntry> metadataLog();

  default Optional<IcebergSnapshot> currentSnapshot() {
    return snapshotById(currentSnapshotIdAsLong());
  }

  default Optional<IcebergSnapshot> snapshotById(long id) {
    if (id == NO_SNAPSHOT_ID) {
      return Optional.empty();
    }
    for (IcebergSnapshot snapshot : snapshots()) {
      if (id == snapshot.snapshotId()) {
        return Optional.of(snapshot);
      }
    }
    return Optional.empty();
  }

  static Builder builder() {
    return ImmutableIcebergTableMetadata.builder();
  }

  @SuppressWarnings("unused")
  interface Builder {
    @CanIgnoreReturnValue
    Builder from(IcebergTableMetadata icebergTableMetadata);

    @CanIgnoreReturnValue
    Builder clear();

    @CanIgnoreReturnValue
    Builder formatVersion(int formatVersion);

    @CanIgnoreReturnValue
    Builder tableUuid(String tableUuid);

    @CanIgnoreReturnValue
    Builder location(String location);

    @CanIgnoreReturnValue
    Builder lastSequenceNumber(@Nullable Long lastSequenceNumber);

    @CanIgnoreReturnValue
    Builder lastUpdatedMs(long lastUpdatedMs);

    @CanIgnoreReturnValue
    Builder lastColumnId(int lastColumnId);

    @CanIgnoreReturnValue
    Builder schema(@Nullable IcebergSchema schema);

    @CanIgnoreReturnValue
    Builder addSchema(IcebergSchema element);

    @CanIgnoreReturnValue
    Builder addSchemas(IcebergSchema... elements);

    @CanIgnoreReturnValue
    Builder schemas(Iterable<? extends IcebergSchema> elements);

    @CanIgnoreReturnValue
    Builder addAllSchemas(Iterable<? extends IcebergSchema> elements);

    @CanIgnoreReturnValue
    Builder currentSchemaId(Integer currentSchemaId);

    //

    @CanIgnoreReturnValue
    Builder addPartitionSpec(IcebergPartitionField element);

    @CanIgnoreReturnValue
    Builder addPartitionSpec(IcebergPartitionField... elements);

    @CanIgnoreReturnValue
    Builder partitionSpec(Iterable<? extends IcebergPartitionField> elements);

    @CanIgnoreReturnValue
    Builder addAllPartitionSpec(Iterable<? extends IcebergPartitionField> elements);

    //

    @CanIgnoreReturnValue
    Builder addPartitionSpec(IcebergPartitionSpec element);

    @CanIgnoreReturnValue
    Builder addPartitionSpecs(IcebergPartitionSpec... elements);

    @CanIgnoreReturnValue
    Builder partitionSpecs(Iterable<? extends IcebergPartitionSpec> elements);

    @CanIgnoreReturnValue
    Builder addAllPartitionSpecs(Iterable<? extends IcebergPartitionSpec> elements);

    @CanIgnoreReturnValue
    Builder defaultSpecId(Integer defaultSpecId);

    @CanIgnoreReturnValue
    Builder lastPartitionId(Integer lastPartitionId);

    @CanIgnoreReturnValue
    Builder defaultSortOrderId(Integer defaultSortOrderId);

    @CanIgnoreReturnValue
    Builder addSortOrder(IcebergSortOrder element);

    @CanIgnoreReturnValue
    Builder addSortOrders(IcebergSortOrder... elements);

    @CanIgnoreReturnValue
    Builder sortOrders(Iterable<? extends IcebergSortOrder> elements);

    @CanIgnoreReturnValue
    Builder addAllSortOrders(Iterable<? extends IcebergSortOrder> elements);

    @CanIgnoreReturnValue
    Builder putProperty(String key, String value);

    @CanIgnoreReturnValue
    Builder putProperty(Map.Entry<String, ? extends String> entry);

    @CanIgnoreReturnValue
    Builder properties(Map<String, ? extends String> entries);

    @CanIgnoreReturnValue
    Builder putAllProperties(Map<String, ? extends String> entries);

    @CanIgnoreReturnValue
    Builder currentSnapshotId(Long currentSnapshotId);

    @CanIgnoreReturnValue
    Builder putRef(String key, IcebergSnapshotRef value);

    @CanIgnoreReturnValue
    Builder putRef(Map.Entry<String, ? extends IcebergSnapshotRef> entry);

    @CanIgnoreReturnValue
    Builder refs(@Nullable Map<String, ? extends IcebergSnapshotRef> entries);

    @CanIgnoreReturnValue
    Builder putAllRefs(Map<String, ? extends IcebergSnapshotRef> entries);

    @CanIgnoreReturnValue
    Builder addSnapshot(IcebergSnapshot element);

    @CanIgnoreReturnValue
    Builder addSnapshots(IcebergSnapshot... elements);

    @CanIgnoreReturnValue
    Builder snapshots(Iterable<? extends IcebergSnapshot> elements);

    @CanIgnoreReturnValue
    Builder addAllSnapshots(Iterable<? extends IcebergSnapshot> elements);

    @CanIgnoreReturnValue
    Builder addStatistic(IcebergStatisticsFile element);

    @CanIgnoreReturnValue
    Builder addStatistics(IcebergStatisticsFile... elements);

    @CanIgnoreReturnValue
    Builder statistics(Iterable<? extends IcebergStatisticsFile> elements);

    @CanIgnoreReturnValue
    Builder addAllStatistics(Iterable<? extends IcebergStatisticsFile> elements);

    @CanIgnoreReturnValue
    Builder addPartitionStatistic(IcebergPartitionStatisticsFile element);

    @CanIgnoreReturnValue
    Builder addPartitionStatistics(IcebergPartitionStatisticsFile... elements);

    @CanIgnoreReturnValue
    Builder partitionStatistics(Iterable<? extends IcebergPartitionStatisticsFile> elements);

    @CanIgnoreReturnValue
    Builder addAllPartitionStatistics(Iterable<? extends IcebergPartitionStatisticsFile> elements);

    @CanIgnoreReturnValue
    Builder addSnapshotLog(IcebergSnapshotLogEntry element);

    @CanIgnoreReturnValue
    Builder addSnapshotLog(IcebergSnapshotLogEntry... elements);

    @CanIgnoreReturnValue
    Builder snapshotLog(Iterable<? extends IcebergSnapshotLogEntry> elements);

    @CanIgnoreReturnValue
    Builder addAllSnapshotLog(Iterable<? extends IcebergSnapshotLogEntry> elements);

    @CanIgnoreReturnValue
    Builder addMetadataLog(IcebergHistoryEntry element);

    @CanIgnoreReturnValue
    Builder addMetadataLog(IcebergHistoryEntry... elements);

    @CanIgnoreReturnValue
    Builder metadataLog(Iterable<? extends IcebergHistoryEntry> elements);

    @CanIgnoreReturnValue
    Builder addAllMetadataLog(Iterable<? extends IcebergHistoryEntry> elements);

    IcebergTableMetadata build();
  }
}
