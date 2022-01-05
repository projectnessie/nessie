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
package org.projectnessie.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.PositiveOrZero;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.immutables.value.Value;
import org.projectnessie.model.iceberg.IcebergMetadataUpdate;
import org.projectnessie.model.iceberg.IcebergPartitionSpec;
import org.projectnessie.model.iceberg.IcebergSchema;
import org.projectnessie.model.iceberg.IcebergSnapshot;
import org.projectnessie.model.iceberg.IcebergSortOrder;

/**
 * Represents the state of an Iceberg table in Nessie. An Iceberg table is globally identified via
 * its {@link Content#getId() unique ID}.
 *
 * <p>The Iceberg-table-state consists of the location to the table-metadata and the state of
 * relevant IDs using a serialized version of those.
 *
 * <p>When adding a new table (aka content-object identified by a content-id), use a {@link
 * org.projectnessie.model.Operation.Put} without an expected-value. In all other cases (updating an
 * existing table). always pass the last known version of {@link IcebergTable} as the expected-value
 * within the put-operation.
 */
@Schema(
    type = SchemaType.OBJECT,
    title = "Iceberg table global state",
    description =
        "Represents the state of an Iceberg table in Nessie. An Iceberg table is globally "
            + "identified via its unique 'Content.id'.\n"
            + "\n"
            + "A Nessie commit-operation, performed via 'TreeApi.commitMultipleOperations',"
            + "for Iceberg consists of a 'Operation.Put' with an 'IcebergTable' as in the 'content' "
            + "field and the previous value of 'IcebergTable' in the 'expectedContent' field.")
@Value.Immutable
@JsonSerialize(as = ImmutableIcebergTable.class)
@JsonDeserialize(as = ImmutableIcebergTable.class)
@JsonTypeName("ICEBERG_TABLE")
public abstract class IcebergTable extends Content {

  public static final int MIN_SUPPORTED_FORMAT_VERSION = 2;
  public static final int MAX_SUPPORTED_FORMAT_VERSION = 2;
  public static final long INITIAL_SEQUENCE_NUMBER = 0;
  public static final long INVALID_SEQUENCE_NUMBER = -1;
  public static final int INITIAL_SPEC_ID = 0;
  public static final int INITIAL_SORT_ORDER_ID = 1;
  public static final long INITIAL_LAST_UPDATED_MILLIS = 0L;
  //
  public static final int INITIAL_LAST_COLUMN_ID = 0;
  public static final int INITIAL_PARTITION_ID = 0;
  public static final int INITIAL_SCHEMA_ID = 0;
  public static final long DEFAULT_SNAPSHOT_ID = -1L;

  /**
   * Location where Iceberg stored its {@code TableMetadata} file. The location depends on the
   * (implementation of) Iceberg's {@code FileIO} configured for the particular Iceberg table.
   */
  @NotNull
  @NotBlank
  public abstract String getMetadataLocation();

  /** Corresponds to Iceberg's {@code currentSnapshotId}. */
  @Min(DEFAULT_SNAPSHOT_ID)
  public abstract long getSnapshotId();

  /** Corresponds to Iceberg's {@code currentSchemaId}. */
  @Min(INITIAL_SCHEMA_ID)
  public abstract int getSchemaId();

  /** Corresponds to Iceberg's {@code defaultSpecId}. */
  @Min(INITIAL_SPEC_ID)
  public abstract int getSpecId();

  /** Corresponds to Iceberg's {@code defaultSortOrderId}. */
  @PositiveOrZero
  public abstract int getSortOrderId();

  @Override
  public Type getType() {
    return Type.ICEBERG_TABLE;
  }

  @Nullable // for backwards compatibility
  @JsonInclude(Include.NON_EMPTY)
  public abstract String getLocation();

  @Nullable // for backwards compatibility
  @Min(MIN_SUPPORTED_FORMAT_VERSION)
  @Max(MAX_SUPPORTED_FORMAT_VERSION)
  @JsonInclude(Include.NON_NULL)
  public abstract Integer getFormatVersion();

  @Nullable // for backwards compatibility
  @JsonInclude(Include.NON_NULL)
  public abstract String getUuid();

  @Nullable // for backwards compatibility
  @Min(INVALID_SEQUENCE_NUMBER)
  @JsonInclude(Include.NON_NULL)
  public abstract Long getLastSequenceNumber();

  @Nullable // for backwards compatibility
  @JsonInclude(Include.NON_NULL)
  public abstract Long getLastUpdatedMillis();

  @Nullable // for backwards compatibility
  @Min(INITIAL_LAST_COLUMN_ID)
  @JsonInclude(Include.NON_NULL)
  public abstract Integer getLastColumnId();

  @Nullable // for backwards compatibility
  @Min(INITIAL_PARTITION_ID)
  @JsonInclude(Include.NON_NULL)
  public abstract Integer getLastAssignedPartitionId();

  @JsonInclude(Include.NON_EMPTY)
  public abstract List<IcebergSchema> getSchemas();

  // TODO We do need this list here - manifests record the partition spec
  @JsonInclude(Include.NON_EMPTY)
  public abstract List<IcebergPartitionSpec> getSpecs();

  // TODO We do need this list here - manifests record the partition spec
  @JsonInclude(Include.NON_EMPTY)
  public abstract List<IcebergSortOrder> getSortOrders();

  @JsonInclude(Include.NON_EMPTY)
  public abstract List<IcebergSnapshot> getSnapshots();

  @JsonInclude(Include.NON_EMPTY)
  public abstract List<IcebergMetadataUpdate> getChanges();

  @JsonInclude(Include.NON_EMPTY)
  public abstract Map<String, String> getProperties();

  public static ImmutableIcebergTable.Builder builder() {
    return ImmutableIcebergTable.builder();
  }

  public static IcebergTable of(
      String metadataLocation, long snapshotId, int schemaId, int specId, int sortOrderId) {
    return builder()
        .metadataLocation(metadataLocation)
        .snapshotId(snapshotId)
        .schemaId(schemaId)
        .specId(specId)
        .sortOrderId(sortOrderId)
        .build();
  }

  public static IcebergTable of(
      String metadataLocation,
      long snapshotId,
      int schemaId,
      int specId,
      int sortOrderId,
      String contentId) {
    return builder()
        .metadataLocation(metadataLocation)
        .snapshotId(snapshotId)
        .schemaId(schemaId)
        .specId(specId)
        .sortOrderId(sortOrderId)
        .id(contentId)
        .build();
  }
}
