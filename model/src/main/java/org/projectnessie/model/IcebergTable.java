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
import javax.annotation.Nullable;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.immutables.value.Value;

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
    title = "Iceberg table state",
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
public abstract class IcebergTable extends IcebergContent {

  @Override
  @Nullable
  @Value.Parameter(order = 1)
  public abstract String getId();

  /**
   * Location where Iceberg stored its {@code TableMetadata} file. The location depends on the
   * (implementation of) Iceberg's {@code FileIO} configured for the particular Iceberg table.
   */
  @NotNull
  @NotBlank
  @Value.Parameter(order = 2)
  public abstract String getMetadataLocation();

  /** Corresponds to Iceberg's {@code currentSnapshotId}. */
  @Value.Parameter(order = 3)
  public abstract long getSnapshotId();

  /** Corresponds to Iceberg's {@code currentSchemaId}. */
  @Value.Parameter(order = 4)
  public abstract int getSchemaId();

  /** Corresponds to Iceberg's {@code defaultSpecId}. */
  @Value.Parameter(order = 5)
  public abstract int getSpecId();

  /** Corresponds to Iceberg's {@code defaultSortOrderId}. */
  @Value.Parameter(order = 6)
  public abstract int getSortOrderId();

  @Override
  public Type getType() {
    return Type.ICEBERG_TABLE;
  }

  @Deprecated
  @Nullable
  @JsonInclude(Include.NON_NULL)
  public abstract GenericMetadata getMetadata();

  public static ImmutableIcebergTable.Builder builder() {
    return ImmutableIcebergTable.builder();
  }

  public static IcebergTable of(
      String metadataLocation, long snapshotId, int schemaId, int specId, int sortOrderId) {
    return ImmutableIcebergTable.of(
        null, metadataLocation, snapshotId, schemaId, specId, sortOrderId);
  }

  public static IcebergTable of(
      String metadataLocation,
      long snapshotId,
      int schemaId,
      int specId,
      int sortOrderId,
      String contentId) {
    return ImmutableIcebergTable.of(
        contentId, metadataLocation, snapshotId, schemaId, specId, sortOrderId);
  }
}
