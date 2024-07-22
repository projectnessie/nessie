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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonView;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Map;
import javax.annotation.Nullable;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.immutables.value.Value;
import org.projectnessie.model.ser.Views;

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

  /**
   * Location where Iceberg stored its {@code TableMetadata} file. The location depends on the
   * (implementation of) Iceberg's {@code FileIO} configured for the particular Iceberg table.
   */
  @NotNull
  @jakarta.validation.constraints.NotNull
  @NotBlank
  @jakarta.validation.constraints.NotBlank
  @Override
  public abstract String getMetadataLocation();

  /** Corresponds to Iceberg's {@code currentSnapshotId}. */
  public abstract long getSnapshotId();

  @Value.NonAttribute
  @JsonIgnore
  @Override
  public long getVersionId() {
    return getSnapshotId();
  }

  /** Corresponds to Iceberg's {@code currentSchemaId}. */
  public abstract int getSchemaId();

  /** Corresponds to Iceberg's {@code defaultSpecId}. */
  public abstract int getSpecId();

  /** Corresponds to Iceberg's {@code defaultSortOrderId}. */
  public abstract int getSortOrderId();

  @Override
  public Type getType() {
    return Type.ICEBERG_TABLE;
  }

  @Deprecated
  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(Include.NON_NULL)
  @JsonView(Views.V1.class)
  // Left here in case an old Nessie client sends this piece of information.
  // To be removed when API v1 gets removed.
  public abstract Map<String, Object> getMetadata();

  @Override
  public abstract IcebergTable withId(String id);

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
