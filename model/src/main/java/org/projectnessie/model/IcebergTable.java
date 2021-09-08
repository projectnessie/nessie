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
 * its {@link Contents#getId() unique ID}.
 *
 * <p>The Iceberg-table-state consists of the location to the table-metadata and the snapshot-ID.
 *
 * <p>The table-metadata-location is managed globally within Nessie, which means that all versions
 * of the same table share the table-metadata across all named-references (branches and tags). There
 * is only one version, the current version, of Iceberg's table-metadata.
 *
 * <p>Within each named-reference (branch or tag), the (current) Iceberg-snapshot-ID will be
 * different. In other words: changes to an Iceberg table update the snapshot-ID.
 *
 * <p>When adding a new table (aka contents-object identified by a contents-id), use a {@link
 * org.projectnessie.model.Operation.Put} without an expected-value. In all other cases (updating an
 * existing table). always pass the last known version of {@link IcebergTable} as the expected-value
 * within the put-operation.
 */
@Schema(
    type = SchemaType.OBJECT,
    title = "Iceberg table global state",
    description =
        "Represents the global state of an Iceberg table in Nessie. An Iceberg table is globally "
            + "identified via its unique 'Contents.id'.\n"
            + "\n"
            + "A Nessie commit-operation, performed via 'TreeApi.commitMultipleOperations', for Iceberg "
            + "for Iceberg consists of a 'Operation.Put' with an 'IcebergTable' and an "
            + "'IcebergTableGlobal' as the expected-global-state via 'Operation.PutGlobal' using the "
            + "same 'ContentsKey' and 'Contents.id'.\n"
            + "\n"
            + "During a commit-operation, Nessie checks whether the known global state of the "
            + "Iceberg table is compatible (think: equal) to 'Operation.PutGlobal.expectedContents'.")
@Value.Immutable(prehash = true)
@JsonSerialize(as = ImmutableIcebergTable.class)
@JsonDeserialize(as = ImmutableIcebergTable.class)
@JsonTypeName("ICEBERG_TABLE")
public abstract class IcebergTable extends Contents {

  /**
   * Location where Iceberg stored its {@code TableMetadata} file. The location depends on the
   * (implementation of) Iceberg's {@code FileIO} configured for the particular Iceberg table.
   */
  @NotNull
  @NotBlank
  public abstract String getMetadataLocation();

  /** ID of the current Iceberg snapshot. This value is not present, if there is no snapshot. */
  @Nullable // TODO to be removed with PR#1923
  public abstract Long getSnapshotId();

  // TODO to be removed with PR#1923
  public static IcebergTable of(String metadataLocation) {
    return ImmutableIcebergTable.builder().metadataLocation(metadataLocation).build();
  }

  public static IcebergTable of(String metadataLocation, long snapshotId) {
    return ImmutableIcebergTable.builder()
        .metadataLocation(metadataLocation)
        .snapshotId(snapshotId)
        .build();
  }

  public static IcebergTable of(String metadataLocation, long snapshotId, String contentsId) {
    return ImmutableIcebergTable.builder()
        .metadataLocation(metadataLocation)
        .snapshotId(snapshotId)
        .id(contentsId)
        .build();
  }
}
