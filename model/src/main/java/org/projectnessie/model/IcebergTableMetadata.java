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
import java.util.Map;
import javax.annotation.Nullable;
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
    title = "Iceberg table global state",
    description =
        "Represents the global state of an Iceberg table in Nessie. An Iceberg table is globally "
            + "identified via its unique 'Content.id'.\n"
            + "\n"
            + "A Nessie commit-operation, performed via 'TreeApi.commitMultipleOperations', for Iceberg "
            + "for Iceberg consists of a 'Operation.Put' with an 'IcebergTable' as in the 'content' "
            + "field and the previous value of 'IcebergTable' in the 'expectedContent' field.\n"
            + "\n"
            + "During a commit-operation, Nessie checks whether the known global state of the "
            + "Iceberg table is compatible (think: equal) to 'Operation.Put.expectedContent'.")
@Value.Immutable
@JsonSerialize(as = ImmutableIcebergTableMetadata.class)
@JsonDeserialize(as = ImmutableIcebergTableMetadata.class)
@JsonTypeName("ICEBERG_METADATA")
public abstract class IcebergTableMetadata extends Content {

  /**
   * Location where Iceberg stored its {@code TableMetadata} file. The location depends on the
   * (implementation of) Iceberg's {@code FileIO} configured for the particular Iceberg table.
   */
  @Nullable
  public abstract String getMetadataLocation();

  public abstract int getFormatVersion();

  public abstract String getUuid();

  public abstract String getLocation();

  public abstract long getLastSequenceNumber();

  public abstract long getLastUpdatedMillis();

  public abstract int getLastColumnId();

  public abstract String getSchema();

  public abstract PartitionSpec getSpec();

  public abstract SortOrder getSortOrder();

  public abstract Snapshot getSnapshot();

  public abstract Map<String, String> getProperties();

  public abstract int getLastAssignedPartitionId();

  public abstract long getSnapshotId();

  public abstract int getSchemaId();

  public abstract int getSpecId();

  public abstract int getSortOrderId();

  @Override
  public Type getType() {
    return Type.ICEBERG_METADATA;
  }
}
