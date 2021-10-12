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
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.immutables.value.Value;

/**
 * Represents the state of an Iceberg table in Nessie. An Iceberg table is globally identified via
 * its {@link Contents#getId() unique ID}.
 *
 * <p>The Iceberg-table-state consists of the location to the table-metadata and the state of the ID
 * generators using the serialized version of Iceberg's {@code TableIdGenerators} object.
 *
 * <p>The table-metadata-location is managed on each Nessie reference, which means that all versions
 * of the same table have distinct table-metadata across all named-references (branches and tags).
 *
 * <p>The information needed to generate IDs for an Iceberg table that need to be globally unique,
 * for example the last-column-ID, is managed globally within Nessie.
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
            + "for Iceberg consists of a 'Operation.Put' with an 'IcebergTable' as in the 'contents' "
            + "field and the previous value of 'IcebergTable' in the 'expectedContetns' field.\n"
            + "\n"
            + "During a commit-operation, Nessie checks whether the known global state of the "
            + "Iceberg table is compatible (think: equal) to 'Operation.Put.expectedContents'.")
@Value.Immutable
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

  /** Opaque representation of Iceberg's {@code TableIdGenerators}. */
  public abstract String getIdGenerators();

  public static IcebergTable of(String metadataLocation, String idGenerators) {
    return ImmutableIcebergTable.builder()
        .metadataLocation(metadataLocation)
        .idGenerators(idGenerators)
        .build();
  }

  public static IcebergTable of(String metadataLocation, String idGenerators, String contentsId) {
    return ImmutableIcebergTable.builder()
        .metadataLocation(metadataLocation)
        .idGenerators(idGenerators)
        .id(contentsId)
        .build();
  }
}
