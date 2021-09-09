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
 * {@link org.projectnessie.api.ContentsApi#getContents(ContentsKey, String, String)} and {@link
 * org.projectnessie.api.ContentsApi#getMultipleContents(String, String, MultiGetContentsRequest)}
 * return this object for Iceberg tables. It contains the attributes from both {@link
 * IcebergSnapshot} and {@link IcebergTable}, so both: the global and per-branch state.
 */
@Schema(
    type = SchemaType.OBJECT,
    title = "Iceberg table global and branch state",
    description =
        "'ContentsApi.getValue' and 'ContentsApi.getValues' return this object for Iceberg tables. "
            + "It contains the attributes from both 'IcebergSnapshot' and 'IcebergTable', so both: "
            + "the global and per-branch state.")
@Value.Immutable(prehash = true)
@JsonSerialize(as = ImmutableIcebergTableState.class)
@JsonDeserialize(as = ImmutableIcebergTableState.class)
@JsonTypeName("ICEBERG_TABLE_STATE")
public abstract class IcebergTableState extends Contents
    implements CompleteState<IcebergTable, IcebergSnapshot> {

  /** ID of the current Iceberg snapshot. This value is not present, if there is no snapshot. */
  public abstract Long getCurrentSnapshotId();

  /**
   * Location where Iceberg stored its {@code TableMetadata} file. The location depends on the
   * (implementation of) Iceberg's {@code FileIO} configured for the particular Iceberg table.
   */
  @NotNull
  @NotBlank
  public abstract String getMetadataLocation();

  @Override
  public IcebergSnapshot asRefState() {
    return IcebergSnapshot.of(getCurrentSnapshotId(), getId());
  }

  @Override
  public IcebergTable asGlobalState() {
    return IcebergTable.of(getMetadataLocation(), getId());
  }

  public static IcebergTableState of(IcebergSnapshot refState, IcebergTable globalState) {
    if (!globalState.getId().equals(refState.getId())) {
      throw new IllegalArgumentException("Contents-IDs must be equal");
    }
    return ImmutableIcebergTableState.builder()
        .currentSnapshotId(refState.getCurrentSnapshotId())
        .metadataLocation(globalState.getMetadataLocation())
        .id(refState.getId())
        .build();
  }
}
