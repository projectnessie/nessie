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
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.immutables.value.Value;

/**
 * Represents the state of an Iceberg table in a Nessie branch. An Iceberg table is globally
 * identified via its fully qualified name via {@link ContentsKey} plus a unique ID, the latter is
 * represented via {@link Contents#getId()}.
 *
 * <p>A Nessie commit-operation, performed via {@link
 * org.projectnessie.api.TreeApi#commitMultipleOperations(String, String, Operations)}, for Iceberg
 * consists of a {@link Operation.Put} with an {@link IcebergSnapshot} <em>and</em> an {@link
 * IcebergTable} as the expected-global-state.
 */
@Schema(
    type = SchemaType.OBJECT,
    title = "Iceberg table snapshot",
    description =
        "Represents the state of an Iceberg table in a Nessie branch. An Iceberg table is globally "
            + "identified via its unique 'Contents.id'.\n"
            + "\n"
            + "Note: If the Iceberg 'TableMetadata' contains no snapshot, 'currentSnapshotId' will "
            + "be -1.\n"
            + "\n"
            + "A Nessie commit-operation, performed via 'TreeApi.commitMultipleOperations', "
            + "for Iceberg consists of a 'Operation.Put' with an 'IcebergSnapshot' and an "
            + "'IcebergTable' as the expected-global-state.")
@Value.Immutable(prehash = true)
@JsonSerialize(as = ImmutableIcebergSnapshot.class)
@JsonDeserialize(as = ImmutableIcebergSnapshot.class)
@JsonTypeName("ICEBERG_SNAPSHOT")
public abstract class IcebergSnapshot extends Contents
    implements OnReferenceState<IcebergTable, IcebergTableState> {

  /** ID of the current Iceberg snapshot. This value is not present, if there is no snapshot. */
  public abstract Long getCurrentSnapshotId();

  @Override
  public IcebergTableState asCompleteState(IcebergTable state) {
    return IcebergTableState.of(this, state);
  }

  public static IcebergSnapshot of(long currentSnapshotId) {
    return ImmutableIcebergSnapshot.builder().currentSnapshotId(currentSnapshotId).build();
  }

  public static IcebergSnapshot of(long currentSnapshotId, String contentsId) {
    return ImmutableIcebergSnapshot.builder()
        .currentSnapshotId(currentSnapshotId)
        .id(contentsId)
        .build();
  }
}
