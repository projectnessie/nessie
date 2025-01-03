/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.catalog.model.snapshot;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.projectnessie.catalog.model.schema.NessieSchema;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@JsonSerialize(as = ImmutableImplicitIcebergSnapshot.class)
@JsonDeserialize(as = ImmutableImplicitIcebergSnapshot.class)
public interface ImplicitIcebergSnapshot {
  static ImplicitIcebergSnapshot implicitIcebergSnapshot(NessieTableSnapshot tableSnapshot) {
    return ImmutableImplicitIcebergSnapshot.of(
        tableSnapshot.icebergSnapshotId(),
        tableSnapshot.icebergSnapshotSequenceNumber(),
        tableSnapshot.snapshotCreatedTimestamp().toEpochMilli(),
        tableSnapshot.icebergSnapshotSummary(),
        tableSnapshot.icebergManifestFileLocations(),
        tableSnapshot.icebergManifestListLocation(),
        tableSnapshot
            .currentSchemaObject()
            .map(NessieSchema::icebergId)
            .orElse(NessieSchema.NO_SCHEMA_ID));
  }

  long snapshotId();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  Long sequenceNumber();

  long timestampMs();

  Map<String, String> summary();

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  List<String> manifests();

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  @jakarta.annotation.Nullable
  String manifestList();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  Integer schemaId();
}
