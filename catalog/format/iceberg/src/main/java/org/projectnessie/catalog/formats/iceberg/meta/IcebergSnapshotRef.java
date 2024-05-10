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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import javax.annotation.Nullable;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@JsonSerialize(as = ImmutableIcebergSnapshotRef.class)
@JsonDeserialize(as = ImmutableIcebergSnapshotRef.class)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonIgnoreProperties(ignoreUnknown = true)
public interface IcebergSnapshotRef {
  static Builder builder() {
    return ImmutableIcebergSnapshotRef.builder();
  }

  static IcebergSnapshotRef snapshotRef(
      long snapshotId,
      String type,
      Integer minSnapshotsToKeep,
      Long maxSnapshotAgeMs,
      Long maxRefAgeMs) {
    return ImmutableIcebergSnapshotRef.of(
        snapshotId, type, minSnapshotsToKeep, maxSnapshotAgeMs, maxRefAgeMs);
  }

  long snapshotId();

  String type();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  Integer minSnapshotsToKeep();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  Long maxSnapshotAgeMs();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  Long maxRefAgeMs();

  @SuppressWarnings("unused")
  interface Builder {
    @CanIgnoreReturnValue
    Builder clear();

    @CanIgnoreReturnValue
    Builder snapshotId(long snapshotId);

    @CanIgnoreReturnValue
    Builder type(String type);

    @CanIgnoreReturnValue
    Builder minSnapshotsToKeep(Integer minSnapshotsToKeep);

    @CanIgnoreReturnValue
    Builder maxSnapshotAgeMs(Long maxSnapshotAgeMs);

    @CanIgnoreReturnValue
    Builder maxRefAgeMs(Long maxRefAgeMs);

    IcebergSnapshotRef build();
  }
}
