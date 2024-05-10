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

import static com.google.common.base.Preconditions.checkState;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonView;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.immutables.value.Value;
import org.projectnessie.catalog.formats.iceberg.IcebergSpec;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@JsonSerialize(as = ImmutableIcebergSnapshot.class)
@JsonDeserialize(as = ImmutableIcebergSnapshot.class)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonIgnoreProperties(ignoreUnknown = true)
public interface IcebergSnapshot {
  static Builder builder() {
    return ImmutableIcebergSnapshot.builder();
  }

  static IcebergSnapshot snapshot(
      Long sequenceNumber,
      long snapshotId,
      Long parentSnapshotId,
      long timestampMs,
      Map<String, String> summary,
      List<String> manifests,
      String manifestList,
      Integer schemaId) {
    return ImmutableIcebergSnapshot.of(
        sequenceNumber,
        snapshotId,
        parentSnapshotId,
        timestampMs,
        summary,
        manifests,
        manifestList,
        schemaId);
  }

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonView(IcebergSpec.IcebergSpecV2.class)
  Long sequenceNumber();

  long snapshotId();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  Long parentSnapshotId();

  long timestampMs();

  String OPERATION = "operation";

  Map<String, String> summary();

  @JsonView(IcebergSpec.IcebergSpecV1.class)
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  List<String> manifests();

  @JsonView(IcebergSpec.IcebergSpecV2.class)
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  @jakarta.annotation.Nullable
  String manifestList();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  Integer schemaId();

  @Value.Check
  default void check() {
    checkState(
        summary().containsKey(OPERATION),
        "summary must contain an entry with the key '%s'",
        OPERATION);
  }

  @SuppressWarnings("unused")
  interface Builder {
    @CanIgnoreReturnValue
    Builder from(IcebergSnapshot instance);

    @CanIgnoreReturnValue
    Builder sequenceNumber(@Nullable Long sequenceNumber);

    @CanIgnoreReturnValue
    Builder snapshotId(long snapshotId);

    @CanIgnoreReturnValue
    Builder parentSnapshotId(@Nullable Long parentSnapshotId);

    @CanIgnoreReturnValue
    Builder timestampMs(long timestampMs);

    @CanIgnoreReturnValue
    Builder putSummary(String key, String value);

    @CanIgnoreReturnValue
    Builder putSummary(Map.Entry<String, ? extends String> entry);

    @CanIgnoreReturnValue
    Builder summary(Map<String, ? extends String> entries);

    @CanIgnoreReturnValue
    Builder putAllSummary(Map<String, ? extends String> entries);

    @CanIgnoreReturnValue
    Builder addManifest(String element);

    @CanIgnoreReturnValue
    Builder addManifests(String... elements);

    @CanIgnoreReturnValue
    Builder manifests(Iterable<String> elements);

    @CanIgnoreReturnValue
    Builder addAllManifests(Iterable<String> elements);

    @CanIgnoreReturnValue
    Builder manifestList(@Nullable String manifestList);

    @CanIgnoreReturnValue
    Builder schemaId(@Nullable Integer schemaId);

    IcebergSnapshot build();
  }
}
