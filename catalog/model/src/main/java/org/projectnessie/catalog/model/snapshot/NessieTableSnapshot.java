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
package org.projectnessie.catalog.model.snapshot;

import static com.google.common.base.Preconditions.checkState;
import static java.util.function.Function.identity;
import static org.projectnessie.catalog.model.schema.NessiePartitionDefinition.NO_PARTITION_SPEC_ID;
import static org.projectnessie.catalog.model.schema.NessieSortDefinition.NO_SORT_ORDER_ID;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.immutables.value.Value;
import org.projectnessie.catalog.model.NessieTable;
import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.catalog.model.schema.NessiePartitionDefinition;
import org.projectnessie.catalog.model.schema.NessieSortDefinition;
import org.projectnessie.catalog.model.statistics.NessiePartitionStatisticsFile;
import org.projectnessie.catalog.model.statistics.NessieStatisticsFile;
import org.projectnessie.model.Content;
import org.projectnessie.model.DeltaLakeTable;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.nessie.immutables.NessieImmutable;

/**
 * Represents the state of a {@link NessieTable} on a specific Nessie reference (commit).
 *
 * <p>The {@linkplain #id() ID of a table's snapshot} in the Nessie catalog is derived from relevant
 * fields in a concrete {@linkplain Content Nessie content object}, for example a {@link
 * IcebergTable} or {@link DeltaLakeTable}. This guarantees that each distinct state of a table is
 * represented by exactly one {@linkplain NessieTableSnapshot snapshot}. How exactly the {@linkplain
 * #id() ID} is derived is opaque to a user.
 */
@NessieImmutable
@JsonSerialize(as = ImmutableNessieTableSnapshot.class)
@JsonDeserialize(as = ImmutableNessieTableSnapshot.class)
@JsonTypeName("TABLE")
// Suppress: "Constructor parameters should be better defined on the same level of inheritance
// hierarchy..."
@SuppressWarnings("immutables:subtype")
public interface NessieTableSnapshot extends NessieEntitySnapshot<NessieTable> {

  @Override
  NessieTableSnapshot withId(NessieId id);

  @Override
  @Value.NonAttribute
  default String type() {
    return "TABLE";
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  @jakarta.annotation.Nullable
  NessieId currentPartitionDefinitionId();

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  List<NessiePartitionDefinition> partitionDefinitions();

  @Value.Lazy
  default Map<Integer, NessiePartitionDefinition> partitionDefinitionByIcebergId() {
    return partitionDefinitions().stream()
        .filter(p -> p.icebergId() != NO_PARTITION_SPEC_ID)
        .collect(Collectors.toMap(NessiePartitionDefinition::icebergId, identity()));
  }

  default Optional<NessiePartitionDefinition> partitionDefinitionByIcebergId(int specId) {
    return partitionDefinitions().stream().filter(p -> p.icebergId() == specId).findFirst();
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  @jakarta.annotation.Nullable
  NessieId currentSortDefinitionId();

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  List<NessieSortDefinition> sortDefinitions();

  @Value.Lazy
  default Map<Integer, NessieSortDefinition> sortDefinitionByIcebergId() {
    return sortDefinitions().stream()
        .filter(s -> s.icebergSortOrderId() != NO_SORT_ORDER_ID)
        .collect(Collectors.toMap(NessieSortDefinition::icebergSortOrderId, identity()));
  }

  default Optional<NessieSortDefinition> sortDefinitionByIcebergId(int orderId) {
    return sortDefinitions().stream().filter(s -> s.icebergSortOrderId() == orderId).findFirst();
  }

  @Override
  NessieTable entity();

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  @jakarta.annotation.Nullable
  Long icebergSnapshotId();

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  @jakarta.annotation.Nullable
  Long icebergLastSequenceNumber();

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  @jakarta.annotation.Nullable
  Long icebergSnapshotSequenceNumber();

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  @jakarta.annotation.Nullable
  Integer icebergLastColumnId();

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  @jakarta.annotation.Nullable
  Integer icebergLastPartitionId();

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  Map<String, String> icebergSnapshotSummary();

  /**
   * Corresponds to the {@code manifest-list} field in Iceberg snapshots.
   *
   * <p>When importing an Iceberg snapshot without a {@code manifest-list} but with the {@code
   * manifests} field populated, both {@link #icebergManifestFileLocations()} <em>and</em> this
   * field are populated, the manifest-list is generated from the manifest files referenced by this
   * list.
   *
   * <p>Iceberg table-metadata/snapshots generated from {@link NessieTableSnapshot} will always have
   * the {@code manifest-list} field populated and no {@code manifests} field.
   */
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  @jakarta.annotation.Nullable
  String icebergManifestListLocation();

  /**
   * Corresponds to the {@code manifests} field in Iceberg snapshots.
   *
   * <p>When importing an Iceberg snapshot without a {@code manifest-list} but with the {@code
   * manifests} field populated, both this list <em>and</em> {@link #icebergManifestListLocation()}
   * are populated, the latter contains the location of the manifest-list, which is generated from
   * the manifest files referenced by this list.
   *
   * <p>Iceberg table-metadata/snapshots generated from {@link NessieTableSnapshot} will always have
   * the {@code manifest-list} field populated and no {@code manifests} field.
   */
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  List<String> icebergManifestFileLocations();

  // TODO Iceberg external name mapping (see org.apache.iceberg.mapping.NameMappingParser +
  //  org.apache.iceberg.TableProperties.DEFAULT_NAME_MAPPING)

  @Value.Lazy
  @JsonIgnore
  default Optional<NessiePartitionDefinition> currentPartitionDefinitionObject() {
    for (NessiePartitionDefinition partitionDefinition : partitionDefinitions()) {
      if (partitionDefinition.id().equals(currentPartitionDefinitionId())) {
        return Optional.of(partitionDefinition);
      }
    }
    return Optional.empty();
  }

  @Value.Lazy
  @JsonIgnore
  default Optional<NessieSortDefinition> currentSortDefinitionObject() {
    for (NessieSortDefinition sortDefinition : sortDefinitions()) {
      if (sortDefinition.id().equals(currentSortDefinitionId())) {
        return Optional.of(sortDefinition);
      }
    }
    return Optional.empty();
  }

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  List<NessieStatisticsFile> statisticsFiles();

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  List<NessiePartitionStatisticsFile> partitionStatisticsFiles();

  @Value.Check
  default void check() {
    // 0 or 1 in icebergLastPartitionId() will cause duplicate field IDs in Avro files!
    Integer i = icebergLastPartitionId();
    checkState(
        i == null || i >= 999, "Iceberg lastPartitionId, if present, must be >= 999, but is %s", i);
  }

  static Builder builder() {
    return ImmutableNessieTableSnapshot.builder();
  }

  @SuppressWarnings("unused")
  interface Builder extends NessieEntitySnapshot.Builder<Builder> {
    @CanIgnoreReturnValue
    Builder from(NessieTableSnapshot instance);

    @CanIgnoreReturnValue
    Builder entity(NessieTable entity);

    @CanIgnoreReturnValue
    Builder currentPartitionDefinitionId(@Nullable NessieId currentPartitionDefinitionId);

    @CanIgnoreReturnValue
    Builder currentSortDefinitionId(@Nullable NessieId currentSortDefinitionId);

    @CanIgnoreReturnValue
    Builder addPartitionDefinition(NessiePartitionDefinition element);

    @CanIgnoreReturnValue
    Builder addPartitionDefinitions(NessiePartitionDefinition... elements);

    @CanIgnoreReturnValue
    Builder partitionDefinitions(Iterable<? extends NessiePartitionDefinition> elements);

    @CanIgnoreReturnValue
    Builder addAllPartitionDefinitions(Iterable<? extends NessiePartitionDefinition> elements);

    @CanIgnoreReturnValue
    Builder addSortDefinition(NessieSortDefinition element);

    @CanIgnoreReturnValue
    Builder addSortDefinitions(NessieSortDefinition... elements);

    @CanIgnoreReturnValue
    Builder sortDefinitions(Iterable<? extends NessieSortDefinition> elements);

    @CanIgnoreReturnValue
    Builder addAllSortDefinitions(Iterable<? extends NessieSortDefinition> elements);

    @CanIgnoreReturnValue
    Builder icebergSnapshotId(@Nullable Long icebergSnapshotId);

    @CanIgnoreReturnValue
    Builder icebergLastSequenceNumber(@Nullable Long icebergLastSequenceNumber);

    @CanIgnoreReturnValue
    Builder icebergSnapshotSequenceNumber(@Nullable Long icebergSnapshotSequenceNumber);

    @CanIgnoreReturnValue
    Builder icebergLastColumnId(@Nullable Integer icebergLastColumnId);

    @CanIgnoreReturnValue
    Builder icebergLastPartitionId(@Nullable Integer icebergLastPartitionId);

    @CanIgnoreReturnValue
    Builder putIcebergSnapshotSummary(String key, String value);

    @CanIgnoreReturnValue
    Builder putIcebergSnapshotSummary(Map.Entry<String, ? extends String> entry);

    @CanIgnoreReturnValue
    Builder icebergSnapshotSummary(Map<String, ? extends String> entries);

    @CanIgnoreReturnValue
    Builder putAllIcebergSnapshotSummary(Map<String, ? extends String> entries);

    @CanIgnoreReturnValue
    Builder icebergManifestListLocation(String icebergManifestListLocation);

    @CanIgnoreReturnValue
    Builder addIcebergManifestFileLocation(String element);

    @CanIgnoreReturnValue
    Builder addIcebergManifestFileLocations(String... elements);

    @CanIgnoreReturnValue
    Builder icebergManifestFileLocations(Iterable<String> elements);

    @CanIgnoreReturnValue
    Builder addAllIcebergManifestFileLocations(Iterable<String> elements);

    @CanIgnoreReturnValue
    Builder addStatisticsFile(NessieStatisticsFile element);

    @CanIgnoreReturnValue
    Builder addStatisticsFiles(NessieStatisticsFile... elements);

    @CanIgnoreReturnValue
    Builder statisticsFiles(Iterable<? extends NessieStatisticsFile> elements);

    @CanIgnoreReturnValue
    Builder addAllStatisticsFiles(Iterable<? extends NessieStatisticsFile> elements);

    @CanIgnoreReturnValue
    Builder addPartitionStatisticsFile(NessiePartitionStatisticsFile element);

    @CanIgnoreReturnValue
    Builder addPartitionStatisticsFiles(NessiePartitionStatisticsFile... elements);

    @CanIgnoreReturnValue
    Builder partitionStatisticsFiles(Iterable<? extends NessiePartitionStatisticsFile> elements);

    @CanIgnoreReturnValue
    Builder addAllPartitionStatisticsFiles(
        Iterable<? extends NessiePartitionStatisticsFile> elements);

    NessieTableSnapshot build();
  }
}
