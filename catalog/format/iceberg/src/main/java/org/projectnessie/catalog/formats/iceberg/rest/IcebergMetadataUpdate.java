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
package org.projectnessie.catalog.formats.iceberg.rest;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.icebergStatisticsFileToNessie;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import javax.annotation.Nullable;
import org.immutables.value.Value;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionSpec;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionStatisticsFile;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSchema;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSnapshot;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSortOrder;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergStatisticsFile;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergViewRepresentation;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergViewVersion;
import org.projectnessie.catalog.formats.iceberg.nessie.CatalogOps;
import org.projectnessie.catalog.formats.iceberg.nessie.IcebergTableMetadataUpdateState;
import org.projectnessie.catalog.formats.iceberg.nessie.IcebergViewMetadataUpdateState;
import org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg;
import org.projectnessie.nessie.immutables.NessieImmutable;

/** Iceberg metadata update objects serialized according to the Iceberg REST Catalog schema. */
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "action")
@JsonSubTypes({
  @JsonSubTypes.Type(value = IcebergMetadataUpdate.AssignUUID.class, name = "assign-uuid"),
  @JsonSubTypes.Type(
      value = IcebergMetadataUpdate.UpgradeFormatVersion.class,
      name = "upgrade-format-version"),
  @JsonSubTypes.Type(value = IcebergMetadataUpdate.AddSchema.class, name = "add-schema"),
  @JsonSubTypes.Type(
      value = IcebergMetadataUpdate.SetCurrentSchema.class,
      name = "set-current-schema"),
  @JsonSubTypes.Type(value = IcebergMetadataUpdate.AddPartitionSpec.class, name = "add-spec"),
  @JsonSubTypes.Type(
      value = IcebergMetadataUpdate.SetDefaultPartitionSpec.class,
      name = "set-default-spec"),
  @JsonSubTypes.Type(
      value = IcebergMetadataUpdate.RemovePartitionSpecs.class,
      name = "remove-partition-specs"),
  @JsonSubTypes.Type(value = IcebergMetadataUpdate.AddSortOrder.class, name = "add-sort-order"),
  @JsonSubTypes.Type(
      value = IcebergMetadataUpdate.SetDefaultSortOrder.class,
      name = "set-default-sort-order"),
  @JsonSubTypes.Type(value = IcebergMetadataUpdate.AddSnapshot.class, name = "add-snapshot"),
  @JsonSubTypes.Type(
      value = IcebergMetadataUpdate.RemoveSnapshots.class,
      name = "remove-snapshots"),
  @JsonSubTypes.Type(
      value = IcebergMetadataUpdate.RemoveSnapshotRef.class,
      name = "remove-snapshot-ref"),
  @JsonSubTypes.Type(value = IcebergMetadataUpdate.SetSnapshotRef.class, name = "set-snapshot-ref"),
  @JsonSubTypes.Type(value = IcebergMetadataUpdate.SetProperties.class, name = "set-properties"),
  @JsonSubTypes.Type(
      value = IcebergMetadataUpdate.RemoveProperties.class,
      name = "remove-properties"),
  @JsonSubTypes.Type(value = IcebergMetadataUpdate.SetLocation.class, name = "set-location"),
  @JsonSubTypes.Type(value = IcebergMetadataUpdate.SetStatistics.class, name = "set-statistics"),
  @JsonSubTypes.Type(
      value = IcebergMetadataUpdate.RemoveStatistics.class,
      name = "remove-statistics"),
  @JsonSubTypes.Type(value = IcebergMetadataUpdate.AddViewVersion.class, name = "add-view-version"),
  @JsonSubTypes.Type(
      value = IcebergMetadataUpdate.SetCurrentViewVersion.class,
      name = "set-current-view-version"),
  @JsonSubTypes.Type(
      value = IcebergMetadataUpdate.SetPartitionStatistics.class,
      name = "set-partition-statistics"),
  @JsonSubTypes.Type(
      value = IcebergMetadataUpdate.RemovePartitionStatistics.class,
      name = "remove-partition-statistics"),
})
public interface IcebergMetadataUpdate {

  default void applyToTable(IcebergTableMetadataUpdateState state) {
    throw new UnsupportedOperationException(
        "Metadata "
            + getClass().getSimpleName().replace("Immutable", "")
            + " update not supported for tables");
  }

  default void applyToView(IcebergViewMetadataUpdateState state) {
    throw new UnsupportedOperationException(
        "Metadata "
            + getClass().getSimpleName().replace("Immutable", "")
            + " update not supported for views");
  }

  @NessieImmutable
  @JsonTypeName("upgrade-format-version")
  @JsonSerialize(as = ImmutableUpgradeFormatVersion.class)
  @JsonDeserialize(as = ImmutableUpgradeFormatVersion.class)
  interface UpgradeFormatVersion extends IcebergMetadataUpdate {

    int formatVersion();

    static UpgradeFormatVersion upgradeFormatVersion(int formatVersion) {
      return ImmutableUpgradeFormatVersion.of(formatVersion);
    }

    @Override
    default void applyToTable(IcebergTableMetadataUpdateState state) {
      state.addCatalogOp(CatalogOps.META_UPGRADE_FORMAT_VERSION);
      NessieModelIceberg.upgradeFormatVersion(formatVersion(), state.snapshot(), state.builder());
    }

    @Override
    default void applyToView(IcebergViewMetadataUpdateState state) {
      state.addCatalogOp(CatalogOps.META_UPGRADE_FORMAT_VERSION);
      NessieModelIceberg.upgradeFormatVersion(formatVersion(), state.snapshot(), state.builder());
    }
  }

  @NessieImmutable
  @JsonTypeName("remove-snapshots")
  @JsonSerialize(as = ImmutableRemoveSnapshots.class)
  @JsonDeserialize(as = ImmutableRemoveSnapshots.class)
  interface RemoveSnapshots extends IcebergMetadataUpdate {

    List<Long> snapshotIds();

    @Override
    default void applyToTable(IcebergTableMetadataUpdateState state) {
      throw new UnsupportedOperationException(
          "Nessie Catalog does not allow external snapshot management");
    }
  }

  @NessieImmutable
  @JsonTypeName("remove-properties")
  @JsonSerialize(as = ImmutableRemoveProperties.class)
  @JsonDeserialize(as = ImmutableRemoveProperties.class)
  interface RemoveProperties extends IcebergMetadataUpdate {

    @JsonAlias({"removals", "removed"})
    List<String> removals();

    @Override
    default void applyToTable(IcebergTableMetadataUpdateState state) {
      state.addCatalogOp(CatalogOps.META_REMOVE_PROPERTIES);
      if (removals().contains("location")) {
        state.addCatalogOp(CatalogOps.META_REMOVE_LOCATION_PROPERTY);
      }
      NessieModelIceberg.removeProperties(this, state.snapshot(), state.builder());
    }

    @Override
    default void applyToView(IcebergViewMetadataUpdateState state) {
      state.addCatalogOp(CatalogOps.META_REMOVE_PROPERTIES);
      if (removals().contains("location")) {
        state.addCatalogOp(CatalogOps.META_REMOVE_LOCATION_PROPERTY);
      }
      NessieModelIceberg.removeProperties(this, state.snapshot(), state.builder());
    }
  }

  @NessieImmutable
  @JsonTypeName("add-view-version")
  @JsonSerialize(as = ImmutableAddViewVersion.class)
  @JsonDeserialize(as = ImmutableAddViewVersion.class)
  interface AddViewVersion extends IcebergMetadataUpdate {

    IcebergViewVersion viewVersion();

    @Override
    default void applyToView(IcebergViewMetadataUpdateState state) {
      state.addCatalogOp(CatalogOps.META_ADD_VIEW_VERSION);
      NessieModelIceberg.addViewVersion(this, state);
    }

    @Value.Check
    default void check() {
      Set<String> keys = new HashSet<>();
      for (IcebergViewRepresentation representation : viewVersion().representations()) {
        if (!keys.add(representation.representationKey())) {
          throw new IllegalArgumentException(
              "Invalid view version: Cannot add multiple queries for dialect "
                  + representation.representationKey());
        }
      }
    }

    static AddViewVersion addViewVersion(IcebergViewVersion viewVersion) {
      return ImmutableAddViewVersion.of(viewVersion);
    }
  }

  @NessieImmutable
  @JsonTypeName("set-current-view-version")
  @JsonSerialize(as = ImmutableSetCurrentViewVersion.class)
  @JsonDeserialize(as = ImmutableSetCurrentViewVersion.class)
  interface SetCurrentViewVersion extends IcebergMetadataUpdate {

    long viewVersionId();

    @Override
    default void applyToView(IcebergViewMetadataUpdateState state) {
      state.addCatalogOp(CatalogOps.META_SET_CURRENT_VIEW_VERSION);
      NessieModelIceberg.setCurrentViewVersion(this, state);
    }

    static SetCurrentViewVersion setCurrentViewVersion(long viewVersionId) {
      return ImmutableSetCurrentViewVersion.of(viewVersionId);
    }
  }

  @NessieImmutable
  @JsonTypeName("set-statistics")
  @JsonSerialize(as = ImmutableSetStatistics.class)
  @JsonDeserialize(as = ImmutableSetStatistics.class)
  interface SetStatistics extends IcebergMetadataUpdate {

    // Becomes deprecated w/ Iceberg 1.8, see https://github.com/apache/iceberg/pull/12010
    OptionalLong snapshotId();

    IcebergStatisticsFile statistics();

    @Override
    default void applyToTable(IcebergTableMetadataUpdateState state) {
      state.addCatalogOp(CatalogOps.META_SET_STATISTICS);
      long snapshotId = Objects.requireNonNull(state.snapshot().icebergSnapshotId());
      if (snapshotId == statistics().snapshotId()) {
        state.builder().statisticsFiles(singleton(icebergStatisticsFileToNessie(statistics())));
      }
    }
  }

  @NessieImmutable
  @JsonTypeName("remove-statistics")
  @JsonSerialize(as = ImmutableRemoveStatistics.class)
  @JsonDeserialize(as = ImmutableRemoveStatistics.class)
  interface RemoveStatistics extends IcebergMetadataUpdate {

    long snapshotId();

    @Override
    default void applyToTable(IcebergTableMetadataUpdateState state) {
      state.addCatalogOp(CatalogOps.META_REMOVE_STATISTICS);
      long snapshotId = Objects.requireNonNull(state.snapshot().icebergSnapshotId());
      if (snapshotId == snapshotId()) {
        state.builder().statisticsFiles(emptyList());
      }
    }
  }

  @NessieImmutable
  @JsonTypeName("set-partition-statistics")
  @JsonSerialize(as = ImmutableSetPartitionStatistics.class)
  @JsonDeserialize(as = ImmutableSetPartitionStatistics.class)
  interface SetPartitionStatistics extends IcebergMetadataUpdate {

    IcebergPartitionStatisticsFile partitionStatistics();

    @Override
    default void applyToTable(IcebergTableMetadataUpdateState state) {
      state.addCatalogOp(CatalogOps.META_SET_PARTITION_STATISTICS);
      long snapshotId = Objects.requireNonNull(state.snapshot().icebergSnapshotId());
      if (snapshotId == partitionStatistics().snapshotId()) {
        state
            .builder()
            .partitionStatisticsFiles(
                singleton(
                    NessieModelIceberg.icebergPartitionStatisticsFileToNessie(
                        partitionStatistics())));
      }
    }
  }

  @NessieImmutable
  @JsonTypeName("remove-partition-statistics")
  @JsonSerialize(as = ImmutableRemovePartitionStatistics.class)
  @JsonDeserialize(as = ImmutableRemovePartitionStatistics.class)
  interface RemovePartitionStatistics extends IcebergMetadataUpdate {
    long snapshotId();

    @Override
    default void applyToTable(IcebergTableMetadataUpdateState state) {
      state.addCatalogOp(CatalogOps.META_REMOVE_PARTITION_STATISTICS);
      long snapshotId = Objects.requireNonNull(state.snapshot().icebergSnapshotId());
      if (snapshotId == snapshotId()) {
        state.builder().partitionStatisticsFiles(emptyList());
      }
    }
  }

  @NessieImmutable
  @JsonTypeName("assign-uuid")
  @JsonSerialize(as = ImmutableAssignUUID.class)
  @JsonDeserialize(as = ImmutableAssignUUID.class)
  interface AssignUUID extends IcebergMetadataUpdate {
    String uuid();

    @Override
    default void applyToTable(IcebergTableMetadataUpdateState state) {
      state.addCatalogOp(CatalogOps.META_ASSIGN_UUID);
      NessieModelIceberg.assignUUID(this, state.snapshot());
    }

    @Override
    default void applyToView(IcebergViewMetadataUpdateState state) {
      state.addCatalogOp(CatalogOps.META_ASSIGN_UUID);
      NessieModelIceberg.assignUUID(this, state.snapshot());
    }

    static AssignUUID assignUUID(String uuid) {
      return ImmutableAssignUUID.of(uuid);
    }
  }

  @NessieImmutable
  @JsonTypeName("add-schema")
  @JsonSerialize(as = ImmutableAddSchema.class)
  @JsonDeserialize(as = ImmutableAddSchema.class)
  interface AddSchema extends IcebergMetadataUpdate {
    IcebergSchema schema();

    OptionalInt lastColumnId();

    @Override
    default void applyToTable(IcebergTableMetadataUpdateState state) {
      state.addCatalogOp(CatalogOps.META_ADD_SCHEMA);
      NessieModelIceberg.addSchema(this, state);
    }

    @Override
    default void applyToView(IcebergViewMetadataUpdateState state) {
      state.addCatalogOp(CatalogOps.META_ADD_SCHEMA);
      NessieModelIceberg.addSchema(this, state);
    }

    static AddSchema addSchema(IcebergSchema schema) {
      return ImmutableAddSchema.of(schema, OptionalInt.empty());
    }
  }

  @NessieImmutable
  @JsonTypeName("set-current-schema")
  @JsonSerialize(as = ImmutableSetCurrentSchema.class)
  @JsonDeserialize(as = ImmutableSetCurrentSchema.class)
  interface SetCurrentSchema extends IcebergMetadataUpdate {
    /** ID of the schema to become the current one or {@code -1} to use the last added schema. */
    int schemaId();

    @Override
    default void applyToTable(IcebergTableMetadataUpdateState state) {
      state.addCatalogOp(CatalogOps.META_SET_CURRENT_SCHEMA);
      NessieModelIceberg.setCurrentSchema(
          this, state.lastAddedSchemaId(), state.snapshot(), state.builder());
    }

    @Override
    default void applyToView(IcebergViewMetadataUpdateState state) {
      state.addCatalogOp(CatalogOps.META_SET_CURRENT_SCHEMA);
      NessieModelIceberg.setCurrentSchema(
          this, state.lastAddedSchemaId(), state.snapshot(), state.builder());
    }

    static SetCurrentSchema setCurrentSchema(int schemaId) {
      return ImmutableSetCurrentSchema.of(schemaId);
    }
  }

  @NessieImmutable
  @JsonTypeName("add-spec")
  @JsonSerialize(as = ImmutableAddPartitionSpec.class)
  @JsonDeserialize(as = ImmutableAddPartitionSpec.class)
  interface AddPartitionSpec extends IcebergMetadataUpdate {
    IcebergPartitionSpec spec();

    @Override
    default void applyToTable(IcebergTableMetadataUpdateState state) {
      state.addCatalogOp(CatalogOps.META_ADD_PARTITION_SPEC);
      NessieModelIceberg.addPartitionSpec(this, state);
    }

    @Value.Check
    default void check() {
      int id = spec().specId();
      checkState(id >= -1, "Illegal spec-ID %s", id);
    }

    static AddPartitionSpec addPartitionSpec(IcebergPartitionSpec spec) {
      return ImmutableAddPartitionSpec.of(spec);
    }
  }

  @NessieImmutable
  @JsonTypeName("remove-partition-specs")
  @JsonSerialize(as = ImmutableRemovePartitionSpecs.class)
  @JsonDeserialize(as = ImmutableRemovePartitionSpecs.class)
  interface RemovePartitionSpecs extends IcebergMetadataUpdate {
    List<Integer> specIds();

    @Override
    default void applyToTable(IcebergTableMetadataUpdateState state) {
      NessieModelIceberg.removePartitionSpecs(this, state);
    }

    @Value.Check
    default void check() {
      for (Integer specId : specIds()) {
        checkState(specId != null && specId >= 0, "Illegal spec-ID %s", specId);
      }
    }

    static RemovePartitionSpecs removePartitionSpecs(List<Integer> specIds) {
      return ImmutableRemovePartitionSpecs.of(specIds);
    }
  }

  @NessieImmutable
  @JsonTypeName("set-default-spec")
  @JsonSerialize(as = ImmutableSetDefaultPartitionSpec.class)
  @JsonDeserialize(as = ImmutableSetDefaultPartitionSpec.class)
  interface SetDefaultPartitionSpec extends IcebergMetadataUpdate {
    /**
     * ID of the partition spec to become the current one or {@code -1} to use the last added
     * partition spec.
     */
    int specId();

    @Override
    default void applyToTable(IcebergTableMetadataUpdateState state) {
      state.addCatalogOp(CatalogOps.META_SET_DEFAULT_PARTITION_SPEC);
      NessieModelIceberg.setDefaultPartitionSpec(this, state);
    }

    static SetDefaultPartitionSpec setDefaultPartitionSpec(int specId) {
      return ImmutableSetDefaultPartitionSpec.of(specId);
    }
  }

  @NessieImmutable
  @JsonTypeName("add-snapshot")
  @JsonSerialize(as = ImmutableAddSnapshot.class)
  @JsonDeserialize(as = ImmutableAddSnapshot.class)
  interface AddSnapshot extends IcebergMetadataUpdate {
    IcebergSnapshot snapshot();

    @Override
    default void applyToTable(IcebergTableMetadataUpdateState state) {
      state.addCatalogOp(CatalogOps.META_ADD_SNAPSHOT);
      Map<String, String> summary = snapshot().summary();

      String v = summary.get("added-data-files");
      if (v != null && Long.parseLong(v) > 0) {
        state.addCatalogOp(CatalogOps.SNAP_ADD_DATA_FILES);
      }
      v = summary.get("deleted-data-files");
      if (v != null && Long.parseLong(v) > 0) {
        state.addCatalogOp(CatalogOps.SNAP_DELETE_DATA_FILES);
      }
      v = summary.get("added-delete-files");
      if (v != null && Long.parseLong(v) > 0) {
        state.addCatalogOp(CatalogOps.SNAP_ADD_DELETE_FILES);
      }
      v = summary.get("added-equality-delete-files");
      if (v != null && Long.parseLong(v) > 0) {
        state.addCatalogOp(CatalogOps.SNAP_ADD_EQUALITY_DELETE_FILES);
      }
      v = summary.get("added-position-delete-files");
      if (v != null && Long.parseLong(v) > 0) {
        state.addCatalogOp(CatalogOps.SNAP_ADD_POSITION_DELETE_FILES);
      }
      v = summary.get("removed-delete-files");
      if (v != null && Long.parseLong(v) > 0) {
        state.addCatalogOp(CatalogOps.SNAP_REMOVE_DELETE_FILES);
      }
      v = summary.get("removed-equality-delete-files");
      if (v != null && Long.parseLong(v) > 0) {
        state.addCatalogOp(CatalogOps.SNAP_REMOVE_EQUALITY_DELETE_FILES);
      }
      v = summary.get("removed-position-delete-files");
      if (v != null && Long.parseLong(v) > 0) {
        state.addCatalogOp(CatalogOps.SNAP_REMOVE_POSITION_DELETE_FILES);
      }
      v = summary.get("added-records");
      if (v != null && Long.parseLong(v) > 0) {
        state.addCatalogOp(CatalogOps.SNAP_ADDED_RECORDS);
      }
      v = summary.get("deleted-records");
      if (v != null && Long.parseLong(v) > 0) {
        state.addCatalogOp(CatalogOps.SNAP_DELETED_RECORDS);
      }
      v = summary.get("added-position-deletes");
      if (v != null && Long.parseLong(v) > 0) {
        state.addCatalogOp(CatalogOps.SNAP_ADDED_POSITION_DELETES);
      }
      v = summary.get("deleted-position-deletes");
      if (v != null && Long.parseLong(v) > 0) {
        state.addCatalogOp(CatalogOps.SNAP_DELETED_POSITION_DELETES);
      }
      v = summary.get("added-equality-deletes");
      if (v != null && Long.parseLong(v) > 0) {
        state.addCatalogOp(CatalogOps.SNAP_ADDED_EQUALITY_DELETES);
      }
      v = summary.get("deleted-equality-deletes");
      if (v != null && Long.parseLong(v) > 0) {
        state.addCatalogOp(CatalogOps.SNAP_DELETED_EQUALITY_DELETES);
      }
      v = summary.get("replace-partitions");
      if (Boolean.parseBoolean(v)) {
        state.addCatalogOp(CatalogOps.SNAP_REPLACE_PARTITIONS);
      }
      v = summary.get("operation");
      if (v != null) {
        switch (v.toLowerCase(Locale.ROOT)) {
          case "append":
            state.addCatalogOp(CatalogOps.SNAP_OP_APPEND);
            break;
          case "replace":
            state.addCatalogOp(CatalogOps.SNAP_OP_REPLACE);
            break;
          case "overwrite":
            state.addCatalogOp(CatalogOps.SNAP_OP_OVERWRITE);
            break;
          case "delete":
            state.addCatalogOp(CatalogOps.SNAP_OP_DELETE);
            break;
          default:
            break;
        }
      }

      NessieModelIceberg.addSnapshot(this, state);
    }
  }

  @NessieImmutable
  @JsonTypeName("add-sort-order")
  @JsonSerialize(as = ImmutableAddSortOrder.class)
  @JsonDeserialize(as = ImmutableAddSortOrder.class)
  interface AddSortOrder extends IcebergMetadataUpdate {
    IcebergSortOrder sortOrder();

    @Override
    default void applyToTable(IcebergTableMetadataUpdateState state) {
      state.addCatalogOp(CatalogOps.META_ADD_SORT_ORDER);
      NessieModelIceberg.addSortOrder(this, state);
    }

    @Value.Check
    default void check() {
      int id = sortOrder().orderId();
      boolean unsorted = sortOrder().isUnsorted();
      checkState(
          id == -1 || id > 0 || (unsorted && id == 0),
          "Illegal order-ID %s for %s",
          id,
          unsorted ? "unsorted" : "sort order");
    }

    static AddSortOrder addSortOrder(IcebergSortOrder sortOrder) {
      return ImmutableAddSortOrder.of(sortOrder);
    }
  }

  @NessieImmutable
  @JsonTypeName("set-default-sort-order")
  @JsonSerialize(as = ImmutableSetDefaultSortOrder.class)
  @JsonDeserialize(as = ImmutableSetDefaultSortOrder.class)
  interface SetDefaultSortOrder extends IcebergMetadataUpdate {

    /**
     * ID of the sort order to become the current one or {@code -1} to use the last added sort
     * order.
     */
    int sortOrderId();

    @Override
    default void applyToTable(IcebergTableMetadataUpdateState state) {
      state.addCatalogOp(CatalogOps.META_SET_DEFAULT_SORT_ORDER);
      NessieModelIceberg.setDefaultSortOrder(this, state);
    }

    static SetDefaultSortOrder setDefaultSortOrder(int sortOrderId) {
      return ImmutableSetDefaultSortOrder.of(sortOrderId);
    }
  }

  @NessieImmutable
  @JsonTypeName("set-location")
  @JsonSerialize(as = ImmutableSetLocation.class)
  @JsonDeserialize(as = ImmutableSetLocation.class)
  interface SetLocation extends IcebergMetadataUpdate {
    String location();

    @JsonIgnore
    @Value.Default
    default boolean trusted() {
      return false;
    }

    @Override
    default void applyToTable(IcebergTableMetadataUpdateState state) {
      state.addCatalogOp(CatalogOps.META_SET_LOCATION);
      if (trusted()) {
        NessieModelIceberg.setLocation(this, state.builder());
      }
    }

    @Override
    default void applyToView(IcebergViewMetadataUpdateState state) {
      state.addCatalogOp(CatalogOps.META_SET_LOCATION);
      if (trusted()) {
        NessieModelIceberg.setLocation(this, state.builder());
      }
    }

    static SetLocation setTrustedLocation(String location) {
      return ImmutableSetLocation.of(location, true);
    }
  }

  @NessieImmutable
  @JsonTypeName("set-properties")
  @JsonSerialize(as = ImmutableSetProperties.class)
  @JsonDeserialize(as = ImmutableSetProperties.class)
  interface SetProperties extends IcebergMetadataUpdate {
    @JsonAlias({"updates", "updated"})
    Map<String, String> updates();

    @Override
    default void applyToTable(IcebergTableMetadataUpdateState state) {
      state.addCatalogOp(CatalogOps.META_SET_PROPERTIES);
      if (updates().containsKey("location")) {
        state.addCatalogOp(CatalogOps.META_SET_LOCATION);
      }
      NessieModelIceberg.setProperties(this, state.snapshot(), state.builder());
    }

    @Override
    default void applyToView(IcebergViewMetadataUpdateState state) {
      state.addCatalogOp(CatalogOps.META_SET_PROPERTIES);
      if (updates().containsKey("location")) {
        state.addCatalogOp(CatalogOps.META_SET_LOCATION);
      }
      NessieModelIceberg.setProperties(this, state.snapshot(), state.builder());
    }

    static SetProperties setProperties(Map<String, String> updates) {
      return ImmutableSetProperties.of(updates);
    }
  }

  @NessieImmutable
  @JsonTypeName("set-snapshot-ref")
  @JsonSerialize(as = ImmutableSetSnapshotRef.class)
  @JsonDeserialize(as = ImmutableSetSnapshotRef.class)
  interface SetSnapshotRef extends IcebergMetadataUpdate {
    String refName();

    Long snapshotId();

    String type(); // BRANCH or TAG

    @Nullable
    @JsonInclude(JsonInclude.Include.NON_NULL)
    Integer minSnapshotsToKeep();

    @Nullable
    @JsonInclude(JsonInclude.Include.NON_NULL)
    Long maxSnapshotAgeMs();

    @Nullable
    @JsonInclude(JsonInclude.Include.NON_NULL)
    Long maxRefAgeMs();

    @Override
    default void applyToTable(IcebergTableMetadataUpdateState state) {
      checkState(
          "main".equals(refName()) && "branch".equals(type()),
          "Nessie only supports the current snapshot-ref 'main', use Nessie's branches instead");
      var currentSnapshotId = state.snapshot().icebergSnapshotId();
      checkState(
          Objects.equals(snapshotId(), state.snapshot().icebergSnapshotId()),
          "Snapshot ID mismatch, must be %s, but is %s",
          currentSnapshotId,
          snapshotId());
      state.addCatalogOp(CatalogOps.META_SET_SNAPSHOT_REF);
      // NOP - This class is used for JSON deserialization only.
      // Nessie has catalog-level branches and tags.
    }
  }

  @NessieImmutable
  @JsonTypeName("remove-snapshot-ref")
  @JsonSerialize(as = ImmutableRemoveSnapshotRef.class)
  @JsonDeserialize(as = ImmutableRemoveSnapshotRef.class)
  interface RemoveSnapshotRef extends IcebergMetadataUpdate {

    String refName();

    @Override
    default void applyToTable(IcebergTableMetadataUpdateState state) {
      checkState(
          "main".equals(refName()),
          "Nessie only supports the Iceberg snapshot-ref 'main', use Nessie's branches instead");
      state.addCatalogOp(CatalogOps.META_REMOVE_SNAPSHOT_REF);
      // NOP - This class is used for JSON deserialization only.
      // Nessie has catalog-level branches and tags.
    }
  }
}
