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
package org.projectnessie.restcatalog.service.resources;

import static java.util.Objects.requireNonNull;

import com.google.common.base.Preconditions;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.enterprise.context.RequestScoped;
import org.agrona.collections.LongHashSet;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.io.FileIO;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.model.MergeResponse;
import org.projectnessie.restcatalog.api.NessieCatalogV0Api;
import org.projectnessie.restcatalog.api.model.AddDataFilesOutcome;
import org.projectnessie.restcatalog.api.model.AddDataFilesRequest;
import org.projectnessie.restcatalog.api.model.CatalogConfig;
import org.projectnessie.restcatalog.api.model.MergeOutcome;
import org.projectnessie.restcatalog.api.model.MergeRequest;
import org.projectnessie.restcatalog.api.model.PickTablesOutcome;
import org.projectnessie.restcatalog.api.model.PickTablesRequest;
import org.projectnessie.restcatalog.service.DecodedPrefix;
import org.projectnessie.restcatalog.service.TableRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RequestScoped
@jakarta.enterprise.context.RequestScoped
public class NessieCatalogV0ApiResource extends BaseIcebergResource implements NessieCatalogV0Api {

  private static final Logger LOGGER = LoggerFactory.getLogger(NessieCatalogV0ApiResource.class);

  @Override
  public CatalogConfig config() {
    return null;
  }

  @Override
  public AddDataFilesOutcome addDataFiles(
      String prefix, String namespace, String table, AddDataFilesRequest addDataFiles)
      throws IOException {
    TableRef tableRef = decodeTableRef(prefix, namespace, table);

    @SuppressWarnings("resource")
    NessieApiV2 api = tenantSpecific.api();

    return null;
  }

  @Override
  public PickTablesOutcome pickTables(String prefix, PickTablesRequest pickTables)
      throws IOException {
    DecodedPrefix decoded = decodePrefix(prefix);

    @SuppressWarnings("resource")
    NessieApiV2 api = tenantSpecific.api();

    return null;
  }

  @Override
  public MergeOutcome merge(String prefix, MergeRequest merge) throws IOException {
    DecodedPrefix decoded = decodePrefix(prefix);

    @SuppressWarnings("resource")
    NessieApiV2 api = tenantSpecific.api();

    try {
      MergeResponse response =
          api.mergeRefIntoBranch()
              .branchName(decoded.parsedReference().name())
              .returnConflictAsResult(true)
              // TODO .mergeMode()
              // TODO .defaultMergeMode()
              // TODO .fromHash()
              // TODO .fromRefName()
              // TODO expected hash on target
              // TODO .dryRun()
              // TODO .commitMeta()
              .merge();

    } catch (NessieConflictException conflict) {
      throw new RuntimeException(conflict);
    }

    return null;
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  private TableMetadata mergeIcebergTables(
      @Nonnull @jakarta.annotation.Nonnull TableMetadata tableMetadataSource,
      @Nonnull @jakarta.annotation.Nonnull TableMetadata tableMetadataTarget,
      @Nonnull @jakarta.annotation.Nonnull FileIO fileIO) {

    Preconditions.checkArgument(
        tableMetadataSource.uuid().equals(tableMetadataTarget.uuid()),
        "Iceberg tables have different Iceberg table UUIDs, source table UUID is %s, target table UUID is %s",
        tableMetadataSource.uuid(),
        tableMetadataTarget.uuid());

    long commonAncestorSnapshotId =
        commonAncestorSnapshot(tableMetadataSource, tableMetadataTarget);
    List<Snapshot> snapshotsToApply =
        collectSnapshotsToApply(tableMetadataSource, commonAncestorSnapshotId);

    TableMetadata.Builder newMetadataBuilder = TableMetadata.buildFrom(tableMetadataTarget);

    for (Snapshot snapshot : snapshotsToApply) {
      // TODO find a way to collect and merge manifest lists
      // TODO find a way to "copy" the manifest files from source snapshots to the new snapshot
      // TODO need to be careful with the "numbers" (added, existing, deleted, etc) represented in a
      //   ManifestFile
      snapshot.allManifests(fileIO);
      snapshot.dataManifests(fileIO);
      snapshot.deleteManifests(fileIO);

      snapshot.addedDataFiles(fileIO);
      snapshot.addedDeleteFiles(fileIO);
      snapshot.removedDataFiles(fileIO);
      snapshot.removedDeleteFiles(fileIO);

      // TODO seems that we have to interpret the summary here  :facepalm:
      snapshot.summary();
    }
    // TODO buiLd the new Snapshot and add it to newMetadataBuilder

    // TODO build diff of source-properties to common-ancestor-properties and apply
    MapDifference<String, String> propertiesDiff =
        Maps.difference(tableMetadataSource.properties(), tableMetadataTarget.properties());
    newMetadataBuilder.removeProperties(propertiesDiff.entriesOnlyOnRight().keySet());
    newMetadataBuilder.setProperties(propertiesDiff.entriesOnlyOnLeft());
    newMetadataBuilder.setProperties(
        propertiesDiff.entriesDiffering().entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().leftValue())));
    // TODO add missing schemas to target
    // TODO need to add all new schemas from the source or just the schemas referenced via the
    //   manifests/data-files?
    tableMetadataSource.schemas();
    // TODO add missing partition-specs to target
    // TODO need to add all new partition-specs from the source or just the partition-specs
    //   referenced via the manifests/data-files?
    tableMetadataSource.specs();
    // TODO add missing sort-orders to target
    // TODO need to add all new sort-orders from the source or just the sort-orders referenced via
    //   the manifests/data-files?
    tableMetadataSource.sortOrders();
    // TODO apply default-sort-order-id, if different
    tableMetadataSource.defaultSortOrderId();
    // TODO apply default-partition-spec-id, if different
    tableMetadataSource.defaultSpecId();

    // TODO do we need the changes from all intermediate table-metadata objects??
    tableMetadataSource.changes();

    // NOTE: we ignore Iceberg refs in Nessie
    tableMetadataSource.refs();

    // TODO probably need to "clear" the list of statistics files, because those probably do not
    // match anymore
    tableMetadataSource.statisticsFiles();

    return newMetadataBuilder.build();
  }

  private List<Snapshot> collectSnapshotsToApply(
      @Nonnull @jakarta.annotation.Nonnull TableMetadata tableMetadata,
      long untilSnapshotIdExcluding) {
    List<Snapshot> snapshots = new ArrayList<>();

    for (Snapshot snapshot = tableMetadata.currentSnapshot();
        snapshot.snapshotId() != untilSnapshotIdExcluding;
        snapshot = tableMetadata.snapshot(requireNonNull(snapshot.parentId()))) {
      snapshots.add(snapshot);
    }

    Collections.reverse(snapshots);

    return snapshots;
  }

  private long commonAncestorSnapshot(
      @Nonnull @jakarta.annotation.Nonnull TableMetadata tableMetadataSource,
      @Nonnull @jakarta.annotation.Nonnull TableMetadata tableMetadataTarget) {
    // TODO handle case when there is no current snapshot in either source or target metadata

    LongHashSet sourceSnapshotIds = new LongHashSet();
    LongHashSet targetSnapshotIds = new LongHashSet();

    Snapshot sourceSnapshot = tableMetadataSource.currentSnapshot();
    Snapshot targetSnapshot = tableMetadataTarget.currentSnapshot();

    List<Snapshot> snapshotsToApply = new ArrayList<>();

    while (sourceSnapshot != null || targetSnapshot != null) {
      if (sourceSnapshot != null) {
        snapshotsToApply.add(sourceSnapshot);
        long sourceId = sourceSnapshot.snapshotId();
        if (targetSnapshotIds.contains(sourceId)) {
          return sourceId;
        }
        sourceSnapshotIds.add(sourceId);

        sourceSnapshot = previousSnapshot(tableMetadataSource, sourceSnapshot);
      }
      if (targetSnapshot != null) {
        long targetId = targetSnapshot.snapshotId();
        if (sourceSnapshotIds.contains(targetId)) {
          return targetId;
        }
        targetSnapshotIds.add(targetId);

        targetSnapshot = previousSnapshot(tableMetadataTarget, targetSnapshot);
      }
    }

    throw new IllegalStateException(
        "Could not find a common ancestor to merge the given Iceberg tables");
  }

  private static Snapshot previousSnapshot(TableMetadata tableMetadata, Snapshot snapshot) {
    if (snapshot != null) {
      Long sourceParent = snapshot.parentId();
      snapshot = sourceParent != null ? tableMetadata.snapshot(sourceParent) : null;
    }
    return snapshot;
  }
}
