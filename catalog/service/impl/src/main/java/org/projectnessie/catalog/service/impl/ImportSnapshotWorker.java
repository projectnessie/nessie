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
package org.projectnessie.catalog.service.impl;

import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.icebergTableSnapshotToNessie;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.icebergViewSnapshotToNessie;
import static org.projectnessie.catalog.service.files.MetadataUtil.readMetadata;
import static org.projectnessie.catalog.service.impl.Util.nessieIdToObjId;
import static org.projectnessie.catalog.service.impl.Util.objIdToNessieId;
import static org.projectnessie.catalog.service.objtypes.EntityObj.entityObjIdForContent;
import static org.projectnessie.nessie.tasks.api.TaskState.successState;
import static org.projectnessie.versioned.storage.common.persist.ObjId.randomObjId;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.LongSupplier;
import org.projectnessie.catalog.formats.iceberg.manifest.IcebergFileFormat;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergJson;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergTableMetadata;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergViewMetadata;
import org.projectnessie.catalog.model.NessieEntity;
import org.projectnessie.catalog.model.NessieTable;
import org.projectnessie.catalog.model.NessieView;
import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.catalog.model.snapshot.NessieTableSnapshot;
import org.projectnessie.catalog.model.snapshot.NessieViewSnapshot;
import org.projectnessie.catalog.model.snapshot.TableFormat;
import org.projectnessie.catalog.service.objtypes.EntityObj;
import org.projectnessie.catalog.service.objtypes.EntitySnapshotObj;
import org.projectnessie.model.Content;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;
import org.projectnessie.storage.uri.StorageUri;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ImportSnapshotWorker {
  private static final Logger LOGGER = LoggerFactory.getLogger(ImportSnapshotWorker.class);

  private final EntitySnapshotTaskRequest taskRequest;

  ImportSnapshotWorker(EntitySnapshotTaskRequest taskRequest) {
    this.taskRequest = taskRequest;
  }

  EntitySnapshotObj.Builder importSnapshot() {
    Content content = taskRequest.content();
    if (content instanceof IcebergTable) {
      return importIcebergTable(
          (IcebergTable) content, (NessieTableSnapshot) taskRequest.snapshot());
    }
    if (content instanceof IcebergView) {
      return importIcebergView((IcebergView) content, (NessieViewSnapshot) taskRequest.snapshot());
    }

    throw new UnsupportedOperationException("Unsupported Nessie content type " + content.getType());
  }

  private EntitySnapshotObj.Builder importIcebergTable(
      IcebergTable content, NessieTableSnapshot snapshot) {
    NessieId snapshotId = objIdToNessieId(taskRequest.objId());

    LOGGER.debug(
        "{} Iceberg table metadata from object store for snapshot ID {} from {}",
        snapshot == null ? "Fetching" : "Storing",
        taskRequest.objId(),
        content.getMetadataLocation());

    ObjId entityObjId = entityObjIdForContent(content);

    // snapshot!=null means that we already have the snapshot (via a committing operation - like a
    // table update) and do not need to import it but can just store it.
    if (snapshot == null) {
      StorageUri metadataLocation = StorageUri.of(content.getMetadataLocation());
      NessieTable table;
      IcebergTableMetadata tableMetadata;
      try {
        tableMetadata = icebergMetadata(metadataLocation, IcebergTableMetadata.class);
        table = entityObjForContent(content, tableMetadata, entityObjId);
      } catch (Exception e) {
        throw new RuntimeException(
            "Failed to read table metadata from " + content.getMetadataLocation(), e);
      }

      snapshot =
          icebergTableSnapshotToNessie(
              snapshotId,
              null,
              table,
              tableMetadata,
              snap -> {
                if (snap.manifestList() != null) {
                  // If the Iceberg snapshot references a manifest-list, use it. This is the regular
                  // case.
                  return snap.manifestList();
                }

                if (snap.manifests() == null || snap.manifests().isEmpty()) {
                  return null;
                }

                // If the Iceberg snapshot references manifest files and no manifest list, which can
                // happen for old Iceberg format v1 snapshots, generate a manifest list location,
                // which is then generated by the manifest-group import-worker.
                // In other words, this code "prepares" the table snapshot with a manifest-list
                // location for Iceberg snapshots that have the field `manifests` set but
                // `manifest-list` not set, as possible for Iceberg spec v1. See
                // https://iceberg.apache.org/spec/#snapshots
                // It seems impossible to have a snapshot from Iceberg that has both `manifests` and
                // `manifest-list` populated, see
                // https://github.com/apache/iceberg/pull/21/files#diff-f32244ab465f79a1dbda5582bf74404b018d044de022e047e1e9f4f9bbf32452R52-R63,
                // which the same logic as today, see
                // https://github.com/apache/iceberg/blob/8109e420e6d563a73e17ff23c321f1a48a2b976d/core/src/main/java/org/apache/iceberg/SnapshotParser.java#L79-L89.

                String listFile =
                    String.format(
                        "snap-%d-%d-%s%s",
                        snap.snapshotId(),
                        // attempt (we use a random number)
                        ThreadLocalRandom.current().nextLong(1000000, Long.MAX_VALUE),
                        // commit-id (we use a random one)
                        UUID.randomUUID(),
                        IcebergFileFormat.AVRO.fileExtension());

                return metadataLocation.resolve(listFile).toString();
              });
    }

    return EntitySnapshotObj.builder()
        .id(nessieIdToObjId(snapshotId))
        .entity(entityObjId)
        .snapshot(snapshot)
        .content(content)
        .taskState(successState());
  }

  private EntitySnapshotObj.Builder importIcebergView(
      IcebergView content, NessieViewSnapshot snapshot) {
    NessieId snapshotId = objIdToNessieId(taskRequest.objId());

    LOGGER.debug(
        "{} Iceberg view metadata from object store for snapshot ID {} from {}",
        snapshot == null ? "Fetching" : "Storing",
        taskRequest.objId(),
        content.getMetadataLocation());

    ObjId entityObjId = entityObjIdForContent(content);

    // snapshot!=null means that we already have the snapshot (via a committing operation - like a
    // table update) and do not need to import it but can just store it.
    if (snapshot == null) {
      NessieView view;
      IcebergViewMetadata viewMetadata;
      StorageUri metadataLocation = StorageUri.of(content.getMetadataLocation());
      try {
        viewMetadata = icebergMetadata(metadataLocation, IcebergViewMetadata.class);
        view =
            entityObjForContent(
                content,
                viewMetadata,
                entityObjId,
                () ->
                    viewMetadata.versions().stream()
                        .filter(v -> v.versionId() == viewMetadata.currentVersionId())
                        .findFirst()
                        .orElseThrow(
                            () ->
                                new IllegalStateException(
                                    "Iceberg view has no version element with the id for the current-version-ID"))
                        .timestampMs());
      } catch (Exception e) {
        throw new RuntimeException(
            "Failed to read view metadata from " + content.getMetadataLocation(), e);
      }

      snapshot = icebergViewSnapshotToNessie(snapshotId, null, view, viewMetadata);
    }

    return EntitySnapshotObj.builder()
        .id(nessieIdToObjId(snapshotId))
        .entity(entityObjId)
        .snapshot(snapshot)
        .content(content)
        .taskState(successState());
  }

  private NessieTable entityObjForContent(
      IcebergTable content, IcebergTableMetadata tableMetadata, ObjId entityObjId)
      throws ObjTooLargeException {
    NessieTable table;
    try {
      EntityObj entity = (EntityObj) taskRequest.persist().fetchObj(entityObjId);
      table = (NessieTable) entity.entity();
    } catch (ObjNotFoundException nf) {

      NessieTable.Builder tableBuilder =
          NessieTable.builder()
              .createdTimestamp(Instant.ofEpochMilli(tableMetadata.lastUpdatedMs()))
              .tableFormat(TableFormat.ICEBERG)
              .icebergUuid(tableMetadata.tableUuid())
              .nessieContentId(content.getId());

      table = tableBuilder.build();

      if (taskRequest.persist().storeObj(buildEntityObj(entityObjId, table))) {
        LOGGER.debug("Persisted new entity object for content ID {}", content.getId());
      }
    }
    return table;
  }

  private NessieView entityObjForContent(
      IcebergView content,
      IcebergViewMetadata viewMetadata,
      ObjId entityObjId,
      LongSupplier lastUpdatedMs)
      throws ObjTooLargeException {
    NessieView view;
    try {
      EntityObj entity = (EntityObj) taskRequest.persist().fetchObj(entityObjId);
      view = (NessieView) entity.entity();
    } catch (ObjNotFoundException nf) {

      NessieView.Builder viewBuilder =
          NessieView.builder()
              .createdTimestamp(Instant.ofEpochMilli(lastUpdatedMs.getAsLong()))
              .tableFormat(TableFormat.ICEBERG)
              .icebergUuid(viewMetadata.viewUuid())
              .nessieContentId(content.getId());

      view = viewBuilder.build();

      if (taskRequest.persist().storeObj(buildEntityObj(entityObjId, view))) {
        LOGGER.debug("Persisted new entity object for content ID {}", content.getId());
      }
    }
    return view;
  }

  private static EntityObj buildEntityObj(ObjId entityObjId, NessieEntity entity) {
    return EntityObj.builder()
        .id(entityObjId)
        .entity(entity)
        .versionToken(randomObjId().toString())
        .build();
  }

  private <T> T icebergMetadata(StorageUri metadataLocation, Class<? extends T> metadataType)
      throws IOException {
    InputStream input = readMetadata(taskRequest.objectIO(), metadataLocation);
    return IcebergJson.objectMapper().readValue(input, metadataType);
  }
}
