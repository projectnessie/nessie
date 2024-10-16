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
package org.projectnessie.catalog.service.impl;

import static java.util.concurrent.CompletableFuture.completedStage;
import static org.projectnessie.catalog.service.impl.EntitySnapshotTaskRequest.entitySnapshotTaskRequest;
import static org.projectnessie.catalog.service.impl.Util.nessieIdToObjId;

import jakarta.annotation.Nonnull;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import org.projectnessie.catalog.files.api.ObjectIO;
import org.projectnessie.catalog.model.snapshot.NessieEntitySnapshot;
import org.projectnessie.catalog.model.snapshot.NessieTableSnapshot;
import org.projectnessie.catalog.model.snapshot.NessieViewSnapshot;
import org.projectnessie.catalog.service.objtypes.EntitySnapshotObj;
import org.projectnessie.model.Content;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;
import org.projectnessie.nessie.tasks.api.TasksService;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO give this class a better name
public class IcebergStuff {
  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergStuff.class);

  private final ObjectIO objectIO;
  private final Persist persist;
  private final TasksService tasksService;
  private final EntitySnapshotTaskBehavior snapshotTaskBehavior;
  private final Executor executor;

  IcebergStuff(
      ObjectIO objectIO,
      Persist persist,
      TasksService tasksService,
      EntitySnapshotTaskBehavior snapshotTaskBehavior,
      Executor executor) {
    this.objectIO = objectIO;
    this.persist = persist;
    this.tasksService = tasksService;
    this.snapshotTaskBehavior = snapshotTaskBehavior;
    this.executor = executor;
  }

  /**
   * Retrieve the Nessie table or view snapshot for an {@linkplain IcebergTable Iceberg table} or
   * {@linkplain IcebergView Iceberg view} snapshot, either from the Nessie Data Catalog database or
   * imported from the data lake into the Nessie Data Catalog's database.
   */
  public <S extends NessieEntitySnapshot<?>> CompletionStage<S> retrieveIcebergSnapshot(
      ObjId snapshotId, Content content) {
    EntitySnapshotTaskRequest snapshotTaskRequest =
        entitySnapshotTaskRequest(
            snapshotId, content, null, snapshotTaskBehavior, persist, objectIO, executor);
    return triggerIcebergSnapshot(snapshotTaskRequest);
  }

  @SuppressWarnings("unchecked")
  @Nonnull
  private <S extends NessieEntitySnapshot<?>> CompletionStage<S> triggerIcebergSnapshot(
      EntitySnapshotTaskRequest snapshotTaskRequest) {
    // TODO Handle hash-collision - when entity-snapshot refers to a different(!) snapshot
    return tasksService
        .forPersist(persist)
        .submit(snapshotTaskRequest)
        .thenCompose(
            snapshotObj -> {
              NessieEntitySnapshot<?> entitySnapshot = snapshotObj.snapshot();
              if (entitySnapshot instanceof NessieTableSnapshot) {
                return completedStage((S) mapToTableSnapshot(snapshotObj));
              }
              if (entitySnapshot instanceof NessieViewSnapshot) {
                return completedStage((S) mapToViewSnapshot(snapshotObj));
              }
              throw new IllegalArgumentException(
                  "Unsupported snapshot type: " + snapshotObj.getClass().getSimpleName());
            });
  }

  @Nonnull
  public <S extends NessieEntitySnapshot<?>> CompletionStage<S> storeSnapshot(
      S snapshot, Content content) {
    EntitySnapshotTaskRequest snapshotTaskRequest =
        entitySnapshotTaskRequest(
            nessieIdToObjId(snapshot.id()),
            content,
            snapshot,
            snapshotTaskBehavior,
            persist,
            objectIO,
            executor);
    return triggerIcebergSnapshot(snapshotTaskRequest);
  }

  /** Fetch requested metadata from the database, the snapshot already exists. */
  NessieTableSnapshot mapToTableSnapshot(@Nonnull EntitySnapshotObj snapshotObj) {
    LOGGER.debug("Fetching table snapshot from database for snapshot ID {}", snapshotObj.id());

    NessieTableSnapshot tableSnapshot = (NessieTableSnapshot) snapshotObj.snapshot();
    NessieTableSnapshot.Builder snapshotBuilder = NessieTableSnapshot.builder().from(tableSnapshot);

    NessieTableSnapshot snapshot;
    snapshot = snapshotBuilder.build();
    LOGGER.debug(
        "Loaded table snapshot with {} schemas and {} partition definitions",
        snapshot.schemas().size(),
        snapshot.partitionDefinitions().size());
    return snapshot;
  }

  /** Fetch requested metadata from the database, the snapshot already exists. */
  NessieViewSnapshot mapToViewSnapshot(@Nonnull EntitySnapshotObj snapshotObj) {
    LOGGER.debug("Fetching view snapshot from database for snapshot ID {}", snapshotObj.id());

    NessieViewSnapshot viewSnapshot = (NessieViewSnapshot) snapshotObj.snapshot();
    NessieViewSnapshot.Builder snapshotBuilder = NessieViewSnapshot.builder().from(viewSnapshot);

    NessieViewSnapshot snapshot;
    snapshot = snapshotBuilder.build();
    LOGGER.debug("Loaded view snapshot with {} schemas", snapshot.schemas().size());
    return snapshot;
  }
}
