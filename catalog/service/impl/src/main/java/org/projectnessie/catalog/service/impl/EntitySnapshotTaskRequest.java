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

import static org.projectnessie.catalog.service.objtypes.EntitySnapshotObj.OBJ_TYPE;

import jakarta.annotation.Nullable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import org.immutables.value.Value;
import org.projectnessie.catalog.files.api.ObjectIO;
import org.projectnessie.catalog.model.snapshot.NessieEntitySnapshot;
import org.projectnessie.catalog.service.objtypes.EntitySnapshotObj;
import org.projectnessie.model.Content;
import org.projectnessie.nessie.immutables.NessieImmutable;
import org.projectnessie.nessie.tasks.api.TaskBehavior;
import org.projectnessie.nessie.tasks.api.TaskRequest;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import org.projectnessie.versioned.storage.common.persist.Persist;

@NessieImmutable
public interface EntitySnapshotTaskRequest
    extends TaskRequest<EntitySnapshotObj, EntitySnapshotObj.Builder> {
  @Override
  @Value.NonAttribute
  default ObjType objType() {
    return OBJ_TYPE;
  }

  @Override
  TaskBehavior<EntitySnapshotObj, EntitySnapshotObj.Builder> behavior();

  @Override
  ObjId objId();

  @Nullable
  Content content();

  /**
   * Used for "committing API operations", holds the produced/built snapshot.
   *
   * <p>For reading API operations, this is {@code null}, indicating that the snapshot needs to be
   * imported from an object store, if it is not already imported.
   *
   * <p>For writing API operations, this is not {@code null}, indicating that the snapshot does not
   * need to be imported, only persisted.
   */
  @Value.Auxiliary
  @Nullable
  NessieEntitySnapshot<?> snapshot();

  @Value.Auxiliary
  Persist persist();

  @Value.Auxiliary
  ObjectIO objectIO();

  @Value.Auxiliary
  Executor executor();

  @Override
  @Value.NonAttribute
  default CompletionStage<EntitySnapshotObj.Builder> submitExecution() {
    return CompletableFuture.supplyAsync(
        new ImportSnapshotWorker(this)::importSnapshot, executor());
  }

  @Override
  default EntitySnapshotObj.Builder applyRequestToObjBuilder(EntitySnapshotObj.Builder builder) {
    return builder.content(content());
  }

  static EntitySnapshotTaskRequest entitySnapshotTaskRequest(
      ObjId objId,
      Content content,
      NessieEntitySnapshot<?> snapshot,
      EntitySnapshotTaskBehavior behavior,
      Persist persist,
      ObjectIO objectIO,
      Executor executor) {
    return ImmutableEntitySnapshotTaskRequest.of(
        behavior, objId, content, snapshot, persist, objectIO, executor);
  }
}
