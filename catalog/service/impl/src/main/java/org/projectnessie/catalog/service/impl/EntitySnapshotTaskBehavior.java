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

import static java.time.temporal.ChronoUnit.MINUTES;
import static java.time.temporal.ChronoUnit.SECONDS;
import static org.projectnessie.catalog.service.impl.Util.throwableAsErrorTaskState;
import static org.projectnessie.nessie.tasks.api.TaskState.runningState;

import java.time.Clock;
import java.time.Instant;
import org.projectnessie.nessie.tasks.api.TaskBehavior;
import org.projectnessie.nessie.tasks.api.TaskState;
import org.projectnessie.versioned.storage.common.persist.ObjType;

final class EntitySnapshotTaskBehavior
    implements TaskBehavior<EntitySnapshotObj, EntitySnapshotObj.Builder> {
  static final EntitySnapshotTaskBehavior INSTANCE = new EntitySnapshotTaskBehavior();

  private EntitySnapshotTaskBehavior() {}

  @Override
  public Throwable stateAsException(EntitySnapshotObj obj) {
    throw new RuntimeException(obj.taskState().message());
  }

  @Override
  public Instant performRunningStateUpdateAt(Clock clock, EntitySnapshotObj running) {
    return clock.instant().plus(2, SECONDS);
  }

  @Override
  public TaskState runningTaskState(Clock clock, EntitySnapshotObj running) {
    Instant now = clock.instant();
    Instant retryNotBefore = now.plus(2, SECONDS);
    Instant lostNotBefore = now.plus(1, MINUTES);
    return runningState(retryNotBefore, lostNotBefore);
  }

  @Override
  public EntitySnapshotObj.Builder newObjBuilder() {
    return EntitySnapshotObj.builder();
  }

  @Override
  public ObjType objType() {
    return EntitySnapshotObj.OBJ_TYPE;
  }

  @Override
  public TaskState asErrorTaskState(Clock clock, EntitySnapshotObj base, Throwable t) {
    return throwableAsErrorTaskState(t);
  }
}
