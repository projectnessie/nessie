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
import static org.projectnessie.nessie.tasks.api.TaskState.failureState;
import static org.projectnessie.nessie.tasks.api.TaskState.retryableErrorState;
import static org.projectnessie.nessie.tasks.api.TaskState.runningState;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import javax.annotation.Nullable;
import org.projectnessie.catalog.files.api.ObjectIOExceptionMapper;
import org.projectnessie.catalog.service.objtypes.EntitySnapshotObj;
import org.projectnessie.nessie.tasks.api.TaskBehavior;
import org.projectnessie.nessie.tasks.api.TaskState;
import org.projectnessie.versioned.storage.common.persist.ObjType;

public final class EntitySnapshotTaskBehavior
    implements TaskBehavior<EntitySnapshotObj, EntitySnapshotObj.Builder> {
  private final ObjectIOExceptionMapper exceptionMapper;
  private final Duration taskTimeout;

  public EntitySnapshotTaskBehavior(ObjectIOExceptionMapper exceptionMapper, Duration taskTimeout) {
    this.exceptionMapper = exceptionMapper;
    this.taskTimeout = taskTimeout;
  }

  @Override
  public Throwable stateAsException(EntitySnapshotObj obj) {
    throw new RuntimeException(obj.taskState().message());
  }

  @Override
  public Instant performRunningStateUpdateAt(Clock clock, EntitySnapshotObj running) {
    return clock.instant().plus(2, SECONDS);
  }

  private TaskState taskState(@Nullable EntitySnapshotObj obj) {
    return obj == null ? null : obj.taskState();
  }

  @Override
  public TaskState runningTaskState(Clock clock, EntitySnapshotObj running) {
    Instant now = clock.instant();
    Instant retryNotBefore = now.plus(2, SECONDS);
    Instant lostNotBefore = now.plus(1, MINUTES);
    return runningState(
        retryNotBefore, lostNotBefore, taskState(running), () -> now.plus(taskTimeout));
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
  public TaskState timedOutTaskState(Clock clock, EntitySnapshotObj base, Throwable t) {
    // Allow immediate retry for unexpected errors (usually requires re-submitting the task).
    return failureState(clock.instant(), t.toString());
  }

  @Override
  public TaskState asErrorTaskState(Clock clock, EntitySnapshotObj base, Throwable t) {
    return exceptionMapper
        .analyze(t)
        .map(
            status -> {
              if (status.isRetryable()) {
                return retryableErrorState(
                    status.reattemptAfter(),
                    t.toString(),
                    base.taskState(),
                    () -> clock.instant().plus(taskTimeout));
              } else {
                return failureState(status.reattemptAfter(), t.toString());
              }
            })
        // Use the deadline as "retry not before" for unexpected errors. This is only to allow
        // re-attempting those tasks in principle, while we cannot have a more reasonable reattempt
        // timeout. Introducing a separate config for that looks like an overkill.
        .orElseGet(() -> failureState(clock.instant().plus(taskTimeout), t.toString()));
  }
}
