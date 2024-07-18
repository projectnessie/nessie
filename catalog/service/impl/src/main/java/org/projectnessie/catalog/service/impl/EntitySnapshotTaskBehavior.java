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
import org.projectnessie.catalog.files.api.BackendExceptionMapper;
import org.projectnessie.catalog.service.objtypes.EntitySnapshotObj;
import org.projectnessie.nessie.tasks.api.TaskBehavior;
import org.projectnessie.nessie.tasks.api.TaskState;
import org.projectnessie.versioned.storage.common.persist.ObjType;

final class EntitySnapshotTaskBehavior
    implements TaskBehavior<EntitySnapshotObj, EntitySnapshotObj.Builder> {
  private final BackendExceptionMapper exceptionMapper;
  private final Duration retryAfterThrottled;

  EntitySnapshotTaskBehavior(BackendExceptionMapper exceptionMapper, Duration retryAfterThrottled) {
    this.exceptionMapper = exceptionMapper;
    this.retryAfterThrottled = retryAfterThrottled;
  }

  @Override
  public Throwable stateAsException(EntitySnapshotObj obj) {
    throw PreviousTaskException.fromTaskState(obj.taskState());
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

  private Instant retryAfter(Clock clock) {
    return clock.instant().plus(retryAfterThrottled);
  }

  @Override
  public TaskState asErrorTaskState(Clock clock, EntitySnapshotObj base, Throwable t) {
    return exceptionMapper
        .analyze(t)
        .map(
            status -> {
              if (status.statusCode().isRetryable()) {
                return retryableErrorState(
                    retryAfter(clock), t.toString(), status.statusCode().name());
              } else {
                return failureState(t.toString(), status.statusCode().name());
              }
            })
        .orElseGet(() -> failureState(t.toString(), null));
  }
}
