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
package org.projectnessie.nessie.tasks.service.tasktypes;

import static org.projectnessie.nessie.tasks.api.TaskState.failureState;
import static org.projectnessie.nessie.tasks.api.TaskState.retryableErrorState;
import static org.projectnessie.nessie.tasks.api.TaskState.runningState;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import org.projectnessie.nessie.tasks.api.TaskBehavior;
import org.projectnessie.nessie.tasks.api.TaskState;
import org.projectnessie.versioned.storage.common.persist.ObjType;

public class BasicTaskBehavior implements TaskBehavior<BasicTaskObj, BasicTaskObj.Builder> {

  public static final TemporalAmount FRESH_RUNNING_RETRY_NOT_BEFORE =
      Duration.of(2, ChronoUnit.SECONDS);
  public static final TemporalAmount FRESH_LOST_RETRY_NOT_BEFORE =
      Duration.of(1, ChronoUnit.MINUTES);
  public static final TemporalAmount RETRYABLE_ERROR_NOT_BEFORE =
      Duration.of(5, ChronoUnit.SECONDS);
  public static final TemporalAmount RUNNING_UPDATE_INTERVAL = Duration.of(250, ChronoUnit.MILLIS);

  public static final BasicTaskBehavior INSTANCE = new BasicTaskBehavior();

  public BasicTaskBehavior() {}

  @Override
  public Throwable stateAsException(BasicTaskObj obj) {
    return new Exception(obj.taskState().message());
  }

  @Override
  public Instant performRunningStateUpdateAt(Clock clock, BasicTaskObj running) {
    return clock.instant().plus(RUNNING_UPDATE_INTERVAL);
  }

  @Override
  public TaskState asErrorTaskState(Clock clock, BasicTaskObj base, Throwable t) {
    if (t instanceof RetryableException) {
      return retryableErrorState(retryableErrorNotBefore(clock), t.getMessage(), "test");
    }
    return failureState(t.toString(), null);
  }

  @Override
  public BasicTaskObj.Builder newObjBuilder() {
    return ImmutableBasicTaskObj.builder();
  }

  @Override
  public TaskState runningTaskState(Clock clock, BasicTaskObj running) {
    return runningState(freshRunningRetryNotBefore(clock), freshLostRetryNotBefore(clock));
  }

  @Override
  public ObjType objType() {
    return BasicTaskObj.TYPE;
  }

  private Instant freshRunningRetryNotBefore(Clock clock) {
    return clock.instant().plus(FRESH_RUNNING_RETRY_NOT_BEFORE);
  }

  private Instant freshLostRetryNotBefore(Clock clock) {
    return clock.instant().plus(FRESH_LOST_RETRY_NOT_BEFORE);
  }

  private Instant retryableErrorNotBefore(Clock clock) {
    return clock.instant().plus(RETRYABLE_ERROR_NOT_BEFORE);
  }
}
