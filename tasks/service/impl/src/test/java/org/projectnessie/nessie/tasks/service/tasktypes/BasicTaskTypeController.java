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
import java.util.concurrent.CompletionStage;
import org.projectnessie.nessie.tasks.api.TaskObj;
import org.projectnessie.nessie.tasks.api.TaskRequest;
import org.projectnessie.nessie.tasks.api.TaskState;
import org.projectnessie.nessie.tasks.api.TaskType;
import org.projectnessie.nessie.tasks.async.TasksAsync;
import org.projectnessie.nessie.tasks.service.spi.TaskTypeController;

public class BasicTaskTypeController implements TaskTypeController {

  public static final TemporalAmount FRESH_RUNNING_RETRY_NOT_BEFORE =
      Duration.of(2, ChronoUnit.SECONDS);
  public static final TemporalAmount FRESH_LOST_RETRY_NOT_BEFORE =
      Duration.of(1, ChronoUnit.MINUTES);
  public static final TemporalAmount RETRYABLE_ERROR_NOT_BEFORE =
      Duration.of(5, ChronoUnit.SECONDS);
  public static final TemporalAmount RUNNING_UPDATE_INTERVAL = Duration.of(250, ChronoUnit.MILLIS);

  public BasicTaskTypeController() {}

  @Override
  public TaskType taskType() {
    return BasicTaskRequest.TASK_TYPE;
  }

  @Override
  public CompletionStage<TaskObj.Builder> submitExecution(
      TasksAsync tasksAsync, TaskRequest taskRequest) {
    return ((BasicTaskRequest) taskRequest).taskCompletionStageSupplier().get();
  }

  @Override
  public Throwable stateAsException(TaskObj obj) {
    return new Exception(obj.taskState().message());
  }

  @Override
  public Instant performRunningStateUpdateAt(Clock clock, TaskObj running) {
    return clock.instant().plus(RUNNING_UPDATE_INTERVAL);
  }

  @Override
  public boolean isRetryableError(Throwable t) {
    return t instanceof RetryableException;
  }

  @Override
  public TaskObj.Builder newObjBuilder(TaskObj base) {
    return ImmutableBasicTaskObj.builder().from(base);
  }

  @Override
  public TaskObj.Builder newObjBuilder(TaskRequest taskRequest) {
    BasicTaskRequest basicTaskRequest = (BasicTaskRequest) taskRequest;
    return ImmutableBasicTaskObj.builder().taskParameter(basicTaskRequest.taskParameter());
  }

  @Override
  public TaskState runningTaskState(Clock clock, TaskObj running) {
    return runningState(freshRunningRetryNotBefore(clock), freshLostRetryNotBefore(clock));
  }

  @Override
  public TaskState failureTaskState(Throwable t) {
    return failureState(t.getMessage());
  }

  @Override
  public TaskState retryableErrorTaskState(Clock clock, TaskObj base, Throwable t) {
    return retryableErrorState(retryableErrorNotBefore(clock), t.getMessage());
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
