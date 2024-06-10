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
package org.projectnessie.nessie.tasks.service.impl;

import org.projectnessie.nessie.tasks.api.TaskRequest;
import org.projectnessie.nessie.tasks.api.TaskStatus;
import org.projectnessie.nessie.tasks.api.Tasks;

/** Used to implement metrics (counters) and to verify interactions in tests. */
public interface TaskServiceMetrics {

  /** A new per-task singleton has been created. */
  void startNewTaskController();

  /** Task attempt iteration. */
  void taskAttempt();

  /** Final, successful task result returned. */
  void taskAttemptFinalSuccess();

  /** Final, failure task result returned. */
  void taskAttemptFinalFailure();

  /** Task attempt detected that task is running in another process. */
  void taskAttemptRunning();

  /** Task attempt detected that task ran into a retryable error. */
  void taskAttemptErrorRetry();

  void taskAttemptRecover();

  /** New task object is being created in the database. */
  void taskCreation();

  /** New task object not created due to conditional-update race. */
  void taskCreationRace();

  /** <em>Unhandled error</em> while storing new task object. */
  void taskCreationUnhandled();

  /** <em>Unhandled error</em> during task attempt. */
  void taskAttemptUnhandled();

  /** Task successfully updated with retry information. */
  void taskRetryStateChangeSucceeded();

  /** Task updated with retry information failed due to conditional-update race. */
  void taskRetryStateChangeRace();

  /** Starting task execution locally. */
  void taskExecution();

  /** Local task execution finished. */
  void taskExecutionFinished();

  /**
   * Local task execution finished with successful result, state successfully updated in database.
   */
  void taskExecutionResult();

  /**
   * Local task execution finished with successful result, failed to update state in database due to
   * conditional-update race.
   */
  void taskExecutionResultRace();

  /** Local task execution finished with retryable error, state successfully updated in database. */
  void taskExecutionRetryableError();

  /** Local task execution finished with failure result, state successfully updated in database. */
  void taskExecutionFailure();

  /**
   * Local task execution finished with failure result, failed to update state in database due to
   * conditional-update race.
   */
  void taskExecutionFailureRace();

  /** <em>Unhandled error</em> during task execution. */
  void taskExecutionUnhandled();

  /** A lost-task has been detected. */
  void taskLossDetected();

  /** Lost task successfully reassigned. */
  void taskLostReassigned();

  /** Lost task not reassigned (race with other instance). */
  void taskLostReassignRace();

  /** Starting running-state update with fresh values. */
  void taskUpdateRunningState();

  /** Running-state update with fresh values succeeded. */
  void taskRunningStateUpdated();

  /** Running-state update with fresh values ran into race with another instance. */
  void taskRunningStateUpdateRace();

  /** Running-state update cancelled, because task is no longer in running-state. */
  void taskRunningStateUpdateNoLongerRunning();

  /**
   * Task object requested for {@link Tasks#submit(TaskRequest)} has the final {@link
   * TaskStatus#FAILURE FAILURE} state and is returned immediately.
   */
  void taskHasFinalFailure();

  /**
   * Task object requested for {@link Tasks#submit(TaskRequest)} has the final {@link
   * TaskStatus#SUCCESS SUCCESS} state and is returned immediately.
   */
  void taskHasFinalSuccess();
}
