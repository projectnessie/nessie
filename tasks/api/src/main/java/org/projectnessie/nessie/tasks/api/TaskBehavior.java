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
package org.projectnessie.nessie.tasks.api;

import java.time.Clock;
import java.time.Instant;

/**
 * Defines the behavior of tasks.
 *
 * <p>This included the calculation or retry timestamps and lost-task timeouts and mapping between
 * exceptions and task state.
 */
public interface TaskBehavior {

  /**
   * Convert the task state as an exception, if the state represents an error or failure state. This
   * is used to transform a persisted {@linkplain TaskStatus#FAILURE failure status} to a Java
   * exception for local callers.
   */
  Throwable stateAsException(TaskObj obj);

  /** Retrieve the timestamp when the next running-state update shall happen. */
  Instant performRunningStateUpdateAt(Clock clock, TaskObj running);

  /**
   * Build a new {@linkplain TaskStatus#RUNNING running} task-state with "fresh" {@linkplain
   * TaskState#retryNotBefore() retry-not-before} and {@linkplain TaskState#lostNotBefore()
   * lost-not-before} timestamps.
   */
  TaskState runningTaskState(Clock clock, TaskObj running);

  /** Build a new {@linkplain TaskStatus#FAILURE failure} task-state. */
  TaskState failureTaskState(Throwable t);

  /**
   * Build a new {@linkplain TaskStatus#ERROR_RETRY error-retry} task-state with "fresh" {@linkplain
   * TaskState#retryNotBefore() retry-not-before} timestamp.
   */
  TaskState retryableErrorTaskState(Clock clock, TaskObj base, Throwable t);

  /**
   * Check whether the given exception, which has been thrown from a {@linkplain
   * TaskRequest#submitExecution() task execution}, can be retried. A retryable exception results in
   * the {@linkplain TaskStatus#ERROR_RETRY error-retry} state, non-retryable in the final
   * {@linkplain TaskStatus#FAILURE failure} state
   */
  default boolean isRetryableError(Throwable t) {
    return false;
  }

  /**
   * Create a new task-object builder for a <em>new</em> task object using the given task-request.
   */
  TaskObj.Builder newObjBuilder(TaskRequest taskRequest);

  /**
   * Create a new task-object builder initialized with the current state of the given base object.
   */
  TaskObj.Builder newObjBuilder(TaskObj base);
}
