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
import org.projectnessie.versioned.storage.common.persist.ObjType;

/**
 * Defines the behavior of tasks.
 *
 * <p>This included the calculation or retry timestamps and lost-task timeouts and mapping between
 * exceptions and task state.
 */
public interface TaskBehavior<T extends TaskObj, B extends TaskObj.Builder> {

  /**
   * Convert the task state as an exception, if the state represents an error or failure state. This
   * is used to transform a persisted {@linkplain TaskStatus#FAILURE failure status} to a Java
   * exception for local callers.
   */
  Throwable stateAsException(T obj);

  /**
   * Retrieve the timestamp when the next running-state update that refreshes the {@link
   * TaskState#retryNotBefore() retry-not-before} and {@link TaskState#lostNotBefore()}
   * lost-not-before} timestamps in the database, shall happen. The returned timestamp must be
   * earlier than any of these "not-before" timestamps and defines when the update will be
   * scheduled.
   */
  Instant performRunningStateUpdateAt(Clock clock, T running);

  /**
   * Build a new {@linkplain TaskStatus#RUNNING running} task-state with "fresh" {@linkplain
   * TaskState#retryNotBefore() retry-not-before} and {@linkplain TaskState#lostNotBefore()
   * lost-not-before} timestamps.
   */
  TaskState runningTaskState(Clock clock, T running);

  /**
   * Called when the task execution resulted in an exception, Build a new {@linkplain
   * TaskStatus#ERROR_RETRY error-retry}, with "fresh" {@linkplain TaskState#retryNotBefore()
   * retry-not-before} timestamp, or {@linkplain TaskStatus#FAILURE failure} task-state.
   */
  TaskState asErrorTaskState(Clock clock, T base, Throwable t);

  /** Create a new, empty task-object builder. */
  B newObjBuilder();

  ObjType objType();
}
