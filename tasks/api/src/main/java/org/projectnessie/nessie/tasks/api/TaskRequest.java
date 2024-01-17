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

import java.util.concurrent.CompletionStage;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;

/** Base interface for value objects that identify a task's input parameters. */
public interface TaskRequest {
  /** Declares the {@linkplain ObjType object type} for this request. */
  ObjType objType();

  /**
   * Globally identifies the task request across all {@linkplain ObjType object types}).
   *
   * <p>Implementations must derive the ID from the task type and the task parameters.
   */
  ObjId objId();

  TaskBehavior behavior();

  /**
   * Start execution of the task, this function must not block and/or wait for the task execution to
   * finish.
   */
  CompletionStage<TaskObj.Builder> submitExecution();
}
