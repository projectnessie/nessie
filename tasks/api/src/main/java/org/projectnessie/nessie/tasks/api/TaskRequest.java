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
public interface TaskRequest<T extends TaskObj, B extends TaskObj.Builder> {
  /** Declares the {@linkplain ObjType object type} for this request. */
  ObjType objType();

  /**
   * Globally identifies the task request across all {@linkplain ObjType object types}).
   *
   * <p>Implementations must derive the ID from the task type and the task parameters.
   */
  ObjId objId();

  TaskBehavior<T, B> behavior();

  /**
   * Start execution of the task, this function must not block and/or wait for the task execution to
   * finish.
   *
   * <p>The implementation is responsible to choose the right scheduling implementation. For
   * example: tasks that are supposed to run very long, like 5 seconds or more, must not use a <a
   * href="https://javadoc.io/static/io.vertx/vertx-core/4.5.1/io/vertx/core/Vertx.html#executeBlocking-java.util.concurrent.Callable-boolean-">Vert.X's
   * {@code executeBlocking()}</a>.
   */
  CompletionStage<B> submitExecution();

  /** Applies parameters from this request to the object builder. */
  default B applyRequestToObjBuilder(B builder) {
    return builder;
  }
}
