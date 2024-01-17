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

/**
 * Central API interface to execute long-running I/O intensive computations, ensuring that no more
 * than one computation for the same input happens.
 */
public interface Tasks {
  /**
   * Issue a request for a task described by {@link TaskRequest}. Tasks for the same request are
   * executed only once. The returned {@link CompletionStage} finishes when the task enters a
   * {@linkplain TaskStatus#isFinal() final status} ({@linkplain TaskStatus#SUCCESS SUCCESS} or
   * {@linkplain TaskStatus#FAILURE FAILURE}).
   */
  <T extends TaskObj, B extends TaskObj.Builder> CompletionStage<T> submit(
      TaskRequest<T, B> taskRequest);
}
