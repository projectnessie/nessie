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

public enum TaskStatus {
  /** The task is currently running. */
  RUNNING(false, false, true),
  /** The task has finished successfully. */
  SUCCESS(true, false, false),
  /** The task has failed and can be retried. */
  ERROR_RETRY(false, true, true),
  /** The task has failed without a way to recover. */
  FAILURE(true, true, false),
  ;

  private final boolean finalState;
  private final boolean error;
  private final boolean retryable;

  TaskStatus(boolean finalState, boolean error, boolean retryable) {
    this.finalState = finalState;
    this.error = error;
    this.retryable = retryable;
  }

  /**
   * Whether the status represents a final state, which will never be updated.
   *
   * <p>Final states are never retried and can be unconditionally cached.
   */
  public boolean isFinal() {
    return finalState;
  }

  /** Whether the status represents an error. */
  public boolean isError() {
    return error;
  }

  public boolean isRetryable() {
    return retryable;
  }
}
