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

import static org.projectnessie.catalog.files.api.BackendErrorCode.UNKNOWN;

import org.projectnessie.catalog.files.api.BackendErrorCode;
import org.projectnessie.nessie.tasks.api.TaskState;

public class PreviousTaskException extends RuntimeException {
  private final BackendErrorCode errorCode;

  private PreviousTaskException(BackendErrorCode errorCode, String message) {
    super(message);
    this.errorCode = errorCode;
  }

  public BackendErrorCode getErrorCode() {
    return errorCode;
  }

  public static PreviousTaskException fromTaskState(TaskState state) {
    if (state == null) {
      return new PreviousTaskException(UNKNOWN, "[unknown]");
    }

    if (state.errorCode() == null) {
      return new PreviousTaskException(UNKNOWN, state.message());
    }

    try {
      BackendErrorCode errorCode = BackendErrorCode.valueOf(state.errorCode());
      return new PreviousTaskException(errorCode, state.message());
    } catch (IllegalArgumentException e) {
      return new PreviousTaskException(UNKNOWN, state.message());
    }
  }
}
