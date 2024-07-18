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
package org.projectnessie.catalog.files.api;

public enum BackendErrorCode {
  /**
   * Treat unknown errors as non-retryable to prevent indefinite retry cycles when the server is not
   * able to determine a specific failure reason. This may be changed in the future when task
   * deadlines are supported.
   */
  UNKNOWN(false),
  THROTTLED(true),
  NOT_FOUND(false),
  UNAUTHORIZED(false),
  FORBIDDEN(false),
  BAD_REQUEST(false),
  NESSIE_ERROR(false),
  ICEBERG_ERROR(false),
  ;

  private final boolean retryable;

  BackendErrorCode(boolean retryable) {
    this.retryable = retryable;
  }

  public boolean isRetryable() {
    return retryable;
  }
}
