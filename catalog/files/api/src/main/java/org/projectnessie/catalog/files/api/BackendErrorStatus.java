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

import static org.projectnessie.catalog.files.api.BackendErrorCode.FORBIDDEN;
import static org.projectnessie.catalog.files.api.BackendErrorCode.NOT_FOUND;
import static org.projectnessie.catalog.files.api.BackendErrorCode.UNAUTHORIZED;
import static org.projectnessie.catalog.files.api.BackendErrorCode.UNKNOWN;

import java.time.Instant;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
public interface BackendErrorStatus {
  BackendErrorCode statusCode();

  boolean isRetryable();

  Instant reattemptAfter();

  Throwable cause();

  default BackendErrorStatus withReattemptAfter(Instant reattemptAfter) {
    return ImmutableBackendErrorStatus.builder().from(this).reattemptAfter(reattemptAfter).build();
  }

  static BackendErrorStatus of(BackendErrorCode statusCode, boolean retryable, Throwable cause) {
    // Use Instant.EPOCH for `reattemptAfter` here. The reattempt delay may be re-assigned later.
    return ImmutableBackendErrorStatus.of(statusCode, retryable, Instant.EPOCH, cause);
  }

  static BackendErrorStatus fromHttpStatusCode(int httpStatusCode, Throwable cause) {
    switch (httpStatusCode) {
      case 401:
        return BackendErrorStatus.of(UNAUTHORIZED, false, cause);
      case 403:
        return BackendErrorStatus.of(FORBIDDEN, false, cause);
      case 404:
        return BackendErrorStatus.of(NOT_FOUND, false, cause);
      default:
        return BackendErrorStatus.of(UNKNOWN, false, cause);
    }
  }
}
