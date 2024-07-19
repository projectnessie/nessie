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

import static org.projectnessie.catalog.files.api.BackendErrorCode.BAD_REQUEST;
import static org.projectnessie.catalog.files.api.BackendErrorCode.FORBIDDEN;
import static org.projectnessie.catalog.files.api.BackendErrorCode.NOT_FOUND;
import static org.projectnessie.catalog.files.api.BackendErrorCode.THROTTLED;
import static org.projectnessie.catalog.files.api.BackendErrorCode.UNAUTHORIZED;
import static org.projectnessie.catalog.files.api.BackendErrorCode.UNKNOWN;

import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
public interface BackendErrorStatus {
  BackendErrorCode statusCode();

  Throwable cause();

  static BackendErrorStatus of(BackendErrorCode statusCode, Throwable cause) {
    return ImmutableBackendErrorStatus.of(statusCode, cause);
  }

  static BackendErrorStatus fromHttpStatusCode(int httpStatusCode, Throwable cause) {
    switch (httpStatusCode) {
      case 400:
        return BackendErrorStatus.of(BAD_REQUEST, cause);
      case 401:
        return BackendErrorStatus.of(UNAUTHORIZED, cause);
      case 403:
        return BackendErrorStatus.of(FORBIDDEN, cause);
      case 404:
        return BackendErrorStatus.of(NOT_FOUND, cause);
      case 429:
        return BackendErrorStatus.of(THROTTLED, cause);
      default:
        return BackendErrorStatus.of(UNKNOWN, cause);
    }
  }
}
