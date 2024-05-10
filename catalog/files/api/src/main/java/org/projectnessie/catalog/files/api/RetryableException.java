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

import java.time.Instant;
import java.util.Optional;

public class RetryableException extends ObjectIOException {
  private final Instant retryNotBefore;

  public RetryableException(Instant retryNotBefore, Throwable cause) {
    super(cause);
    this.retryNotBefore = retryNotBefore;
  }

  public RetryableException(Instant retryNotBefore, String message) {
    super(message);
    this.retryNotBefore = retryNotBefore;
  }

  public RetryableException(Instant retryNotBefore, String message, Throwable cause) {
    super(message, cause);
    this.retryNotBefore = retryNotBefore;
  }

  public boolean isRetryable() {
    return true;
  }

  public Optional<Instant> retryNotBefore() {
    return Optional.of(retryNotBefore);
  }
}
