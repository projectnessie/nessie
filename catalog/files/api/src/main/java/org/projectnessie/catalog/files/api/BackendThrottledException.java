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

import static java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME;
import static java.time.temporal.ChronoUnit.SECONDS;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Optional;
import java.util.function.Function;

public class BackendThrottledException extends RetryableException {
  public BackendThrottledException(Instant retryNotBefore, Throwable cause) {
    super(retryNotBefore, cause);
  }

  public BackendThrottledException(Instant retryNotBefore, String message) {
    super(retryNotBefore, message);
  }

  public BackendThrottledException(Instant retryNotBefore, String message, Throwable cause) {
    super(retryNotBefore, message, cause);
  }

  /**
   * Provides a {@link BackendThrottledException} instance for HTTP responses, for the HTTP status
   * codes 429 (too many requests) + 503 (service unavailable), handling HTTP responses having the
   * {@code Retry-After} header.
   */
  public static Optional<BackendThrottledException> fromHttpStatusCode(
      int statusCode,
      Function<String, String> headers,
      Clock clock,
      Function<Instant, BackendThrottledException> exceptionSupplier,
      Duration defaultRetryAfter) {
    switch (statusCode) {
      case 429:
      case 503:
        String retryAfter = headers.apply("Retry-After");
        Instant retryNotBefore = null;
        Instant now = clock.instant();
        if (retryAfter != null) {
          try {
            int seconds = Integer.parseInt(retryAfter);
            if (seconds > 0) {
              retryNotBefore = now.plus(seconds, SECONDS);
            }
          } catch (NumberFormatException e) {
            try {
              retryNotBefore = Instant.from(RFC_1123_DATE_TIME.parse(retryAfter));
              if (retryNotBefore.compareTo(now) <= 0) {
                retryNotBefore = null;
              }
            } catch (DateTimeParseException e2) {
              //
            }
          }
        }
        if (retryNotBefore == null) {
          retryNotBefore = now.plus(defaultRetryAfter);
        }
        return Optional.of(exceptionSupplier.apply(retryNotBefore));
      default:
        break;
    }
    return Optional.empty();
  }
}
