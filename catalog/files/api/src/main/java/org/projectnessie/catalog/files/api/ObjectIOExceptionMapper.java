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

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
public abstract class ObjectIOExceptionMapper {

  protected abstract List<Analyzer> analyzers();

  protected abstract Duration retryAfterThrottled();

  protected abstract Duration retryAfterNetworkError();

  protected abstract Duration reattemptAfterFetchError();

  protected abstract Clock clock();

  public static ImmutableObjectIOExceptionMapper.Builder builder() {
    return ImmutableObjectIOExceptionMapper.builder();
  }

  public Optional<ObjectIOStatus> analyze(Throwable ex) {
    for (Throwable th = ex; th != null; th = th.getCause()) {
      for (Analyzer analyzer : analyzers()) {
        OptionalInt statusCode = analyzer.httpStatusCode(th);
        if (statusCode.isPresent()) {
          return Optional.of(toStatus(statusCode.getAsInt(), th));
        }
      }
    }

    return Optional.empty();
  }

  private Instant retryAfter(Duration delay) {
    return clock().instant().plus(delay);
  }

  private ObjectIOStatus toStatus(int httpStatus, Throwable th) {
    switch (httpStatus) {
      case 408:
      case 425:
      case 429:
        return ObjectIOStatus.of(httpStatus, true, retryAfter(retryAfterThrottled()), th);

      case 502:
      case 503:
      case 504:
        return ObjectIOStatus.of(httpStatus, true, retryAfter(retryAfterNetworkError()), th);

      default:
        return ObjectIOStatus.of(httpStatus, false, retryAfter(reattemptAfterFetchError()), th);
    }
  }

  public interface Analyzer {
    OptionalInt httpStatusCode(Throwable th);
  }
}
