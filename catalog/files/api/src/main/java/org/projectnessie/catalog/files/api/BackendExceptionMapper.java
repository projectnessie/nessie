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

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
public abstract class BackendExceptionMapper {

  protected abstract List<Analyzer> analyzers();

  protected abstract Duration retryAfterThrottled();

  protected abstract Clock clock();

  public static Builder builder() {
    return ImmutableBackendExceptionMapper.builder();
  }

  public Optional<BackendErrorStatus> analyze(Throwable ex) {
    for (Throwable th = ex; th != null; th = th.getCause()) {
      for (Analyzer analyzer : analyzers()) {
        BackendErrorStatus status = analyzer.analyze(th);
        if (status != null) {
          if (status.statusCode() == BackendErrorCode.THROTTLED) {
            return Optional.of(status.withReattemptAfter(retryAfter(retryAfterThrottled())));
          }
          return Optional.of(status);
        }
      }
    }

    return Optional.empty();
  }

  private Instant retryAfter(Duration delay) {
    return clock().instant().plus(delay);
  }

  public interface Analyzer {
    BackendErrorStatus analyze(Throwable th);
  }

  public interface Builder {
    @CanIgnoreReturnValue
    Builder clock(Clock clock);

    @CanIgnoreReturnValue
    Builder retryAfterThrottled(Duration retryDelay);

    @CanIgnoreReturnValue
    Builder addAnalyzer(Analyzer analyzer);

    BackendExceptionMapper build();
  }
}
