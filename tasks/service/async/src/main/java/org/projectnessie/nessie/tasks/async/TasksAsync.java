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
package org.projectnessie.nessie.tasks.async;

import java.time.Clock;
import java.time.Instant;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

public interface TasksAsync {
  Clock clock();

  default CompletionStage<Void> call(Runnable runnable) {
    return supply(
        () -> {
          runnable.run();
          return null;
        });
  }

  <R> CompletionStage<R> supply(Supplier<R> runnable);

  /**
   * Schedule a {@link Runnable} to run at the given timestamp.
   *
   * <p>The scheduled task can always be cancelled as long as it is not running via <code>
   * {@link CompletionStage#toCompletableFuture() CompletionStage.toCompletableFuture()}.{@link java.util.concurrent.CompletableFuture#cancel(boolean) cancel(false)};
   * </code>.
   *
   * <p>Whether an already running task can be interrupted is not guaranteed and depends on the
   * implementation. It is highly recommended to not assume that a running task can be interrupted.
   */
  CompletionStage<Void> schedule(Runnable runnable, Instant scheduleNotBefore);

  default long calculateDelay(Clock clock, long minimumDelayMillis, Instant scheduleNotBefore) {
    long retryEarliestEpochMillis = scheduleNotBefore.toEpochMilli();
    long delayMillis = retryEarliestEpochMillis - clock.millis();

    return Math.max(delayMillis, minimumDelayMillis);
  }
}
