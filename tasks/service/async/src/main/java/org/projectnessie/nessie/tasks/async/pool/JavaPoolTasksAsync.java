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
package org.projectnessie.nessie.tasks.async.pool;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.time.Clock;
import java.time.Instant;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Supplier;
import org.projectnessie.nessie.tasks.async.TasksAsync;

public class JavaPoolTasksAsync implements TasksAsync {
  private final ScheduledExecutorService executorService;
  private final Clock clock;
  private final long minimumDelayMillis;

  public JavaPoolTasksAsync(
      ScheduledExecutorService executorService, Clock clock, long minimumDelayMillis) {
    this.executorService = executorService;
    this.clock = clock;
    this.minimumDelayMillis = minimumDelayMillis;
  }

  @Override
  public <R> CompletionStage<R> supply(Supplier<R> supplier) {
    return CompletableFuture.supplyAsync(supplier, executorService);
  }

  @Override
  public CompletionStage<Void> schedule(Runnable runnable, Instant scheduleNotBefore) {
    long realDelay = calculateDelay(clock, minimumDelayMillis, scheduleNotBefore);

    CompletableFuture<Void> completable = new CompletableFuture<>();

    ScheduledFuture<?> future =
        executorService.schedule(
            () -> {
              try {
                runnable.run();
                completable.complete(null);
              } catch (Throwable t) {
                completable.completeExceptionally(new CompletionException(t));
              }
            },
            realDelay,
            MILLISECONDS);

    completable.whenComplete(
        (v, t) -> {
          if (t instanceof CancellationException) {
            future.cancel(true); // allow interruption of blocking tasks
          }
        });

    return completable;
  }

  @Override
  public Clock clock() {
    return clock;
  }
}
