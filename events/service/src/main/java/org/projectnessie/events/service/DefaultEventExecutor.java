/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.events.service;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Simple implementation of {@link EventExecutor} that uses the same {@link
 * ScheduledExecutorService} for scheduling retries and for submitting blocking delivery tasks.
 *
 * <p>This class is meant as a reference implementation for tests, and is not optimized for
 * performance. Nessie on Quarkus will use a more sophisticated implementation.
 */
public class DefaultEventExecutor implements EventExecutor {

  private final ScheduledExecutorService executor =
      Executors.newScheduledThreadPool(
          1,
          r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.setName("nessie-events-delivery");
            return t;
          });

  // visible for testing
  volatile ScheduledFuture<?> retryFuture;

  @Override
  @SuppressWarnings("FutureReturnValueIgnored")
  public CompletionStage<Void> submitBlocking(Runnable blockingDelivery) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    Future<?> submitFuture =
        executor.submit(
            () -> {
              try {
                blockingDelivery.run();
                future.complete(null);
              } catch (Throwable t) {
                future.completeExceptionally(t);
              }
            });
    future.whenComplete(
        (v, t) -> {
          if (t != null) {
            submitFuture.cancel(true);
          }
        });
    return future;
  }

  @Override
  @SuppressWarnings("FutureReturnValueIgnored")
  public CompletionStage<Void> scheduleRetry(Runnable retry, Duration delay) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    ScheduledFuture<?> retryFuture =
        executor.schedule(
            () -> {
              try {
                retry.run();
                future.complete(null);
              } catch (Throwable t) {
                future.completeExceptionally(t);
              }
            },
            delay.toMillis(),
            TimeUnit.MILLISECONDS);
    future.whenComplete(
        (v, t) -> {
          if (t != null) {
            retryFuture.cancel(true);
          }
        });
    this.retryFuture = retryFuture;
    return future;
  }

  @Override
  public void close() throws InterruptedException {
    ScheduledFuture<?> retryFuture = this.retryFuture;
    if (retryFuture != null) {
      retryFuture.cancel(true);
    }
    executor.shutdown();
    if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
      executor.shutdownNow();
    }
    this.retryFuture = null;
  }
}
