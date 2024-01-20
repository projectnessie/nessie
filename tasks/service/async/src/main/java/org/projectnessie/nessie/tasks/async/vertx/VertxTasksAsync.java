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
package org.projectnessie.nessie.tasks.async.vertx;

import io.vertx.core.Vertx;
import java.time.Clock;
import java.time.Instant;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import org.projectnessie.nessie.tasks.async.TasksAsync;

public class VertxTasksAsync implements TasksAsync {
  private final Vertx vertx;
  private final Clock clock;
  private final long minimumDelayMillis;

  public VertxTasksAsync(Vertx vertx, Clock clock, long minimumDelayMillis) {
    this.vertx = vertx;
    this.clock = clock;
    this.minimumDelayMillis = minimumDelayMillis;
  }

  @Override
  public <R> CompletionStage<R> supply(Supplier<R> supplier) {
    return vertx
        .executeBlocking(
            () -> {
              try {
                return supplier.get();
              } catch (Throwable t) {
                throw new CompletionException(t);
              }
            },
            false)
        .toCompletionStage();
  }

  @Override
  public CompletionStage<Void> schedule(Runnable runnable, Instant scheduleNotBefore) {
    long realDelay = calculateDelay(clock, minimumDelayMillis, scheduleNotBefore);

    // Cannot use Vertx.timer(), because current Quarkus 3.6.6 has a Vertx version that does not
    // have Vertx.timer(). We can use this once Quarkus uses a Vertx version >= 4.5.1:
    //    Timer timer = vertx.timer(realDelay, TimeUnit.MILLISECONDS);
    //    CompletionStage<Void> stage = timer.toCompletionStage().thenRun(runnable);
    //    return new VertxScheduledHandle(timer, stage);

    CompletableFuture<Void> completableFuture = new CompletableFuture<>();
    long timerId =
        vertx.setTimer(
            realDelay,
            id ->
                // scheduling the runnable as a blocking task, a timer handlers must not block
                vertx.executeBlocking(
                    () -> {
                      try {
                        runnable.run();
                        completableFuture.complete(null);
                        return null;
                      } catch (Throwable t) {
                        completableFuture.completeExceptionally(new CompletionException(t));
                        return null;
                      }
                    }));

    completableFuture.whenComplete(
        (v, t) -> {
          if (t instanceof CancellationException) {
            vertx.cancelTimer(timerId); // won't interrupt blocking tasks!
          }
        });

    return completableFuture;
  }

  @Override
  public Clock clock() {
    return clock;
  }
}
