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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import org.projectnessie.nessie.tasks.async.ScheduledHandle;
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
  public CompletionStage<Void> call(Runnable runnable) {
    return supply(
        () -> {
          runnable.run();
          return null;
        });
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
  public ScheduledHandle schedule(Runnable runnable, Instant scheduleNotBefore) {
    long realDelay = calculateDelay(clock, minimumDelayMillis, scheduleNotBefore);

    // Cannot use Vertx.timer(), because current Quarkus 3.6.5 has a Vertx version that does not
    // have Vertx.timer(). We can use this once Quarkus uses a Vertx version >= 4.5.1:
    //    Timer timer = vertx.timer(realDelay, TimeUnit.MILLISECONDS);
    //    CompletionStage<Void> stage = timer.toCompletionStage().thenRun(runnable);
    //    return new VertxScheduledHandle(timer, stage);

    CompletableFuture<Void> completableFuture = new CompletableFuture<>();
    long timerId =
        vertx.setTimer(
            realDelay,
            id -> {
              try {
                runnable.run();
                completableFuture.complete(null);
              } catch (Throwable t) {
                completableFuture.completeExceptionally(new CompletionException(t));
              }
            });

    return new VertxScheduledHandle(vertx, timerId, completableFuture);
  }

  @Override
  public Clock clock() {
    return clock;
  }

  private static final class VertxScheduledHandle implements ScheduledHandle {

    private final Vertx vertx;
    private final long timerId;
    private final CompletionStage<Void> stage;

    VertxScheduledHandle(Vertx vertx, long timerId, CompletionStage<Void> stage) {
      this.vertx = vertx;
      this.timerId = timerId;
      this.stage = stage;
    }

    @Override
    public CompletionStage<Void> toCompletionStage() {
      return stage;
    }

    @Override
    public void cancel() {
      vertx.cancelTimer(timerId);
    }
  }
}
