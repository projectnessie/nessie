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
package org.projectnessie.nessie.tasks.service.impl;

import java.time.Clock;
import java.time.Instant;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import org.projectnessie.nessie.tasks.async.TasksAsync;

/**
 * {@link TasksAsync} implementation for testing purposes. Collects tasks to run in an ordered
 * collection, tasks are run by calling {@link #doWork()}. There are no threads nor is there any
 * other concurrent code involved. Maybe (not) surprisingly, this class is <em>not</em> thread safe.
 */
public class TestingTasksAsync implements TasksAsync {

  private final Queue<ScheduledTask> scheduledTasks = new PriorityQueue<>();

  private final Clock clock;

  public TestingTasksAsync(Clock clock) {
    this.clock = clock;
  }

  public int doWork() {
    int tasks = 0;
    Instant now = clock.instant();
    while (true) {
      ScheduledTask t;
      t = scheduledTasks.peek();
      if (t == null || t.runAt.compareTo(now) > 0) {
        break;
      }
      scheduledTasks.remove(t);
      if (t.cancelled) {
        continue;
      }
      tasks++;
      try {
        t.completable.complete(t.runnable.get());
      } catch (Throwable ex) {
        t.completable.completeExceptionally(ex);
      }
    }
    return tasks;
  }

  @Override
  public Clock clock() {
    return clock;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <R> CompletionStage<R> supply(Supplier<R> supplier) {
    ScheduledTask task = new ScheduledTask(Instant.EPOCH, (Supplier<Object>) supplier);
    scheduledTasks.add(task);
    return (CompletionStage<R>) task.completable;
  }

  @Override
  public CompletionStage<Void> schedule(Runnable runnable, Instant scheduleNotBefore) {
    ScheduledTask task =
        new ScheduledTask(
            scheduleNotBefore,
            () -> {
              runnable.run();
              return null;
            });
    scheduledTasks.add(task);
    task.completable.whenComplete(
        (v, t) -> {
          if (t instanceof CancellationException) {
            task.cancelled = true;
          }
        });
    return task.completionStage();
  }

  static final class ScheduledTask implements Comparable<ScheduledTask> {
    private final Instant runAt;
    private final Supplier<Object> runnable;
    private final long random;
    volatile boolean cancelled;
    private final CompletableFuture<Object> completable;

    ScheduledTask(Instant runAt, Supplier<Object> runnable) {
      this.runAt = runAt;
      this.runnable = runnable;
      this.random = ThreadLocalRandom.current().nextLong();
      this.completable = new CompletableFuture<>();
    }

    @Override
    public int compareTo(ScheduledTask o) {
      int c = runAt.compareTo(o.runAt);
      if (c != 0) {
        return c;
      }
      return Long.compare(random, o.random);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    CompletionStage<Void> completionStage() {
      return (CompletionStage) completable;
    }
  }
}
