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
package org.projectnessie.nessie.tasks.async.testing;

import java.time.Clock;
import java.time.Instant;
import java.util.NoSuchElementException;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import org.projectnessie.nessie.tasks.async.ScheduledHandle;
import org.projectnessie.nessie.tasks.async.TasksAsync;

/**
 * {@link TasksAsync} implementation for testing purposes. Collects tasks to run in an ordered
 * collection, tasks are run by calling {@link #doWork()}. There are no threads nor is there any
 * other concurrent code involved. Maybe (not) surprisingly, this class is <em>not</em> thread safe.
 */
public class TestingTasksAsync implements TasksAsync {

  private final TreeSet<ScheduledTask> scheduledTasks = new TreeSet<>();

  private final Clock clock;

  public TestingTasksAsync(Clock clock) {
    this.clock = clock;
  }

  public int doWork() {
    int tasks = 0;
    Instant now = clock.instant();
    while (true) {
      ScheduledTask t;
      try {
        t = scheduledTasks.first();
      } catch (NoSuchElementException empty) {
        break;
      }
      if (t == null || t.runAt.compareTo(now) > 0) {
        break;
      }
      scheduledTasks.pollFirst();
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

  @Override
  public CompletionStage<Void> call(Runnable runnable) {
    return schedule(runnable, Instant.EPOCH).toCompletionStage();
  }

  @SuppressWarnings("unchecked")
  @Override
  public <R> CompletionStage<R> supply(Supplier<R> supplier) {
    ScheduledTask task = new ScheduledTask(Instant.EPOCH, (Supplier<Object>) supplier);
    scheduledTasks.add(task);
    return (CompletionStage<R>) task.completable;
  }

  @Override
  public ScheduledHandle schedule(Runnable runnable, Instant scheduleNotBefore) {
    ScheduledTask task =
        new ScheduledTask(
            scheduleNotBefore,
            () -> {
              runnable.run();
              return null;
            });
    scheduledTasks.add(task);
    return task;
  }

  @Override
  public void cancel(ScheduledHandle handle) {
    ((ScheduledTask) handle).cancelled = true;
  }

  static final class ScheduledTask implements ScheduledHandle, Comparable<ScheduledTask> {
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
    @Override
    public CompletionStage<Void> toCompletionStage() {
      return (CompletionStage) completable;
    }
  }
}
