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
package org.projectnessie.nessie.tasks.async.wrapping;

import java.time.Clock;
import java.time.Instant;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import org.projectnessie.nessie.tasks.async.TasksAsync;

/**
 * Used to wrap {@link Runnable}s and {@link Supplier}s passed to {@link TasksAsync}
 * implementations.
 */
public abstract class WrappingTasksAsync implements TasksAsync {
  private final TasksAsync delegate;

  protected WrappingTasksAsync(TasksAsync delegate) {
    this.delegate = delegate;
  }

  protected abstract Runnable wrapRunnable(Runnable runnable);

  protected abstract <R> Supplier<R> wrapSupplier(Supplier<R> supplier);

  @Override
  public Clock clock() {
    return delegate.clock();
  }

  @Override
  public CompletionStage<Void> call(Runnable runnable) {
    return delegate.call(wrapRunnable(runnable));
  }

  @Override
  public <R> CompletionStage<R> supply(Supplier<R> supplier) {
    return delegate.supply(wrapSupplier(supplier));
  }

  @Override
  public CompletionStage<Void> schedule(Runnable runnable, Instant scheduleNotBefore) {
    return delegate.schedule(wrapRunnable(runnable), scheduleNotBefore);
  }

  @Override
  public long calculateDelay(Clock clock, long minimumDelayMillis, Instant scheduleNotBefore) {
    return delegate.calculateDelay(clock, minimumDelayMillis, scheduleNotBefore);
  }
}
