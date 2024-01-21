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

import java.util.function.Supplier;
import org.eclipse.microprofile.context.ThreadContext;
import org.projectnessie.nessie.tasks.async.TasksAsync;

/** Allows using the Microprofile {@link ThreadContext} with a {@link TasksAsync}. */
public class ThreadContextTasksAsync extends WrappingTasksAsync {
  private final ThreadContext threadContext;

  public ThreadContextTasksAsync(TasksAsync tasksAsync, ThreadContext threadContext) {
    super(tasksAsync);
    this.threadContext = threadContext;
  }

  @Override
  protected Runnable wrapRunnable(Runnable runnable) {
    return threadContext.contextualRunnable(runnable);
  }

  @Override
  protected <R> Supplier<R> wrapSupplier(Supplier<R> supplier) {
    return threadContext.contextualSupplier(supplier);
  }
}
