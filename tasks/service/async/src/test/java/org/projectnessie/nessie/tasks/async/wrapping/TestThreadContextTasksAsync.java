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

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.eclipse.microprofile.context.ThreadContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.nessie.tasks.async.TasksAsync;
import org.projectnessie.nessie.tasks.async.pool.JavaPoolTasksAsync;

@ExtendWith(SoftAssertionsExtension.class)
public class TestThreadContextTasksAsync {
  @InjectSoftAssertions protected SoftAssertions soft;

  protected static ScheduledExecutorService executorService;

  @BeforeAll
  public static void setup() {
    executorService = Executors.newScheduledThreadPool(3);
  }

  @AfterAll
  public static void tearDown() throws InterruptedException {
    executorService.shutdownNow();
    assertTrue(executorService.awaitTermination(30, TimeUnit.SECONDS));
  }

  @Test
  public void call() throws Exception {
    TasksAsync base = new JavaPoolTasksAsync(executorService, Clock.systemUTC(), 1L);

    ThreadContext threadContext = mock(ThreadContext.class);

    TasksAsync async = new ThreadContextTasksAsync(base, threadContext);

    CountDownLatch latchCall = new CountDownLatch(1);
    Runnable runnable = latchCall::countDown;
    when(threadContext.contextualRunnable(any())).thenReturn(runnable);

    async.call(runnable);
    soft.assertThat(latchCall.await(10, TimeUnit.SECONDS)).isTrue();
    verify(threadContext).contextualRunnable(any());
    verifyNoMoreInteractions(threadContext);
  }

  @Test
  public void supply() throws Exception {
    TasksAsync base = new JavaPoolTasksAsync(executorService, Clock.systemUTC(), 1L);

    ThreadContext threadContext = mock(ThreadContext.class);

    TasksAsync async = new ThreadContextTasksAsync(base, threadContext);

    CountDownLatch latchSupply = new CountDownLatch(1);
    Supplier<Object> supplier =
        () -> {
          latchSupply.countDown();
          return "supply";
        };
    when(threadContext.contextualSupplier(any())).thenReturn(supplier);

    async.supply(supplier);
    soft.assertThat(latchSupply.await(10, TimeUnit.SECONDS)).isTrue();
    verify(threadContext).contextualSupplier(any());
    verifyNoMoreInteractions(threadContext);
  }

  @Test
  public void schedule() throws Exception {
    TasksAsync base = new JavaPoolTasksAsync(executorService, Clock.systemUTC(), 1L);

    ThreadContext threadContext = mock(ThreadContext.class);

    TasksAsync async = new ThreadContextTasksAsync(base, threadContext);

    CountDownLatch latchSchedule = new CountDownLatch(1);
    Runnable runnable = latchSchedule::countDown;
    when(threadContext.contextualRunnable(any())).thenReturn(runnable);

    async.schedule(runnable, Instant.EPOCH);
    soft.assertThat(latchSchedule.await(10, TimeUnit.SECONDS)).isTrue();
    verify(threadContext).contextualRunnable(any());
    verifyNoMoreInteractions(threadContext);
  }
}
