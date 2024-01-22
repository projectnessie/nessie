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

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Clock;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.projectnessie.nessie.tasks.async.BaseTasksAsync;
import org.projectnessie.nessie.tasks.async.TasksAsync;

public class TestJavaPoolTasksAsync extends BaseTasksAsync {
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

  @Override
  protected TasksAsync tasksAsync() {
    return new JavaPoolTasksAsync(executorService, Clock.systemUTC(), 1L);
  }

  @Test
  public void cancelScheduledWithInterrupt() throws InterruptedException {
    TasksAsync async = tasksAsync();

    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch finished = new CountDownLatch(1);

    CompletionStage<Void> handle =
        async.schedule(
            () -> {
              try {
                started.countDown();
                new Semaphore(0).acquire();
              } catch (InterruptedException ignored) {
                finished.countDown();
              }
            },
            async.clock().instant());
    soft.assertThat(handle).isNotNull();

    soft.assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
    handle.toCompletableFuture().cancel(true);
    soft.assertThat(finished.await(10, TimeUnit.SECONDS)).isTrue();
  }

  @Test
  @Disabled(
      "Interrupt does not work, although the CompletableFuture is properly cancelled and yields a CompletionException as its result")
  public void cancelSubmittedWithInterrupt() throws InterruptedException {
    TasksAsync async = tasksAsync();

    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch finished = new CountDownLatch(1);

    CompletionStage<String> handle =
        async.supply(
            () -> {
              try {
                started.countDown();
                new Semaphore(0).acquire();
                return "foo";
              } catch (InterruptedException ignored) {
                finished.countDown();
                throw new RuntimeException();
              } catch (Throwable e) {
                throw new RuntimeException(e);
              }
            });
    soft.assertThat(handle).isNotNull();

    soft.assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
    handle.toCompletableFuture().cancel(true);
    soft.assertThat(finished.await(10, TimeUnit.SECONDS)).isTrue();
  }
}
