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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.nessie.tasks.async.TasksAsync;
import org.projectnessie.nessie.tasks.async.pool.JavaPoolTasksAsync;

@ExtendWith(SoftAssertionsExtension.class)
public class TestWrappingTasksAsync {
  @InjectSoftAssertions protected SoftAssertions soft;

  protected static ScheduledExecutorService executorService;
  protected static TasksAsync base;

  @BeforeAll
  public static void setup() {
    executorService = Executors.newScheduledThreadPool(3);
    base = new JavaPoolTasksAsync(executorService, Clock.systemUTC(), 1L);
  }

  @AfterAll
  public static void tearDown() throws InterruptedException {
    executorService.shutdownNow();
    assertTrue(executorService.awaitTermination(30, TimeUnit.SECONDS));
  }

  @Test
  public void wrapClock() {
    TasksAsync mock = mock(TasksAsync.class);

    TasksAsync async =
        new WrappingTasksAsync(mock) {
          @Override
          protected Runnable wrapRunnable(Runnable runnable) {
            return runnable;
          }

          @Override
          protected <R> Supplier<R> wrapSupplier(Supplier<R> supplier) {
            return supplier;
          }
        };

    Clock c = Clock.fixed(Instant.EPOCH, ZoneId.of("UTC"));
    when(mock.clock()).thenReturn(c);

    soft.assertThat(async.clock()).isSameAs(c);
    verify(mock).clock();
    verifyNoMoreInteractions(mock);
  }

  @Test
  public void wrapCalculateDelay() {
    TasksAsync mock = mock(TasksAsync.class);

    TasksAsync async =
        new WrappingTasksAsync(mock) {
          @Override
          protected Runnable wrapRunnable(Runnable runnable) {
            return runnable;
          }

          @Override
          protected <R> Supplier<R> wrapSupplier(Supplier<R> supplier) {
            return supplier;
          }
        };

    Long magic = 4242424242L;
    when(mock.calculateDelay(any(), anyLong(), any())).thenReturn(magic);

    soft.assertThat(async.calculateDelay(Clock.systemUTC(), 1L, Instant.EPOCH)).isEqualTo(magic);
    verify(mock).calculateDelay(any(), anyLong(), any());
    verifyNoMoreInteractions(mock);
  }

  @Test
  public void wrapCall() throws Exception {
    AtomicReference<Object> wrapped = new AtomicReference<>();
    AtomicReference<Object> called = new AtomicReference<>();

    TasksAsync async =
        new WrappingTasksAsync(base) {
          @Override
          protected Runnable wrapRunnable(Runnable runnable) {
            wrapped.set(runnable);
            return () -> {
              called.set(runnable);
              runnable.run();
            };
          }

          @Override
          protected <R> Supplier<R> wrapSupplier(Supplier<R> supplier) {
            wrapped.set(supplier);
            return () -> {
              called.set(supplier);
              return supplier.get();
            };
          }
        };

    CountDownLatch latchCall = new CountDownLatch(1);
    Runnable runnable = latchCall::countDown;
    async.call(runnable);
    soft.assertThat(latchCall.await(10, TimeUnit.SECONDS)).isTrue();
    soft.assertThat(wrapped.get()).isSameAs(runnable);
    soft.assertThat(called.get()).isSameAs(runnable);
    wrapped.set(null);
    called.set(null);

    CountDownLatch latchSupply = new CountDownLatch(1);
    Supplier<String> supplier =
        () -> {
          latchSupply.countDown();
          return "supply";
        };
    async.supply(supplier);
    soft.assertThat(latchSupply.await(10, TimeUnit.SECONDS)).isTrue();
    soft.assertThat(wrapped.get()).isSameAs(supplier);
    soft.assertThat(called.get()).isSameAs(supplier);
    wrapped.set(null);
    called.set(null);

    CountDownLatch latchSchedule = new CountDownLatch(1);
    runnable = latchSchedule::countDown;
    async.schedule(runnable, Instant.EPOCH);
    soft.assertThat(latchSchedule.await(10, TimeUnit.SECONDS)).isTrue();
    soft.assertThat(wrapped.get()).isSameAs(runnable);
    soft.assertThat(called.get()).isSameAs(runnable);
    wrapped.set(null);
    called.set(null);
  }

  @Test
  public void wrapSupply() throws Exception {
    AtomicReference<Object> wrapped = new AtomicReference<>();
    AtomicReference<Object> called = new AtomicReference<>();

    TasksAsync async =
        new WrappingTasksAsync(base) {
          @Override
          protected Runnable wrapRunnable(Runnable runnable) {
            wrapped.set(runnable);
            return () -> {
              called.set(runnable);
              runnable.run();
            };
          }

          @Override
          protected <R> Supplier<R> wrapSupplier(Supplier<R> supplier) {
            wrapped.set(supplier);
            return () -> {
              called.set(supplier);
              return supplier.get();
            };
          }
        };

    CountDownLatch latchSupply = new CountDownLatch(1);
    Supplier<String> supplier =
        () -> {
          latchSupply.countDown();
          return "supply";
        };
    async.supply(supplier);
    soft.assertThat(latchSupply.await(10, TimeUnit.SECONDS)).isTrue();
    soft.assertThat(wrapped.get()).isSameAs(supplier);
    soft.assertThat(called.get()).isSameAs(supplier);
    wrapped.set(null);
    called.set(null);
  }

  @Test
  public void wrapSchedule() throws Exception {
    AtomicReference<Object> wrapped = new AtomicReference<>();
    AtomicReference<Object> called = new AtomicReference<>();

    TasksAsync async =
        new WrappingTasksAsync(base) {
          @Override
          protected Runnable wrapRunnable(Runnable runnable) {
            wrapped.set(runnable);
            return () -> {
              called.set(runnable);
              runnable.run();
            };
          }

          @Override
          protected <R> Supplier<R> wrapSupplier(Supplier<R> supplier) {
            wrapped.set(supplier);
            return () -> {
              called.set(supplier);
              return supplier.get();
            };
          }
        };

    CountDownLatch latchSchedule = new CountDownLatch(1);
    Runnable runnable = latchSchedule::countDown;
    async.schedule(runnable, Instant.EPOCH);
    soft.assertThat(latchSchedule.await(10, TimeUnit.SECONDS)).isTrue();
    soft.assertThat(wrapped.get()).isSameAs(runnable);
    soft.assertThat(called.get()).isSameAs(runnable);
    wrapped.set(null);
    called.set(null);
  }
}
