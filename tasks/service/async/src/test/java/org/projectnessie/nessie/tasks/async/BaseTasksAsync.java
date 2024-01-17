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
package org.projectnessie.nessie.tasks.async;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@ExtendWith(SoftAssertionsExtension.class)
public abstract class BaseTasksAsync {
  @InjectSoftAssertions protected SoftAssertions soft;

  protected abstract TasksAsync tasksAsync();

  @ParameterizedTest
  @ValueSource(ints = {1, 10, 1000})
  public void callWorks(int num) throws InterruptedException {
    TasksAsync async = tasksAsync();

    Semaphore semImmediate = new Semaphore(0);
    Semaphore semStage = new Semaphore(0);
    List<CompletionStage<Void>> stages = new ArrayList<>(num);

    for (int i = 0; i < num; i++) {
      stages.add(
          async
              .call(semImmediate::release)
              // on CompletionStage
              .thenAccept(x -> semStage.release()));
    }

    soft.assertThat(semImmediate.tryAcquire(num, 10, TimeUnit.SECONDS)).isTrue();
    soft.assertThat(semStage.tryAcquire(num, 10, TimeUnit.SECONDS)).isTrue();

    stages.stream()
        .map(CompletionStage::toCompletableFuture)
        .forEach(
            cf -> {
              try {
                cf.get();
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });

    soft.assertThat(stages)
        .hasSize(num)
        .extracting(CompletionStage::toCompletableFuture)
        .allMatch(CompletableFuture::isDone);
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 10, 1000})
  public void supplyWorks(int num) throws InterruptedException {
    TasksAsync async = tasksAsync();

    Semaphore semImmediate = new Semaphore(0);
    Semaphore semStage = new Semaphore(0);
    List<CompletionStage<Integer>> stages = new ArrayList<>(num);

    for (int i = 0; i < num; i++) {
      int i2 = i;
      stages.add(
          async
              .supply(
                  () -> {
                    semImmediate.release();
                    return i2;
                  })
              // on CompletionStage
              .thenApply(
                  v -> {
                    semStage.release();
                    return v;
                  }));
    }

    soft.assertThat(semImmediate.tryAcquire(num, 10, TimeUnit.SECONDS)).isTrue();
    soft.assertThat(semStage.tryAcquire(num, 10, TimeUnit.SECONDS)).isTrue();

    Set<Integer> nums =
        stages.stream()
            .map(CompletionStage::toCompletableFuture)
            .map(
                cf -> {
                  try {
                    return cf.get();
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                })
            .collect(Collectors.toSet());

    soft.assertThat(stages)
        .hasSize(num)
        .extracting(CompletionStage::toCompletableFuture)
        .allMatch(CompletableFuture::isDone);

    soft.assertThat(nums)
        .containsExactlyInAnyOrderElementsOf(
            IntStream.range(0, num).boxed().collect(Collectors.toSet()));
  }

  @Test
  public void callExceptionallyWorks() throws Exception {
    TasksAsync async = tasksAsync();

    CompletionStage<Void> stage =
        async.call(
            () -> {
              throw new RuntimeException("hello");
            });

    Throwable mappedFailure =
        stage.handle((result, failure) -> failure).toCompletableFuture().get();

    soft.assertThat(mappedFailure)
        .isInstanceOf(CompletionException.class)
        .extracting(Throwable::getCause)
        .isInstanceOf(RuntimeException.class)
        .extracting(Throwable::getMessage)
        .isEqualTo("hello");
  }

  @Test
  public void scheduledExceptionallyWorks() throws Exception {
    TasksAsync async = tasksAsync();

    CompletionStage<Void> stage =
        async
            .schedule(
                () -> {
                  throw new RuntimeException("hello");
                },
                Instant.now())
            .toCompletionStage();

    Throwable mappedFailure =
        stage.handle((result, failure) -> failure).toCompletableFuture().get();

    soft.assertThat(mappedFailure)
        .isInstanceOf(CompletionException.class)
        .extracting(Throwable::getCause)
        .isInstanceOf(RuntimeException.class)
        .extracting(Throwable::getMessage)
        .isEqualTo("hello");
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 10, 1000})
  public void scheduleWorks(int num) throws InterruptedException {
    TasksAsync async = tasksAsync();

    Semaphore sem = new Semaphore(0);

    for (int i = 0; i < num; i++) {
      async.schedule(sem::release, async.clock().instant());
    }

    soft.assertThat(sem.tryAcquire(num, 10, TimeUnit.SECONDS)).isTrue();
  }

  @Test
  public void cancelDoesNotError() {
    // See below
    TasksAsync async = tasksAsync();
    ScheduledHandle handle =
        async.schedule(() -> {}, async.clock().instant().plus(1500, ChronoUnit.MILLIS));
    soft.assertThatCode(handle::cancel).doesNotThrowAnyException();
  }

  @Test
  @Disabled(
      "Testing scheduled/timer cancellation requires either tweaking the inner workings of Vert.X / ScheduledExecutorService or results in extremely long test durations.")
  public void cancelReallyWorks() throws InterruptedException {
    TasksAsync async = tasksAsync();

    Semaphore sem = new Semaphore(0);

    ScheduledHandle handle =
        async.schedule(sem::release, async.clock().instant().plus(1500, ChronoUnit.MILLIS));
    soft.assertThat(handle).isNotNull();

    handle.cancel();
    soft.assertThat(sem.tryAcquire(1, 4500, TimeUnit.MILLISECONDS)).isFalse();
  }

  @Test
  public void realDelayCalculation() {
    TasksAsync async = tasksAsync();

    Clock clock = Clock.fixed(Instant.EPOCH, ZoneId.of("UTC"));
    Instant now = clock.instant();

    soft.assertThat(async.calculateDelay(clock, 1L, now)).isEqualTo(1L);
    soft.assertThat(async.calculateDelay(clock, 42L, now)).isEqualTo(42L);

    soft.assertThat(async.calculateDelay(clock, 1L, now.plus(10, ChronoUnit.MILLIS)))
        .isEqualTo(10L);

    soft.assertThat(async.calculateDelay(clock, 1L, now.minus(10, ChronoUnit.MILLIS)))
        .isEqualTo(1L);
  }
}
