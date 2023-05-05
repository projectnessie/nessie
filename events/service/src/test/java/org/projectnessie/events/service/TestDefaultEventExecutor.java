/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.events.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.google.common.util.concurrent.Uninterruptibles;
import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class TestDefaultEventExecutor {

  @Test
  void submitBlocking() throws InterruptedException {
    try (DefaultEventExecutor executor = new DefaultEventExecutor()) {
      CountDownLatch latch = new CountDownLatch(1);
      CompletionStage<Void> stage =
          executor.submitBlocking(() -> Uninterruptibles.awaitUninterruptibly(latch));
      assertThat(stage).isNotCompleted();
      latch.countDown();
      await()
          .atMost(5, TimeUnit.SECONDS)
          .untilAsserted(() -> assertThat(stage).isCompletedWithValue(null));
    }
  }

  @Test
  void submitBlockingFailure() throws InterruptedException {
    try (DefaultEventExecutor executor = new DefaultEventExecutor()) {
      CountDownLatch latch = new CountDownLatch(1);
      CompletionStage<Void> stage =
          executor.submitBlocking(
              () -> {
                Uninterruptibles.awaitUninterruptibly(latch);
                throw new IllegalArgumentException("test");
              });
      assertThat(stage).isNotCompleted();
      latch.countDown();
      await()
          .atMost(5, TimeUnit.SECONDS)
          .untilAsserted(() -> assertThat(stage).isCompletedExceptionally());
    }
  }

  @Test
  void submitBlockingCancel() throws InterruptedException {
    try (DefaultEventExecutor executor = new DefaultEventExecutor()) {
      CountDownLatch latch1 = new CountDownLatch(1);
      CountDownLatch latch2 = new CountDownLatch(1);
      AtomicBoolean interrupted = new AtomicBoolean(false);
      CompletionStage<Void> stage =
          executor.submitBlocking(
              () -> {
                try {
                  latch1.countDown();
                  latch2.await();
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  interrupted.set(true);
                }
              });
      latch1.await();
      assertThat(stage).isNotCompleted();
      stage.toCompletableFuture().cancel(true);
      await()
          .atMost(5, TimeUnit.SECONDS)
          .untilAsserted(() -> assertThat(interrupted.get()).isTrue());
    }
  }

  @Test
  void scheduleRetry() throws InterruptedException {
    try (DefaultEventExecutor executor = new DefaultEventExecutor()) {
      CountDownLatch latch = new CountDownLatch(1);
      CompletionStage<Void> stage =
          executor.scheduleRetry(
              () -> Uninterruptibles.awaitUninterruptibly(latch), Duration.ofMillis(10));
      assertThat(stage).isNotCompleted();
      latch.countDown();
      await()
          .atMost(5, TimeUnit.SECONDS)
          .untilAsserted(() -> assertThat(stage).isCompletedWithValue(null));
    }
  }

  @Test
  void scheduleRetryFailure() throws InterruptedException {
    try (DefaultEventExecutor executor = new DefaultEventExecutor()) {
      CountDownLatch latch = new CountDownLatch(1);
      CompletionStage<Void> stage =
          executor.scheduleRetry(
              () -> {
                Uninterruptibles.awaitUninterruptibly(latch);
                throw new IllegalArgumentException("test");
              },
              Duration.ofMillis(10));
      assertThat(stage).isNotCompleted();
      latch.countDown();
      await()
          .atMost(5, TimeUnit.SECONDS)
          .untilAsserted(() -> assertThat(stage).isCompletedExceptionally());
    }
  }

  @Test
  void scheduleRetryCancel() throws InterruptedException {
    try (DefaultEventExecutor executor = new DefaultEventExecutor()) {
      CountDownLatch latch1 = new CountDownLatch(1);
      CountDownLatch latch2 = new CountDownLatch(1);
      AtomicBoolean interrupted = new AtomicBoolean(false);
      CompletionStage<Void> stage =
          executor.scheduleRetry(
              () -> {
                try {
                  latch1.countDown();
                  latch2.await();
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  interrupted.set(true);
                }
              },
              Duration.ofMillis(10));
      latch1.await();
      assertThat(stage).isNotCompleted();
      stage.toCompletableFuture().cancel(true);
      await()
          .atMost(5, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                assertThat(interrupted.get()).isTrue();
                assertThat((Future<?>) executor.retryFuture).isCancelled();
              });
    }
  }
}
