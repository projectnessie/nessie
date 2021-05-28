/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie.quarkus.runner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

class TestListenUrlWaiter {

  private static ExecutorService executor;

  @BeforeAll
  static void createExecutor() {
    executor = Executors.newCachedThreadPool();
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @AfterAll
  static void stopExecutor() throws Exception {
    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.SECONDS);
  }

  @Test
  void ioHandling() {
    AtomicLong clock = new AtomicLong();
    long timeout = 10L;

    AtomicReference<String> line = new AtomicReference<>();
    ListenUrlWaiter waiter = new ListenUrlWaiter(clock::get, timeout, line::set);

    waiter.accept("Hello World");
    assertThat(line.getAndSet(null)).isEqualTo("Hello World");
    assertThat(waiter.peekListenUrl()).isNull();

    waiter.accept("");
    assertThat(line.getAndSet(null)).isEqualTo("");
    assertThat(waiter.peekListenUrl()).isNull();

    String listenLine =
        "2021-05-28 12:12:25,753 INFO  [io.quarkus] (main) nessie-quarkus 0.6.2-SNAPSHOT on JVM (powered by Quarkus 1.13.4.Final) started in 1.444s. Listening on: http://0.0.0.0:39423";
    waiter.accept(listenLine);
    assertThat(line.getAndSet(null)).isEqualTo(listenLine);
    assertThat(waiter.peekListenUrl()).isEqualTo("http://0.0.0.0:39423");

    // Must *not* change the already extracted listen-url
    listenLine =
        "2021-05-28 12:12:25,753 INFO  [io.quarkus] (main) nessie-quarkus 0.6.2-SNAPSHOT on JVM (powered by Quarkus 1.13.4.Final) started in 1.444s. Listening on: http://4.2.4.2:4242";
    waiter.accept(listenLine);
    assertThat(line.getAndSet(null)).isEqualTo(listenLine);
    assertThat(waiter.peekListenUrl()).isEqualTo("http://0.0.0.0:39423");

    waiter = new ListenUrlWaiter(clock::get, timeout, line::set);
    listenLine =
        "2021-05-28 12:12:25,753 INFO  [io.quarkus] (main) nessie-quarkus 0.6.2-SNAPSHOT on JVM (powered by Quarkus 1.13.4.Final) started in 1.444s. Listening on: https://localhost.in.some.space:12345";
    waiter.accept(listenLine);
    assertThat(line.getAndSet(null)).isEqualTo(listenLine);
    assertThat(waiter.peekListenUrl()).isEqualTo("https://localhost.in.some.space:12345");

    waiter = new ListenUrlWaiter(clock::get, timeout, line::set);
    listenLine = "Listening on: https://localhost.in.some.space:4242";
    waiter.accept(listenLine);
    assertThat(line.getAndSet(null)).isEqualTo(listenLine);
    assertThat(waiter.peekListenUrl()).isEqualTo("https://localhost.in.some.space:4242");
  }

  @RepeatedTest(20) // repeat, risk of flakiness
  void timeout() {
    AtomicLong clock = new AtomicLong();
    long timeout = 10_000L; // long timeout, for slow CI

    AtomicReference<String> line = new AtomicReference<>();
    ListenUrlWaiter waiter = new ListenUrlWaiter(clock::get, timeout, line::set);

    assertThat(waiter.isTimeout()).isFalse();

    clock.set(TimeUnit.MILLISECONDS.toNanos(timeout + 1));

    assertThat(waiter.isTimeout()).isTrue();

    assertThat(executor.submit(waiter::getListenUrl))
        .failsWithin(5, TimeUnit.SECONDS)
        .withThrowableOfType(ExecutionException.class)
        .withRootCauseExactlyInstanceOf(TimeoutException.class)
        .withMessageEndingWith(ListenUrlWaiter.TIMEOUT_MESSAGE);
  }

  @RepeatedTest(20) // repeat, risk of flakiness
  void noTimeout() throws Exception {
    AtomicLong clock = new AtomicLong();
    long timeout = 10_000L; // long timeout, for slow CI

    // Note: the implementation uses "our clock" to check the timeout, but uses a "standard
    // Future.get(time)" for the actual get.

    AtomicReference<String> line = new AtomicReference<>();
    ListenUrlWaiter waiter = new ListenUrlWaiter(clock::get, timeout, line::set);

    // Clock exactly at the timeout-boundary is not a timeout
    clock.set(TimeUnit.MILLISECONDS.toNanos(timeout));

    String listenLine =
        "2021-05-28 12:12:25,753 INFO  [io.quarkus] (main) nessie-quarkus 0.6.2-SNAPSHOT on JVM (powered by Quarkus 1.13.4.Final) started in 1.444s. Listening on: http://4.2.4.2:4242";
    waiter.accept(listenLine);
    assertThat(line.getAndSet(null)).isEqualTo(listenLine);
    assertThat(waiter.getListenUrl()).isEqualTo("http://4.2.4.2:4242");
    assertThat(waiter.isTimeout()).isFalse();

    // Clock post the timeout-boundary (so a timeout-check would trigger)
    clock.set(TimeUnit.MILLISECONDS.toNanos(timeout + 1));
    assertThat(waiter.getListenUrl()).isEqualTo("http://4.2.4.2:4242");
    assertThat(waiter.isTimeout()).isFalse();
  }
}
