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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

class TestProcessHandler {

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
  void notStarted() {
    ProcessHandlerMock phMock = new ProcessHandlerMock();

    assertThatThrownBy(phMock.ph::stop)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("No process started");
  }

  @Test
  void doubleStart() {
    ProcessHandlerMock phMock = new ProcessHandlerMock();

    phMock.ph.started(phMock.proc);

    assertThatThrownBy(() -> phMock.ph.started(phMock.proc))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Process already started");
  }

  @RepeatedTest(20) // repeat, risk of flakiness
  void processWithNoOutput() {
    ProcessHandlerMock phMock = new ProcessHandlerMock();

    phMock.ph.started(phMock.proc);

    Future<String> futureListenUrl = executor.submit(phMock.ph::getListenUrl);

    while (phMock.clock.get() < TimeUnit.MILLISECONDS.toNanos(phMock.timeToUrl)) {
      assertThat(futureListenUrl).isNotDone();
      phMock.clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(10));
    }
    // should be exactly at (but not "past") the time to wait for the listen-url now

    // bump the clock "past" the listen-url-timeout
    phMock.clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(10));

    assertThat(futureListenUrl)
        .failsWithin(5, TimeUnit.SECONDS)
        .withThrowableOfType(ExecutionException.class) // EE from ForkJoinPool/executor (test code)
        .withRootCauseInstanceOf(
            TimeoutException.class) // TE from ProcessHandler/ListenUrlWaiter.getListenUrl
        .withMessageEndingWith(ListenUrlWaiter.TIMEOUT_MESSAGE);

    // Need to wait for the watchdog to finish, before we can do any further assertion
    phMock.ph.watchdogExitGrace();

    assertThat(phMock.ph.isAlive()).isFalse();
  }

  @RepeatedTest(20) // repeat, risk of flakiness
  void processExitsEarly() {
    ProcessHandlerMock phMock = new ProcessHandlerMock();

    phMock.ph.started(phMock.proc);

    Future<String> futureListenUrl = executor.submit(phMock.ph::getListenUrl);

    assertThat(phMock.ph.isAlive()).isTrue();
    assertThatThrownBy(() -> phMock.ph.getExitCode())
        .isInstanceOf(IllegalThreadStateException.class);

    phMock.exitCode.set(88);

    assertThat(futureListenUrl)
        .failsWithin(5, TimeUnit.SECONDS)
        .withThrowableOfType(ExecutionException.class) // EE from ForkJoinPool/executor (test code)
        .withRootCauseInstanceOf(
            TimeoutException.class) // TE from ProcessHandler/ListenUrlWaiter.getListenUrl
        .withMessageEndingWith(ListenUrlWaiter.TIMEOUT_MESSAGE);

    // Need to wait for the watchdog to finish, before we can do any further assertion
    phMock.ph.watchdogExitGrace();

    assertThat(phMock.stderrLines).containsExactly("Watched process exited with exit-code 88");

    assertThat(phMock.ph.isAlive()).isFalse();
    assertThat(phMock.ph.getExitCode()).isEqualTo(88);
  }

  @RepeatedTest(20) // repeat, risk of flakiness
  void processLotsOfIoNoListen() {
    ProcessHandlerMock phMock = new ProcessHandlerMock();

    phMock.ph.started(phMock.proc);

    Future<String> futureListenUrl = executor.submit(phMock.ph::getListenUrl);

    while (phMock.clock.get() < TimeUnit.MILLISECONDS.toNanos(phMock.timeToUrl)) {
      for (char c : "Hello world\n".toCharArray()) {
        phMock.stdout.add((byte) c);
      }
      for (char c : "Errors do not exist\n".toCharArray()) {
        phMock.stderr.add((byte) c);
      }
      assertThat(futureListenUrl).isNotDone();
      phMock.clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(10));
    }
    // should be exactly at (but not "past") the time to wait for the listen-url now

    // bump the clock "past" the listen-url-timeout
    phMock.clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(10));

    assertThat(futureListenUrl)
        .failsWithin(5, TimeUnit.SECONDS)
        .withThrowableOfType(ExecutionException.class) // EE from ForkJoinPool/executor (test code)
        .withRootCauseInstanceOf(
            TimeoutException.class) // TE from ProcessHandler/ListenUrlWaiter.getListenUrl
        .withMessageEndingWith(ListenUrlWaiter.TIMEOUT_MESSAGE);

    // Need to wait for the watchdog to finish, before we can do any further assertion
    phMock.ph.watchdogExitGrace();

    assertThat(phMock.ph.isAlive()).isFalse();
    assertThat(phMock.ph.getExitCode()).isGreaterThanOrEqualTo(0);

    assertThat(phMock.stdoutLines).hasSize((int) (phMock.timeToUrl / 10));
    assertThat(phMock.stderrLines).hasSize((int) (phMock.timeToUrl / 10));
  }

  @RepeatedTest(20) // repeat, risk of flakiness
  void processLotsOfIoProperListenUrl() {
    ProcessHandlerMock phMock = new ProcessHandlerMock();

    phMock.ph.started(phMock.proc);

    Future<String> futureListenUrl = executor.submit(phMock.ph::getListenUrl);

    while (phMock.clock.get() < TimeUnit.MILLISECONDS.toNanos(phMock.timeToUrl / 2)) {
      for (char c : "Hello world\n".toCharArray()) {
        phMock.stdout.add((byte) c);
      }
      for (char c : "Errors do not exist\n".toCharArray()) {
        phMock.stderr.add((byte) c);
      }
      assertThat(futureListenUrl).isNotDone();
      phMock.clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(10));
    }
    // should be exactly at (but not "past") the time to wait for the listen-url now

    for (char c : "Quarkus startup message... Listening on: http://0.0.0.0:4242\n".toCharArray()) {
      phMock.stdout.add((byte) c);
    }

    // bump the clock "past" the listen-url-timeout
    phMock.clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(10));

    assertThat(futureListenUrl)
        .succeedsWithin(5, TimeUnit.SECONDS)
        .isEqualTo("http://0.0.0.0:4242");

    assertThat(phMock.ph.isAlive()).isTrue();
    assertThatThrownBy(() -> phMock.ph.getExitCode())
        .isInstanceOf(IllegalThreadStateException.class);

    // The .stop() waits until the watchdog has finished its work
    phMock.ph.stop();

    assertThat(phMock.ph.isAlive()).isFalse();
    assertThat(phMock.ph.getExitCode()).isGreaterThanOrEqualTo(0);

    assertThat(phMock.stdoutLines).hasSize((int) (phMock.timeToUrl / 10 / 2) + 1);
    assertThat(phMock.stderrLines).hasSize((int) (phMock.timeToUrl / 10 / 2));
  }

  static final class ProcessHandlerMock {

    AtomicLong clock = new AtomicLong();

    AtomicInteger exitCode = new AtomicInteger(-1);

    // Full lines received "form the process" via stdout/stderr is collected in these lists
    List<String> stderrLines = Collections.synchronizedList(new ArrayList<>());
    List<String> stdoutLines = Collections.synchronizedList(new ArrayList<>());

    // Data that's "written by the process" to stdout/stderr is "piped" through these queues
    ArrayBlockingQueue<Byte> stdout = new ArrayBlockingQueue<>(1024);
    ArrayBlockingQueue<Byte> stderr = new ArrayBlockingQueue<>(1024);

    InputStream stdoutStream =
        new InputStream() {
          @Override
          public int available() {
            return stdout.size();
          }

          @Override
          public int read() {
            Byte b = stdout.poll();
            return b == null ? -1 : b.intValue();
          }
        };
    InputStream stderrStream =
        new InputStream() {
          @Override
          public int available() {
            return stderr.size();
          }

          @Override
          public int read() {
            Byte b = stderr.poll();
            return b == null ? -1 : b.intValue();
          }
        };

    Process proc =
        new Process() {
          @Override
          public OutputStream getOutputStream() {
            throw new UnsupportedOperationException();
          }

          @Override
          public InputStream getInputStream() {
            return stdoutStream;
          }

          @Override
          public InputStream getErrorStream() {
            return stderrStream;
          }

          @Override
          public int waitFor() {
            throw new UnsupportedOperationException();
          }

          @Override
          public boolean waitFor(long timeout, TimeUnit unit) throws InterruptedException {
            return super.waitFor(timeout, unit);
          }

          @Override
          public int exitValue() {
            int ec = exitCode.get();
            if (ec < 0) {
              throw new IllegalThreadStateException();
            }
            return ec;
          }

          @Override
          public void destroy() {
            exitCode.set(42);
          }

          @Override
          public Process destroyForcibly() {
            exitCode.set(42);
            return this;
          }
        };

    long timeToUrl = 500;

    ProcessHandler ph =
        new ProcessHandler()
            .setStderrTarget(stderrLines::add)
            .setStdoutTarget(stdoutLines::add)
            .setClock(clock::get)
            .setTimeToListenUrlMillis(timeToUrl)
            .setTimeStopMillis(42);
  }
}
