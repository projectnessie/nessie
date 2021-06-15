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

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Accepts {@link String}s via it's {@link #accept(String)} method and checks for the {@code
 * Listening on: http...} pattern.
 */
final class ListenUrlWaiter implements Consumer<String> {

  private static final Pattern HTTP_PORT_LOG_PATTERN =
      Pattern.compile("^.*Listening on: (http[s]?://[^ ]*)$");
  static final String TIMEOUT_MESSAGE =
      "Did not get the http(s) listen URL from the console output.";
  private static final long MAX_ITER_WAIT_NANOS = TimeUnit.MILLISECONDS.toNanos(50);

  private final LongSupplier clock;
  private final Consumer<String> stdoutTarget;
  private final long deadlineListenUrl;

  private final CompletableFuture<String> listenUrl = new CompletableFuture<>();

  /**
   * Construct a new instance to wait for Quarkus' {@code Listening on: ...} message.
   *
   * @param clock monotonic clock, nanoseconds
   * @param timeToListenUrlMillis timeout in millis, the "Listen on: ..." must be received within
   *     this time (otherwise it will fail)
   * @param stdoutTarget "real" target for "stdout"
   */
  ListenUrlWaiter(LongSupplier clock, long timeToListenUrlMillis, Consumer<String> stdoutTarget) {
    this.clock = clock;
    this.stdoutTarget = stdoutTarget;
    this.deadlineListenUrl =
        clock.getAsLong() + TimeUnit.MILLISECONDS.toNanos(timeToListenUrlMillis);
  }

  public void accept(String line) {
    if (!listenUrl.isDone()) {
      Matcher m = HTTP_PORT_LOG_PATTERN.matcher(line);
      if (m.matches()) {
        listenUrl.complete(m.group(1));
      }
    }
    stdoutTarget.accept(line);
  }

  String peekListenUrl() {
    try {
      return listenUrl.isDone() ? listenUrl.get() : null;
    } catch (Exception e) {
      throw new RuntimeException();
    }
  }

  /**
   * Get the first captured {@code Listening on: http...} pattern.
   *
   * @return the captured listen URL or {@code null} if none has been found (so far).
   */
  String getListenUrl() throws InterruptedException, TimeoutException {
    while (true) {
      long remainingNanos = remainingNanos();
      // must succeed if the listen-url has been captured, even if it's called after the timeout has
      // elapsed
      if (remainingNanos < 0 && !listenUrl.isDone()) {
        throw new TimeoutException(TIMEOUT_MESSAGE);
      }

      try {
        return listenUrl.get(Math.min(MAX_ITER_WAIT_NANOS, remainingNanos), TimeUnit.NANOSECONDS);
      } catch (TimeoutException e) {
        // Continue, check above.
        // This "short get()" is implemented to make the unit test TestListenUrlWaiter.noTimeout()
        // run faster.
      } catch (CancellationException e) {
        throw new TimeoutException(TIMEOUT_MESSAGE);
      } catch (ExecutionException e) {
        if (e.getCause() instanceof RuntimeException) {
          throw (RuntimeException) e.getCause();
        } else {
          throw new RuntimeException(e.getCause());
        }
      }
    }
  }

  void cancel() {
    listenUrl.cancel(false);
  }

  private long remainingNanos() {
    return deadlineListenUrl - clock.getAsLong();
  }

  boolean isTimeout() {
    if (listenUrl.isDone() && !listenUrl.isCompletedExceptionally()) {
      return false;
    }
    return remainingNanos() < 0;
  }
}
