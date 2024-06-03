/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.versioned.storage.common.logic;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.projectnessie.versioned.storage.common.logic.CommitRetry.TryLoopState.newTryLoopState;

import com.google.common.annotations.VisibleForTesting;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import org.projectnessie.versioned.storage.common.config.StoreConfig;
import org.projectnessie.versioned.storage.common.exceptions.CommitConflictException;
import org.projectnessie.versioned.storage.common.exceptions.CommitWrappedException;
import org.projectnessie.versioned.storage.common.exceptions.RetryTimeoutException;
import org.projectnessie.versioned.storage.common.exceptions.UnknownOperationResultException;
import org.projectnessie.versioned.storage.common.persist.Persist;

public class CommitRetry {

  private static final String OTEL_SLEEP_EVENT_NAME = "nessie.commit-retry.sleep";
  private static final AttributeKey<Long> OTEL_SLEEP_EVENT_TIME_KEY =
      AttributeKey.longKey("nessie.commit-retry.sleep.duration-millis");

  private CommitRetry() {}

  public static <T> T commitRetry(Persist persist, CommitAttempt<T> attempt)
      throws CommitWrappedException, CommitConflictException, RetryTimeoutException {
    return commitRetry(persist, attempt, newTryLoopState(persist));
  }

  @VisibleForTesting
  static <T> T commitRetry(Persist persist, CommitAttempt<T> attempt, TryLoopState tls)
      throws CommitWrappedException, CommitConflictException, RetryTimeoutException {
    Optional<?> retryState = Optional.empty();

    long t0 = tls.currentNanos();
    long t1 = t0;
    for (int i = 0; true; i++, t1 = tls.currentNanos()) {
      try {
        return attempt.attempt(persist, retryState);
      } catch (RetryException e) {
        if (!tls.retry(t1)) {
          throw new RetryTimeoutException(i, tls.currentNanos() - t0);
        }
        retryState = e.retryState();
      } catch (UnknownOperationResultException e) {
        if (!tls.retry(t1)) {
          throw new RetryTimeoutException(i, tls.currentNanos() - t0);
        }
      }
    }
  }

  public static final class RetryException extends Exception {
    private final Optional<?> retryState;

    public RetryException() {
      this(Optional.empty());
    }

    public RetryException(@Nonnull Optional<?> retryState) {
      this.retryState = retryState;
    }

    public Optional<?> retryState() {
      return retryState;
    }
  }

  @FunctionalInterface
  public interface CommitAttempt<T> {

    /**
     * Attempt a retryable operation.
     *
     * @param persist The {@link Persist} instance to use for the operation attempt.
     * @param retryState The initial call to this function will receive an empty value, subsequent
     *     calls receive the parameter passed to the previous {@link RetryException}.
     */
    T attempt(@Nonnull Persist persist, @Nonnull Optional<?> retryState)
        throws CommitWrappedException, CommitConflictException, RetryException;
  }

  static final class TryLoopState {

    private final MonotonicClock monotonicClock;
    private final long t0;
    private final long maxTime;
    private final int maxRetries;
    private final long maxSleep;
    private long lowerBound;
    private long upperBound;
    private int retries;
    private boolean unsuccessful;

    TryLoopState(StoreConfig config, MonotonicClock monotonicClock) {
      this.maxTime = MILLISECONDS.toNanos(config.commitTimeoutMillis());
      this.maxRetries = config.commitRetries();
      this.monotonicClock = monotonicClock;
      this.t0 = monotonicClock.currentNanos();
      this.lowerBound = config.retryInitialSleepMillisLower();
      this.upperBound = config.retryInitialSleepMillisUpper();
      this.maxSleep = config.retryMaxSleepMillis();
    }

    public static TryLoopState newTryLoopState(Persist persist) {
      return new TryLoopState(
          persist.config(),
          new MonotonicClock() {
            @Override
            public long currentNanos() {
              return System.nanoTime();
            }

            @Override
            public void sleepMillis(long millis) {
              try {
                Thread.sleep(millis);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            }
          });
    }

    long currentNanos() {
      return monotonicClock.currentNanos();
    }

    public boolean retry(long timeAttemptStarted) {
      if (unsuccessful) {
        return false;
      }

      retries++;

      long current = currentNanos();
      long totalElapsed = current - t0;
      long attemptElapsed = timeAttemptStarted - current;

      if (maxTime < totalElapsed || maxRetries < retries) {
        unsuccessful = true;
        return false;
      }

      sleepAndBackoff(totalElapsed, attemptElapsed);

      return true;
    }

    private void sleepAndBackoff(long totalElapsed, long attemptElapsed) {
      long lower = lowerBound;
      long upper = upperBound;
      long sleepMillis =
          lower == upper ? lower : ThreadLocalRandom.current().nextLong(lower, upper);

      // Prevent that we "sleep" too long and exceed 'maxTime'
      sleepMillis = Math.min(NANOSECONDS.toMillis(maxTime - totalElapsed), sleepMillis);

      // consider the already elapsed time of the last attempt
      sleepMillis = Math.max(1L, sleepMillis - NANOSECONDS.toMillis(attemptElapsed));

      Span.current()
          .addEvent(OTEL_SLEEP_EVENT_NAME, Attributes.of(OTEL_SLEEP_EVENT_TIME_KEY, sleepMillis));

      monotonicClock.sleepMillis(sleepMillis);

      upper = upper * 2;
      long max = maxSleep;
      if (upper <= max) {
        lowerBound *= 2;
        upperBound = upper;
      } else {
        upperBound = max;
      }
    }

    /** Abstracts {@code System.nanoTime()} and {@code Thread.sleep()} for testing purposes. */
    interface MonotonicClock {
      long currentNanos();

      void sleepMillis(long millis);
    }
  }
}
