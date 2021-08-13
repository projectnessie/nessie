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
package org.projectnessie.versioned.persist.adapter.spi;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ReferenceRetryFailureException;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapterConfig;

/** Retry-logic for attempts for compare-and-swap-like operations. */
public final class TryLoopState implements AutoCloseable {

  static final int INITIAL_LOWER_BOUND = 5;
  static final int INITIAL_UPPER_BOUND = 25;

  private final MonotonicClock monotonicClock;
  private final long t0;
  private final long maxTime;
  private final int maxRetries;
  private int retries;
  private final Supplier<String> retryErrorMessage;

  private long lowerBound = INITIAL_LOWER_BOUND;
  private long upperBound = INITIAL_UPPER_BOUND;

  TryLoopState(
      Supplier<String> retryErrorMessage,
      DatabaseAdapterConfig config,
      MonotonicClock monotonicClock) {
    this.retryErrorMessage = retryErrorMessage;
    this.maxTime = TimeUnit.MILLISECONDS.toNanos(config.getCommitTimeout());
    this.maxRetries = config.getCommitRetries();
    this.monotonicClock = monotonicClock;
    this.t0 = monotonicClock.currentNanos();
  }

  public static TryLoopState newTryLoopState(
      Supplier<String> retryErrorMessage, DatabaseAdapterConfig config) {
    return new TryLoopState(retryErrorMessage, config, DefaultMonotonicClock.INSTANCE);
  }

  /**
   * Called when the operation succeeded. The current implementation is rather a no-op, but can be
   * used to track/trace/monitor successes.
   */
  public Hash success(Hash result) {
    return result;
  }

  /**
   * Called when a database-adapter operation needs to retry due to a concurrent-modification / CAS
   * failure.
   *
   * <p>If this method just returns, the called can safely retry. If this method throws a {@link
   * ReferenceRetryFailureException}, the {@link DatabaseAdapterConfig#getCommitTimeout() total
   * time} and/or maximum {@link DatabaseAdapterConfig#getCommitRetries() allowed number of retries}
   * exceeded the configured values.
   */
  public void retry() throws ReferenceRetryFailureException {
    retries++;

    long current = monotonicClock.currentNanos();
    long elapsed = current - t0;

    if (maxTime < elapsed || maxRetries < retries) {
      throw new ReferenceRetryFailureException(retryErrorMessage.get());
    }

    long sleepMillis = ThreadLocalRandom.current().nextLong(lowerBound, upperBound);

    // Prevent that we "sleep" too long and exceed 'maxTime'
    sleepMillis = Math.min(TimeUnit.NANOSECONDS.toMillis(maxTime - elapsed), sleepMillis);

    monotonicClock.sleepMillis(sleepMillis);

    lowerBound *= 2;
    upperBound *= 2;
  }

  @Override
  public void close() {
    // Can detect success/failed/too-many-retries here, if needed.
  }

  /** Abstracts {@code System.nanoTime()} and {@code Thread.sleep()} for testing purposes. */
  interface MonotonicClock {
    long currentNanos();

    void sleepMillis(long nanos);
  }

  static class DefaultMonotonicClock implements MonotonicClock {

    static final MonotonicClock INSTANCE = new DefaultMonotonicClock();

    private DefaultMonotonicClock() {}

    @Override
    public long currentNanos() {
      return System.nanoTime();
    }

    @Override
    public void sleepMillis(long sleepMillis) {
      try {
        Thread.sleep(sleepMillis);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
