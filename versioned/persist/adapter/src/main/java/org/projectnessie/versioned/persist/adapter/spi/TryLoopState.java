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
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ReferenceRetryFailureException;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapterConfig;

/** Retry-logic for attempts for compare-and-swap-like operations. */
public final class TryLoopState implements AutoCloseable {

  private final MonotonicClock monotonicClock;
  private final long t0;
  private final long maxTime;
  private final int maxRetries;
  private final Function<TryLoopState, String> retryErrorMessage;
  private final BiConsumer<Boolean, TryLoopState> completionNotifier;
  private final long maxSleep;
  private long lowerBound;
  private long upperBound;
  private int retries;

  TryLoopState(
      Function<TryLoopState, String> retryErrorMessage,
      DatabaseAdapterConfig config,
      MonotonicClock monotonicClock,
      BiConsumer<Boolean, TryLoopState> completionNotifier) {
    this.retryErrorMessage = retryErrorMessage;
    this.maxTime = TimeUnit.MILLISECONDS.toNanos(config.getCommitTimeout());
    this.maxRetries = config.getCommitRetries();
    this.monotonicClock = monotonicClock;
    this.t0 = monotonicClock.currentNanos();
    this.lowerBound = config.getRetryInitialSleepMillisLower();
    this.upperBound = config.getRetryInitialSleepMillisUpper();
    this.maxSleep = config.getRetryMaxSleepMillis();
    this.completionNotifier = completionNotifier;
  }

  public static TryLoopState newTryLoopState(
      Function<TryLoopState, String> retryErrorMessage,
      BiConsumer<Boolean, TryLoopState> completionNotifier,
      DatabaseAdapterConfig config) {
    return new TryLoopState(
        retryErrorMessage, config, DefaultMonotonicClock.INSTANCE, completionNotifier);
  }

  public int getRetries() {
    return retries;
  }

  public long getDuration(TimeUnit timeUnit) {
    return timeUnit.convert(monotonicClock.currentNanos() - t0, TimeUnit.NANOSECONDS);
  }

  /**
   * Called when the operation succeeded. The current implementation is rather a no-op, but can be
   * used to track/trace/monitor successes.
   */
  public Hash success(Hash result) {
    completionNotifier.accept(true, this);
    return result;
  }

  private ReferenceRetryFailureException unsuccessful() {
    completionNotifier.accept(false, this);
    return new ReferenceRetryFailureException(
        retryErrorMessage.apply(this), this.getRetries(), this.getDuration(TimeUnit.MILLISECONDS));
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
      throw unsuccessful();
    }

    long sleepMillis = ThreadLocalRandom.current().nextLong(lowerBound, upperBound);

    // Prevent that we "sleep" too long and exceed 'maxTime'
    sleepMillis = Math.min(TimeUnit.NANOSECONDS.toMillis(maxTime - elapsed), sleepMillis);

    monotonicClock.sleepMillis(sleepMillis);

    long upper = upperBound * 2;
    if (upper <= maxSleep) {
      lowerBound *= 2;
      upperBound = upper;
    }
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
