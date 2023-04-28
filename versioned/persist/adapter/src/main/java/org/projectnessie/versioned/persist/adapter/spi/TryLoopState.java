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

import static org.projectnessie.versioned.persist.adapter.spi.Traced.trace;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.projectnessie.versioned.ReferenceRetryFailureException;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapterConfig;

/** Retry-logic for attempts for compare-and-swap-like operations. */
public class TryLoopState<R> implements AutoCloseable {

  private static final String TAG_ATTEMPT = "try-loop.attempt";
  private static final String TAG_RETRIES = "try-loop.retries";
  private static final String TAG_ENDLESS_RETRIES = "try-loop.endless-retries";

  private Traced traced;
  private final String opName;
  private final MonotonicClock monotonicClock;
  private final long t0;
  private final long maxTime;
  private final int maxRetries;
  private final Function<TryLoopState<R>, String> retryErrorMessage;
  private final BiConsumer<Boolean, TryLoopState<R>> completionNotifier;
  private final long maxSleep;
  private final long initialLowerBound;
  private final long initialUpperBound;
  private long lowerBound;
  private long upperBound;
  private int retries;
  private int endlessRetries;
  private boolean unsuccessful;

  TryLoopState(
      String opName,
      Function<TryLoopState<R>, String> retryErrorMessage,
      DatabaseAdapterConfig config,
      MonotonicClock monotonicClock,
      BiConsumer<Boolean, TryLoopState<R>> completionNotifier) {
    this.opName = opName;
    this.retryErrorMessage = retryErrorMessage;
    this.maxTime = TimeUnit.MILLISECONDS.toNanos(config.getCommitTimeout());
    this.maxRetries = config.getCommitRetries();
    this.monotonicClock = monotonicClock;
    this.t0 = monotonicClock.currentNanos();
    this.lowerBound = this.initialLowerBound = config.getRetryInitialSleepMillisLower();
    this.upperBound = this.initialUpperBound = config.getRetryInitialSleepMillisUpper();
    this.maxSleep = config.getRetryMaxSleepMillis();
    this.completionNotifier = completionNotifier;
    start();
  }

  public static <R> TryLoopState<R> newTryLoopState(
      String opName,
      Function<TryLoopState<R>, String> retryErrorMessage,
      BiConsumer<Boolean, TryLoopState<R>> completionNotifier,
      DatabaseAdapterConfig config) {
    return new TryLoopState<>(
        "try-loop." + opName,
        retryErrorMessage,
        config,
        DefaultMonotonicClock.INSTANCE,
        completionNotifier);
  }

  public int getRetries() {
    return retries;
  }

  public int getEndlessRetries() {
    return endlessRetries;
  }

  public int getTotalRetries() {
    return retries + endlessRetries;
  }

  public long getDuration(TimeUnit timeUnit) {
    return timeUnit.convert(monotonicClock.currentNanos() - t0, TimeUnit.NANOSECONDS);
  }

  /**
   * Called when the operation succeeded. The current implementation is rather a no-op, but can be
   * used to track/trace/monitor successes.
   */
  public R success(R result) {
    completionNotifier.accept(true, this);
    return result;
  }

  private ReferenceRetryFailureException unsuccessful() {
    completionNotifier.accept(false, this);
    return new ReferenceRetryFailureException(
        retryErrorMessage.apply(this),
        this.getTotalRetries(),
        this.getDuration(TimeUnit.MILLISECONDS));
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
    if (unsuccessful) {
      throw unsuccessful();
    }

    stop();

    retries++;

    long current = monotonicClock.currentNanos();
    long elapsed = current - t0;

    if (maxTime < elapsed || maxRetries < retries + endlessRetries) {
      unsuccessful = true;
      throw unsuccessful();
    }

    sleepAndBackoff(elapsed);

    start();
  }

  public void resetBounds() {
    lowerBound = initialLowerBound;
    upperBound = initialUpperBound;
  }

  /**
   * Similar to {@link #retry()}, but never throws a {@link ReferenceRetryFailureException} (unless
   * a preceding call to {@link #retry()} already raised it).
   */
  public void retryEndless() throws ReferenceRetryFailureException {
    if (unsuccessful) {
      throw unsuccessful();
    }

    stop();

    endlessRetries++;

    sleepAndBackoff(0L);

    start();
  }

  private void sleepAndBackoff(long elapsed) {
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
    stop();
  }

  /** Abstracts {@code System.nanoTime()} and {@code Thread.sleep()} for testing purposes. */
  interface MonotonicClock {
    long currentNanos();

    void sleepMillis(long nanos);
  }

  static final class DefaultMonotonicClock implements MonotonicClock {

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

  private void start() {
    traced = trace(opName).tag(TAG_ATTEMPT, getTotalRetries());
  }

  private void stop() {
    traced.tag(TAG_RETRIES, getRetries()).tag(TAG_ENDLESS_RETRIES, getEndlessRetries()).close();
  }
}
