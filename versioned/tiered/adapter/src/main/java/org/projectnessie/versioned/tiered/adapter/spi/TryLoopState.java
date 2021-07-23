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
package org.projectnessie.versioned.tiered.adapter.spi;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceRetryFailureException;
import org.projectnessie.versioned.tiered.adapter.DatabaseAdapterConfig;

/** Retry-logic for attempts for compare-and-swap-like operations. */
public final class TryLoopState implements AutoCloseable {

  private final long t0 = System.nanoTime();
  private final long maxTime;
  private final int maxRetries;
  private int retries;
  private final Supplier<String> retryErrorMessage;

  private long lowerBound = 5;
  private long upperBound = 25;

  private TryLoopState(Supplier<String> retryErrorMessage, DatabaseAdapterConfig config) {
    this.retryErrorMessage = retryErrorMessage;
    this.maxTime = TimeUnit.MILLISECONDS.toNanos(config.getCommitTimeout());
    this.maxRetries = config.getCommitRetries();
  }

  public static TryLoopState newTryLoopState(
      Supplier<String> retryErrorMessage, DatabaseAdapterConfig config) {
    return new TryLoopState(retryErrorMessage, config);
  }

  public Hash success(Hash result) {
    return result;
  }

  public void retry() throws ReferenceConflictException {
    retries++;
    long t = System.nanoTime() - t0;
    if (maxTime < t || maxRetries <= retries) {
      throw new ReferenceRetryFailureException(retryErrorMessage.get());
    }

    long sleepMillis = ThreadLocalRandom.current().nextLong(lowerBound, upperBound);

    // Prevent that we "sleep" too long and exceed 'maxTime'
    sleepMillis = Math.min(TimeUnit.NANOSECONDS.toMillis(maxTime - t), sleepMillis);

    try {
      Thread.sleep(sleepMillis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    lowerBound *= 2;
    upperBound *= 2;
  }

  @Override
  public void close() {
    // Can detect success/failed/too-many-retries here, if needed.
  }
}
