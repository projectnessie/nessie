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
package org.projectnessie.versioned.persist.adapter.spi;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import javax.annotation.Nonnull;

public final class DatabaseAdapterMetrics {

  private DatabaseAdapterMetrics() {}

  static void tryLoopFinished(@Nonnull String result, int retries, long durationNanos) {
    tryLoopCounts(result).increment();
    tryLoopRetries(result).increment(retries);
    tryLoopDuration(result).record(durationNanos, NANOSECONDS);
  }

  public static Timer tryLoopDuration(@Nonnull String result) {
    return Timer.builder("nessie.databaseadapter.tryloop.duration")
        .tag("result", result)
        .register(Metrics.globalRegistry);
  }

  public static Counter tryLoopRetries(@Nonnull String result) {
    return Counter.builder("nessie.databaseadapter.tryloop.retries")
        .tag("result", result)
        .register(Metrics.globalRegistry);
  }

  public static Counter tryLoopCounts(@Nonnull String result) {
    return Counter.builder("nessie.databaseadapter.tryloop.count")
        .tag("result", result)
        .register(Metrics.globalRegistry);
  }
}
