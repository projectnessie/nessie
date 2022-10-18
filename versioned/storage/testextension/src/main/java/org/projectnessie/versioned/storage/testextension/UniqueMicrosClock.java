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
package org.projectnessie.versioned.storage.testextension;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.TimeUnit;

/**
 * {@link Clock} that ensures that every call to {@link #instant()} returns a different and higher
 * value (monotonically increasing), ticking at least 1 microsecond. It helps preventing flaky
 * tests.
 *
 * <p>This special test-clock is particularly useful to perform assertions on exact {@link
 * org.projectnessie.versioned.storage.common.objtypes.CommitObj} instances, considering the
 * created-time in microseconds since epoch.
 */
final class UniqueMicrosClock extends Clock {

  static final long MICROS_PER_SECOND = SECONDS.toMicros(1);
  private final Clock clock;

  private long lastSec;
  private int lastMicro;

  UniqueMicrosClock(Clock clock) {
    this.clock = clock;
  }

  UniqueMicrosClock() {
    this(Clock.systemUTC());
  }

  @Override
  public ZoneId getZone() {
    return clock.getZone();
  }

  @Override
  public Clock withZone(ZoneId zone) {
    return new UniqueMicrosClock(clock.withZone(zone));
  }

  @Override
  public synchronized Instant instant() {
    Instant i = clock.instant();
    long sec = i.getEpochSecond();
    int micro = (int) NANOSECONDS.toMicros(i.getNano());
    if (sec < lastSec) {
      sec = lastSec;
    }
    if (sec == lastSec) {
      if (micro <= lastMicro) {
        micro = lastMicro + 1;
        if (micro >= MICROS_PER_SECOND) {
          micro -= MICROS_PER_SECOND;
          sec++;
        }
        i = Instant.ofEpochSecond(sec, TimeUnit.MICROSECONDS.toNanos(micro));
      }
    }
    lastSec = sec;
    lastMicro = micro;
    return i;
  }
}
