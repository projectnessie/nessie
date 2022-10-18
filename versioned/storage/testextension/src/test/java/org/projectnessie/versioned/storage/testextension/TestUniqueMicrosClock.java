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

import static java.time.Clock.fixed;
import static java.time.Instant.ofEpochSecond;
import static java.time.ZoneOffset.UTC;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.time.Clock;
import java.time.Instant;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.versioned.storage.common.config.StoreConfig;
import org.threeten.extra.MutableClock;

@ExtendWith(SoftAssertionsExtension.class)
public class TestUniqueMicrosClock {

  static final long ONE_SEC_IN_MICROS = SECONDS.toMicros(1);
  static final long HALF_SEC_IN_MICROS = MILLISECONDS.toMicros(500);

  @InjectSoftAssertions protected SoftAssertions soft;

  @Test
  public void wrapNonSecond() {
    Clock clock = new UniqueMicrosClock(fixed(ofEpochSecond(0L, 0L), UTC));
    StoreConfig.Adjustable config = StoreConfig.Adjustable.empty().withClock(clock);

    long now = config.currentTimeMicros();
    long next = config.currentTimeMicros();
    soft.assertThat(next).isEqualTo(now + 1);
  }

  @Test
  public void wrapSecond() {
    long nanosBeforeWrapAround = MICROSECONDS.toNanos(ONE_SEC_IN_MICROS - 1);
    Clock clock = new UniqueMicrosClock(fixed(ofEpochSecond(0L, nanosBeforeWrapAround), UTC));
    StoreConfig.Adjustable config = StoreConfig.Adjustable.empty().withClock(clock);

    config.currentTimeMicros();
    long next = config.currentTimeMicros();
    soft.assertThat(next).isEqualTo(ONE_SEC_IN_MICROS);
  }

  @Test
  public void clockBackwardsMicros() {
    MutableClock base = MutableClock.of(Instant.now(), UTC);
    Clock clock = new UniqueMicrosClock(base);
    StoreConfig.Adjustable config = StoreConfig.Adjustable.empty().withClock(clock);

    base.setInstant(ofEpochSecond(0, MICROSECONDS.toNanos(HALF_SEC_IN_MICROS)));
    soft.assertThat(config.currentTimeMicros()).isEqualTo(HALF_SEC_IN_MICROS);

    base.setInstant(ofEpochSecond(0, MICROSECONDS.toNanos(HALF_SEC_IN_MICROS - 1)));
    soft.assertThat(config.currentTimeMicros()).isEqualTo(HALF_SEC_IN_MICROS + 1);
  }

  @Test
  public void clockBackwardsSecs() {
    MutableClock base = MutableClock.of(Instant.now(), UTC);
    Clock clock = new UniqueMicrosClock(base);
    StoreConfig.Adjustable config = StoreConfig.Adjustable.empty().withClock(clock);

    base.setInstant(ofEpochSecond(1, 0));
    soft.assertThat(config.currentTimeMicros()).isEqualTo(ONE_SEC_IN_MICROS);

    base.setInstant(ofEpochSecond(0, 0));
    soft.assertThat(config.currentTimeMicros()).isEqualTo(ONE_SEC_IN_MICROS + 1);
  }
}
