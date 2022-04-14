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
package org.projectnessie.gc.base;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestInstantConversion {

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testInstantConversion(boolean isNanoPrecision) {
    Instant instant =
        isNanoPrecision
            ? Instant.parse("2018-08-19T16:02:42.123456789Z")
            : Instant.parse("2018-08-19T16:02:42.123456Z");
    long time = instant.getEpochSecond();
    long nano = instant.getNano();
    // current time in microseconds since epoch
    long micro = TimeUnit.SECONDS.toMicros(time) + TimeUnit.NANOSECONDS.toMicros(nano);
    Instant instantFromMicros = GCUtil.getInstantFromMicros(micro);
    // Instant is capable of nanosecond precision.
    // Hence, convert to microsecond precision before comparing.
    Instant instantInMicros = instant.truncatedTo(ChronoUnit.MICROS);
    assertThat(instantFromMicros).isEqualTo(instantInMicros);
  }
}
