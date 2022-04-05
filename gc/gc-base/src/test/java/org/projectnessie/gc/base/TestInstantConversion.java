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
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

public class TestInstantConversion {

  @Test
  void testInstantConversion() {
    Instant now = Instant.now();
    long time = now.getEpochSecond();
    long nano = now.getNano();
    // current time in microseconds since epoch
    long micro = TimeUnit.SECONDS.toMicros(time) + TimeUnit.NANOSECONDS.toMicros(nano);
    Instant instantFromMicros = GCUtil.getInstantFromMicros(micro);
    // Even though instant is capable of nanosecond precision,
    // Instant.now() gives micro second precision by default.
    // Because of that the below validation can pass.
    assertThat(instantFromMicros).isEqualTo(now);
  }
}
