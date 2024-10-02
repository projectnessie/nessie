/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.versioned.storage.cleanup;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
public class TestPurgeStatsBuilder {
  @InjectSoftAssertions SoftAssertions soft;

  @Test
  void purgeStatsBuilder() {
    var builder = new PurgeStatsBuilder();

    var expected = ImmutablePurgeStats.builder();

    builder.started = Instant.EPOCH;
    expected.started(Instant.EPOCH);
    builder.ended = Instant.EPOCH.plus(42, ChronoUnit.DAYS);
    expected.ended(Instant.EPOCH.plus(42, ChronoUnit.DAYS));
    builder.numPurgedObjs = 1;
    expected.numPurgedObjs(1);
    builder.numScannedObjs = 2;
    expected.numScannedObjs(2);
    builder.failure = new Exception("hello");
    expected.failure(builder.failure);

    soft.assertThat(builder.build()).isEqualTo(expected.build());
  }
}
