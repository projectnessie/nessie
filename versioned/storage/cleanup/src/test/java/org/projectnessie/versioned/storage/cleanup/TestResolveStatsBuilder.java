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
public class TestResolveStatsBuilder {
  @InjectSoftAssertions SoftAssertions soft;

  @Test
  void resolveStatsBuilder() {
    var builder = new ResolveStatsBuilder();

    var expected = ImmutableResolveStats.builder();

    builder.mustRestart = true;
    expected.mustRestart(true);
    builder.started = Instant.EPOCH;
    expected.started(Instant.EPOCH);
    builder.ended = Instant.EPOCH.plus(42, ChronoUnit.DAYS);
    expected.ended(Instant.EPOCH.plus(42, ChronoUnit.DAYS));
    builder.numCommits = 1;
    expected.numCommits(1);
    builder.numContents = 2;
    expected.numContents(2);
    builder.numObjs = 3;
    expected.numObjs(3);
    builder.numReferences = 4;
    expected.numReferences(4);
    builder.numUniqueCommits = 5;
    expected.numUniqueCommits(5);
    builder.numCommitChainHeads = 7;
    expected.numCommitChainHeads(7);
    builder.numQueuedObjs = 8;
    expected.numQueuedObjs(8);
    builder.numQueuedObjsBulkFetches = 9;
    expected.numQueuedObjsBulkFetches(9);
    builder.failure = new Exception("hello");
    expected.failure(builder.failure);

    soft.assertThat(builder.build()).isEqualTo(expected.build());
  }
}
