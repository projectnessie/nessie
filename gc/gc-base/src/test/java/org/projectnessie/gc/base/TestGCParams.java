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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_URI;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class TestGCParams {

  @Test
  void testInvalidValues() {
    ImmutableGCParams.Builder builder = ImmutableGCParams.builder();

    final Map<String, String> options = new HashMap<>();
    options.put(CONF_NESSIE_URI, "someURI");

    Instant futureTime = Instant.now().plus(10, ChronoUnit.DAYS);
    Instant validTime = Instant.now().minus(1, ChronoUnit.HOURS);

    final Map<String, Instant> perRefCutoffTime = new HashMap<>();
    perRefCutoffTime.put("branch1", futureTime);

    // invalid defaultCutOffTimestamp
    builder
        .defaultCutOffTimestamp(futureTime)
        .nessieCatalogName("nessie")
        .outputBranchName("gcBranch")
        .outputTableIdentifier("db.gc_results")
        .nessieClientConfigs(options);
    //
    assertThatThrownBy(builder::build)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cutoff time cannot be from the future:");

    // invalid deadReferenceCutOffTimeStamp
    builder.defaultCutOffTimestamp(validTime).deadReferenceCutOffTimeStamp(futureTime);
    //
    assertThatThrownBy(builder::build)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Dead Reference cutoff time cannot be from the future:");

    // invalid cutOffTimestampPerRef
    builder.deadReferenceCutOffTimeStamp(validTime).cutOffTimestampPerRef(perRefCutoffTime);
    //
    assertThatThrownBy(builder::build)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Reference cutoff time for branch1 cannot be from the future:");

    perRefCutoffTime.put("branch1", validTime);

    // invalid partitionsCount
    builder.cutOffTimestampPerRef(perRefCutoffTime).sparkPartitionsCount(-3);
    //
    assertThatThrownBy(builder::build)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("partitionsCount has invalid value: -3");

    // invalid bloomFilterExpectedEntries
    builder.sparkPartitionsCount(5).bloomFilterExpectedEntries(-3L);
    //
    assertThatThrownBy(builder::build)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("bloomFilterExpectedEntries has invalid value: -3");

    // invalid partitionsCount
    builder.bloomFilterExpectedEntries(10L).bloomFilterFpp(1);
    //
    assertThatThrownBy(builder::build)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("bloomFilterFpp has invalid value: 1.0");
  }
}
