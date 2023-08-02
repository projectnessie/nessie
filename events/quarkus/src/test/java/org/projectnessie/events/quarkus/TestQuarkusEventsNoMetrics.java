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
package org.projectnessie.events.quarkus;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.projectnessie.events.quarkus.assertions.EventAssertions;
import org.projectnessie.events.quarkus.assertions.MetricsAssertions;
import org.projectnessie.events.quarkus.assertions.TracingAssertions;
import org.projectnessie.events.quarkus.scenarios.EventScenarios;

@QuarkusTest
@TestProfile(TestQuarkusEventsNoMetrics.Profile.class)
class TestQuarkusEventsNoMetrics {

  @Inject EventScenarios scenarios;
  @Inject EventAssertions events;
  @Inject TracingAssertions tracing;
  @Inject MetricsAssertions metrics;

  @AfterEach
  void reset() {
    events.reset();
    tracing.reset();
    metrics.reset();
  }

  @Test
  void testCommitNoMetrics() {
    scenarios.commit();
    events.awaitAndAssertCommitEvents(true);
    tracing.awaitAndAssertCommitTraces(true);
    metrics.assertMicrometerDisabled();
  }

  @Test
  public void testMergeNoMetrics() {
    scenarios.merge();
    events.awaitAndAssertMergeEvents(true);
    tracing.awaitAndAssertMergeTraces(true);
    metrics.assertMicrometerDisabled();
  }

  @Test
  public void testTransplantNoMetrics() {
    scenarios.transplant();
    events.awaitAndAssertTransplantEvents(true);
    tracing.awaitAndAssertTransplantTraces(true);
    metrics.assertMicrometerDisabled();
  }

  @Test
  public void testReferenceCreatedNoMetrics() {
    scenarios.referenceCreated();
    events.awaitAndAssertReferenceCreatedEvents(true);
    tracing.awaitAndAssertReferenceCreatedTraces(true);
    metrics.assertMicrometerDisabled();
  }

  @Test
  public void testReferenceDeletedNoMetrics() {
    scenarios.referenceDeleted();
    events.awaitAndAssertReferenceDeletedEvents(true);
    tracing.awaitAndAssertReferenceDeletedTraces(true);
    metrics.assertMicrometerDisabled();
  }

  @Test
  public void testReferenceUpdatedNoMetrics() {
    scenarios.referenceUpdated();
    events.awaitAndAssertReferenceUpdatedEvents(true);
    tracing.awaitAndAssertReferenceUpdatedTraces(true);
    metrics.assertMicrometerDisabled();
  }

  public static class Profile extends TestQuarkusEvents.Profile {

    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> map = new HashMap<>(super.getConfigOverrides());
      map.put("quarkus.micrometer.enabled", "false");
      return map;
    }
  }
}
