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
@TestProfile(TestQuarkusEventsNoAuth.Profile.class)
class TestQuarkusEventsNoAuth {

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
  public void testCommitWithAuthDisabled() {
    scenarios.commit();
    events.awaitAndAssertCommitEvents(false);
    tracing.awaitAndAssertCommitTraces(false);
    metrics.awaitAndAssertCommitMetrics();
  }

  @Test
  public void testMergeWithAuthDisabled() {
    scenarios.merge();
    events.awaitAndAssertMergeEvents(false);
    tracing.awaitAndAssertMergeTraces(false);
    metrics.awaitAndAssertMergeMetrics();
  }

  @Test
  public void testTransplantWithAuthDisabled() {
    scenarios.transplant();
    events.awaitAndAssertTransplantEvents(false);
    tracing.awaitAndAssertTransplantTraces(false);
    metrics.awaitAndAssertTransplantMetrics();
  }

  @Test
  public void testReferenceCreatedWithAuthDisabled() {
    scenarios.referenceCreated();
    events.awaitAndAssertReferenceCreatedEvents(false);
    tracing.awaitAndAssertReferenceCreatedTraces(false);
    metrics.awaitAndAssertReferenceCreatedMetrics();
  }

  @Test
  public void testReferenceDeletedWithAuthDisabled() {
    scenarios.referenceDeleted();
    events.awaitAndAssertReferenceDeletedEvents(false);
    tracing.awaitAndAssertReferenceDeletedTraces(false);
    metrics.awaitAndAssertReferenceDeletedMetrics();
  }

  @Test
  public void testReferenceUpdatedWithAuthDisabled() {
    scenarios.referenceUpdated();
    events.awaitAndAssertReferenceUpdatedEvents(false);
    tracing.awaitAndAssertReferenceUpdatedTraces(false);
    metrics.awaitAndAssertReferenceUpdatedMetrics();
  }

  public static class Profile extends TestQuarkusEvents.Profile {

    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> map = new HashMap<>(super.getConfigOverrides());
      map.put("nessie.server.authentication.enabled", "false");
      return map;
    }
  }
}
