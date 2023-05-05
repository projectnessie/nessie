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
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import java.util.HashMap;
import java.util.Map;
import javax.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.projectnessie.events.quarkus.assertions.EventAssertions;
import org.projectnessie.events.quarkus.assertions.MetricsAssertions;
import org.projectnessie.events.quarkus.assertions.TracingAssertions;
import org.projectnessie.events.quarkus.scenarios.EventScenarios;

@QuarkusTest
@TestProfile(TestQuarkusEvents.Profile.class)
class TestQuarkusEvents {

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
  public void testCommit() {
    scenarios.commit();
    events.awaitAndAssertCommitEvents(true);
    tracing.assertNoNessieTraces();
    metrics.assertNoNessieMetrics();
  }

  @Test
  public void testMerge() {
    scenarios.merge();
    events.awaitAndAssertMergeEvents(true);
    tracing.assertNoNessieTraces();
    metrics.assertNoNessieMetrics();
  }

  @Test
  public void testTransplant() {
    scenarios.transplant();
    events.awaitAndAssertTransplantEvents(true);
    tracing.assertNoNessieTraces();
    metrics.assertNoNessieMetrics();
  }

  @Test
  public void testReferenceCreated() {
    scenarios.referenceCreated();
    events.awaitAndAssertReferenceCreatedEvents(true);
    tracing.assertNoNessieTraces();
    metrics.assertNoNessieMetrics();
  }

  @Test
  public void testReferenceDeleted() {
    scenarios.referenceDeleted();
    events.awaitAndAssertReferenceDeletedEvents(true);
    tracing.assertNoNessieTraces();
    metrics.assertNoNessieMetrics();
  }

  @Test
  public void testReferenceUpdated() {
    scenarios.referenceUpdated();
    events.awaitAndAssertReferenceUpdatedEvents(true);
    tracing.assertNoNessieTraces();
    metrics.assertNoNessieMetrics();
  }

  public static class Profile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> map = new HashMap<>();
      // Events and auth enabled, tracing and metrics disabled by default
      map.put("nessie.server.authentication.enabled", "true");
      map.put("nessie.version.store.events.enable", "true");
      map.put("nessie.version.store.trace.enable", "false");
      map.put("nessie.version.store.metrics.enable", "false");
      // Extensions all enabled by default
      map.put("quarkus.micrometer.enabled", "true");
      map.put("quarkus.opentelemetry.enabled", "true");
      // Sample everything
      map.put("quarkus.opentelemetry.tracer.sampler", "on");
      // Speed up retries
      map.put("nessie.version.store.events.retry.initial-delay", "PT0.01S");
      // Test config properties
      map.put("nessie.version.store.events.static-properties.foo", "bar");
      return map;
    }
  }
}
