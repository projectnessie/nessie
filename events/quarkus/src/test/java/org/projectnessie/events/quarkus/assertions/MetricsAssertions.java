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
package org.projectnessie.events.quarkus.assertions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.projectnessie.events.api.EventType.COMMIT;
import static org.projectnessie.events.api.EventType.CONTENT_REMOVED;
import static org.projectnessie.events.api.EventType.CONTENT_STORED;
import static org.projectnessie.events.api.EventType.MERGE;
import static org.projectnessie.events.api.EventType.REFERENCE_CREATED;
import static org.projectnessie.events.api.EventType.REFERENCE_DELETED;
import static org.projectnessie.events.api.EventType.REFERENCE_UPDATED;
import static org.projectnessie.events.api.EventType.TRANSPLANT;
import static org.projectnessie.events.quarkus.assertions.EventAssertions.TIMEOUT;
import static org.projectnessie.events.quarkus.collector.QuarkusMetricsResultCollector.NESSIE_RESULTS_REJECTED;
import static org.projectnessie.events.quarkus.collector.QuarkusMetricsResultCollector.NESSIE_RESULTS_TOTAL;
import static org.projectnessie.events.quarkus.delivery.MetricsEventDelivery.DeliveryStatus.FAILED;
import static org.projectnessie.events.quarkus.delivery.MetricsEventDelivery.DeliveryStatus.REJECTED;
import static org.projectnessie.events.quarkus.delivery.MetricsEventDelivery.DeliveryStatus.SUCCESSFUL;
import static org.projectnessie.events.quarkus.delivery.MetricsEventDelivery.EVENT_TYPE_TAG_NAME;
import static org.projectnessie.events.quarkus.delivery.MetricsEventDelivery.NESSIE_EVENTS_FAILED;
import static org.projectnessie.events.quarkus.delivery.MetricsEventDelivery.NESSIE_EVENTS_REJECTED;
import static org.projectnessie.events.quarkus.delivery.MetricsEventDelivery.NESSIE_EVENTS_RETRIES;
import static org.projectnessie.events.quarkus.delivery.MetricsEventDelivery.NESSIE_EVENTS_SUCCESSFUL;
import static org.projectnessie.events.quarkus.delivery.MetricsEventDelivery.NESSIE_EVENTS_TOTAL;
import static org.projectnessie.events.quarkus.delivery.MetricsEventDelivery.STATUS_TAG_NAME;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.Collection;
import org.projectnessie.events.api.EventType;
import org.projectnessie.events.quarkus.delivery.MetricsEventDelivery.DeliveryStatus;

@Singleton
public class MetricsAssertions {

  @Inject Instance<MeterRegistry> registry;

  public void reset() {
    if (registry.isResolvable()) {
      registry.get().clear();
    }
  }

  public void assertMicrometerDisabled() {
    assertThat(registry.isResolvable()).isFalse();
  }

  // Note: the assertions below rely on the predefined subscriber behavior, see MockEventSubscriber
  // for details

  public void awaitAndAssertCommitMetrics() {
    await().atMost(TIMEOUT).untilAsserted(this::assertCommitMetrics);
  }

  public void awaitAndAssertMergeMetrics() {
    await().atMost(TIMEOUT).untilAsserted(this::assertMergeMetrics);
  }

  public void awaitAndAssertTransplantMetrics() {
    await().atMost(TIMEOUT).untilAsserted(this::assertTransplantMetrics);
  }

  public void awaitAndAssertReferenceCreatedMetrics() {
    await().atMost(TIMEOUT).untilAsserted(this::assertReferenceCreatedMetrics);
  }

  public void awaitAndAssertReferenceDeletedMetrics() {
    await().atMost(TIMEOUT).untilAsserted(this::assertReferenceDeletedMetrics);
  }

  public void awaitAndAssertReferenceUpdatedMetrics() {
    await().atMost(TIMEOUT).untilAsserted(this::assertReferenceUpdatedMetrics);
  }

  private void assertCommitMetrics() {
    assertThat(registry.isResolvable()).isTrue();
    assertResultCounters();
    assertTotalEventsTimer(COMMIT, SUCCESSFUL, 1); // only subscriber1
    assertTotalEventsTimer(CONTENT_STORED, SUCCESSFUL, 1);
    assertTotalEventsTimer(CONTENT_STORED, REJECTED, 1);
    assertTotalEventsTimer(CONTENT_REMOVED, SUCCESSFUL, 2);
    assertEventCounter(NESSIE_EVENTS_SUCCESSFUL, COMMIT, 1); // only subscriber1
    assertEventCounter(NESSIE_EVENTS_SUCCESSFUL, CONTENT_STORED, 1); // rejected by subscriber2
    assertEventCounter(NESSIE_EVENTS_SUCCESSFUL, CONTENT_REMOVED, 2);
    assertEventCounter(NESSIE_EVENTS_REJECTED, CONTENT_STORED, 1);
    assertEventCounter(NESSIE_EVENTS_RETRIES, CONTENT_REMOVED, 2);
    assertEventCounter(NESSIE_EVENTS_FAILED, CONTENT_REMOVED, 0);
    assertNoFailedEvents();
  }

  private void assertMergeMetrics() {
    assertCommitMetrics();
    assertTotalEventsTimer(MERGE, SUCCESSFUL, 1); // only subscriber1
  }

  private void assertTransplantMetrics() {
    assertCommitMetrics();
    assertTotalEventsTimer(TRANSPLANT, SUCCESSFUL, 1); // only subscriber1
  }

  private void assertReferenceCreatedMetrics() {
    assertThat(registry.isResolvable()).isTrue();
    assertResultCounters();
    assertTotalEventsTimer(REFERENCE_CREATED, SUCCESSFUL, 2);
    assertEventCounter(NESSIE_EVENTS_SUCCESSFUL, REFERENCE_CREATED, 2);
    assertEventCounter(NESSIE_EVENTS_FAILED, REFERENCE_CREATED, 0);
    assertEventCounter(NESSIE_EVENTS_REJECTED, REFERENCE_CREATED, 0);
    assertEventCounter(NESSIE_EVENTS_RETRIES, REFERENCE_CREATED, 0);
    assertNoFailedEvents();
  }

  private void assertReferenceDeletedMetrics() {
    assertThat(registry.isResolvable()).isTrue();
    assertResultCounters();
    assertTotalEventsTimer(REFERENCE_DELETED, SUCCESSFUL, 1);
    assertTotalEventsTimer(REFERENCE_DELETED, FAILED, 1);
    assertEventCounter(NESSIE_EVENTS_SUCCESSFUL, REFERENCE_DELETED, 1); // subscriber1
    assertEventCounter(NESSIE_EVENTS_FAILED, REFERENCE_DELETED, 1); // subscriber2
    assertEventCounter(NESSIE_EVENTS_REJECTED, REFERENCE_DELETED, 0);
    assertEventCounter(NESSIE_EVENTS_RETRIES, REFERENCE_DELETED, 2); // subscriber2
  }

  private void assertReferenceUpdatedMetrics() {
    assertThat(registry.isResolvable()).isTrue();
    assertResultCounters();
    assertTotalEventsTimer(REFERENCE_UPDATED, SUCCESSFUL, 2);
    assertEventCounter(NESSIE_EVENTS_SUCCESSFUL, REFERENCE_UPDATED, 2);
    assertEventCounter(NESSIE_EVENTS_FAILED, REFERENCE_UPDATED, 0);
    assertEventCounter(NESSIE_EVENTS_REJECTED, REFERENCE_UPDATED, 0);
    assertEventCounter(NESSIE_EVENTS_RETRIES, REFERENCE_UPDATED, 0);
    assertNoFailedEvents();
  }

  private void assertTotalEventsTimer(EventType eventType, DeliveryStatus status, int expected) {
    Timer timer =
        registry
            .get()
            .find(NESSIE_EVENTS_TOTAL)
            .tag(EVENT_TYPE_TAG_NAME, eventType.name())
            .tag(STATUS_TAG_NAME, status.name())
            .timer();
    if (expected == 0) {
      assertThat(timer).isNull();
    } else {
      assertThat(timer).isNotNull();
      assertThat(timer.count()).isEqualTo(expected);
    }
  }

  private void assertEventCounter(String name, EventType eventType, int expected) {
    Counter counter =
        registry.get().find(name).tag(EVENT_TYPE_TAG_NAME, eventType.name()).counter();
    if (expected == 0) {
      assertThat(counter).isNull();
    } else {
      assertThat(counter).isNotNull();
      assertThat(counter.count()).isEqualTo(expected);
    }
  }

  private void assertResultCounters() {
    Counter total = registry.get().find(NESSIE_RESULTS_TOTAL).counter();
    assertThat(total).isNotNull();
    assertThat(total.count()).isEqualTo(1);
    Counter rejected = registry.get().find(NESSIE_RESULTS_REJECTED).counter();
    assertThat(rejected).isNull();
  }

  private void assertNoFailedEvents() {
    Collection<Counter> counters = registry.get().find(NESSIE_EVENTS_FAILED).counters();
    assertThat(counters).isEmpty();
  }
}
