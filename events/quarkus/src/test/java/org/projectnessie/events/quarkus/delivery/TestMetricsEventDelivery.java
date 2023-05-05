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
package org.projectnessie.events.quarkus.delivery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
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
import static org.projectnessie.events.quarkus.delivery.StandardEventDelivery.STATE_FAILURE;
import static org.projectnessie.events.quarkus.delivery.StandardEventDelivery.STATE_REJECTED;
import static org.projectnessie.events.quarkus.delivery.StandardEventDelivery.STATE_SUCCESS;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.projectnessie.events.api.EventType;

class TestMetricsEventDelivery extends TestRetriableEventDelivery<MetricsEventDelivery> {

  StandardEventDelivery delegate;
  MeterRegistry registry;
  @Mock Clock clock;

  @Override
  MetricsEventDelivery newDelivery() {
    when(event.getType()).thenReturn(EventType.COMMIT);
    delegate = new StandardEventDelivery(event, subscriber, retryConfig, vertx);
    registry = new SimpleMeterRegistry();
    when(clock.monotonicTime()).thenReturn(0L, 10_000_000L); // 10 ms
    return new MetricsEventDelivery(delegate, event, registry, clock);
  }

  @Override
  @Test
  void testDeliverySuccessNoRetry() {
    super.testDeliverySuccessNoRetry();
    assertThat(delegate.state).isEqualTo(STATE_SUCCESS);
    assertThat(totalTimer(SUCCESSFUL).totalTime(TimeUnit.MILLISECONDS)).isEqualTo(10L);
    assertThat(totalTimer(SUCCESSFUL).count()).isEqualTo(1);
    assertThat(totalTimer(FAILED).count()).isEqualTo(0);
    assertThat(totalTimer(REJECTED).count()).isEqualTo(0);
    assertThat(successfulCounter().count()).isEqualTo(1);
    assertThat(failedCounter().count()).isEqualTo(0);
    assertThat(rejectedCounter().count()).isEqualTo(0);
    assertThat(retriesCounter().count()).isEqualTo(0);
  }

  @Override
  @Test
  void testDeliverySuccessWithRetry() {
    super.testDeliverySuccessWithRetry();
    assertThat(delegate.state).isEqualTo(STATE_SUCCESS);
    assertThat(totalTimer(SUCCESSFUL).totalTime(TimeUnit.MILLISECONDS)).isEqualTo(10L);
    assertThat(totalTimer(SUCCESSFUL).count()).isEqualTo(1);
    assertThat(totalTimer(FAILED).count()).isEqualTo(0);
    assertThat(totalTimer(REJECTED).count()).isEqualTo(0);
    assertThat(successfulCounter().count()).isEqualTo(1);
    assertThat(failedCounter().count()).isEqualTo(0);
    assertThat(rejectedCounter().count()).isEqualTo(0);
    assertThat(retriesCounter().count()).isEqualTo(2);
  }

  @Override
  @Test
  void testDeliveryFailureWithRetry() {
    super.testDeliveryFailureWithRetry();
    assertThat(delegate.state).isEqualTo(STATE_FAILURE);
    assertThat(totalTimer(FAILED).totalTime(TimeUnit.MILLISECONDS)).isEqualTo(10L);
    assertThat(totalTimer(SUCCESSFUL).count()).isEqualTo(0);
    assertThat(totalTimer(FAILED).count()).isEqualTo(1);
    assertThat(totalTimer(REJECTED).count()).isEqualTo(0);
    assertThat(successfulCounter().count()).isEqualTo(0);
    assertThat(failedCounter().count()).isEqualTo(1);
    assertThat(rejectedCounter().count()).isEqualTo(0);
    assertThat(retriesCounter().count()).isEqualTo(2);
  }

  @Override
  @Test
  void testDeliveryRejected() {
    super.testDeliveryRejected();
    assertThat(delegate.state).isEqualTo(STATE_REJECTED);
    assertThat(totalTimer(REJECTED).totalTime(TimeUnit.MILLISECONDS)).isEqualTo(10L);
    assertThat(totalTimer(SUCCESSFUL).count()).isEqualTo(0);
    assertThat(totalTimer(FAILED).count()).isEqualTo(0);
    assertThat(totalTimer(REJECTED).count()).isEqualTo(1);
    assertThat(successfulCounter().count()).isEqualTo(0);
    assertThat(failedCounter().count()).isEqualTo(0);
    assertThat(rejectedCounter().count()).isEqualTo(1);
    assertThat(retriesCounter().count()).isEqualTo(0);
  }

  Timer totalTimer(MetricsEventDelivery.DeliveryStatus status) {
    return registry.timer(
        NESSIE_EVENTS_TOTAL,
        Tags.of(
            EVENT_TYPE_TAG_NAME, EventType.COMMIT.name(),
            STATUS_TAG_NAME, status.name()));
  }

  Counter successfulCounter() {
    return registry.counter(
        NESSIE_EVENTS_SUCCESSFUL, Tags.of(EVENT_TYPE_TAG_NAME, EventType.COMMIT.name()));
  }

  Counter failedCounter() {
    return registry.counter(
        NESSIE_EVENTS_FAILED, Tags.of(EVENT_TYPE_TAG_NAME, EventType.COMMIT.name()));
  }

  Counter rejectedCounter() {
    return registry.counter(
        NESSIE_EVENTS_REJECTED, Tags.of(EVENT_TYPE_TAG_NAME, EventType.COMMIT.name()));
  }

  Counter retriesCounter() {
    return registry.counter(
        NESSIE_EVENTS_RETRIES, Tags.of(EVENT_TYPE_TAG_NAME, EventType.COMMIT.name()));
  }
}
