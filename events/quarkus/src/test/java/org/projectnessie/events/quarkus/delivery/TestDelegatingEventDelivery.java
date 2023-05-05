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

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.opentelemetry.api.trace.Tracer;
import io.vertx.core.Vertx;
import java.util.stream.Stream;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.projectnessie.events.api.Event;
import org.projectnessie.events.api.EventType;
import org.projectnessie.events.quarkus.config.QuarkusEventConfig;
import org.projectnessie.events.spi.EventSubscriber;
import org.projectnessie.events.spi.EventSubscription;

@ExtendWith(MockitoExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestDelegatingEventDelivery {

  @Mock Event event;
  @Mock EventSubscriber subscriber;
  @Mock QuarkusEventConfig config;
  @Mock QuarkusEventConfig.RetryConfig retryConfig;
  @Mock EventSubscription subscription;
  @Mock Vertx vertx;
  @Mock MeterRegistry registry;
  @Mock Clock clock;
  @Mock Tracer tracer;

  @ParameterizedTest
  @MethodSource("deliveries")
  public void testDelegate(StandardEventDelivery root) {
    when(event.getType()).thenReturn(EventType.COMMIT);
    LoggingEventDelivery logging = new LoggingEventDelivery(root, event, subscription);
    MetricsEventDelivery metrics = new MetricsEventDelivery(logging, event, registry, clock);
    TracingEventDelivery tracing =
        new TracingEventDelivery(metrics, event, subscription, config, tracer);
    assertThat(tracing)
        .extracting("delegate")
        .isSameAs(metrics)
        .extracting("delegate")
        .isSameAs(logging)
        .extracting("delegate")
        .isSameAs(root);
  }

  @ParameterizedTest
  @MethodSource("deliveries")
  public void testSelf(StandardEventDelivery root) {
    when(event.getType()).thenReturn(EventType.COMMIT);
    LoggingEventDelivery logging = new LoggingEventDelivery(root, event, subscription);
    MetricsEventDelivery metrics = new MetricsEventDelivery(logging, event, registry, clock);
    TracingEventDelivery tracing =
        new TracingEventDelivery(metrics, event, subscription, config, tracer);
    // Expect all instances to have the same "self" receiver:
    // the outermost instance (tracing in this case).
    assertThat(tracing.getSelf()).isSameAs(tracing);
    assertThat(metrics.getSelf()).isSameAs(tracing);
    assertThat(logging.getSelf()).isSameAs(tracing);
    assertThat(root.getSelf()).isSameAs(tracing);
  }

  public Stream<StandardEventDelivery> deliveries() {
    return Stream.of(
        new StandardEventDelivery(event, subscriber, retryConfig, vertx),
        new BlockingEventDelivery(event, subscriber, retryConfig, vertx));
  }
}
