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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.projectnessie.events.quarkus.delivery.TracingEventDelivery.DELIVERY_ATTEMPT_KEY;
import static org.projectnessie.events.quarkus.delivery.TracingEventDelivery.EVENT_ID_KEY;
import static org.projectnessie.events.quarkus.delivery.TracingEventDelivery.EVENT_TYPE_KEY;
import static org.projectnessie.events.quarkus.delivery.TracingEventDelivery.RETRIES_KEY;
import static org.projectnessie.events.quarkus.delivery.TracingEventDelivery.SUBSCRIPTION_ID_KEY;

import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.sdk.trace.ReadWriteSpan;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SpanProcessor;
import java.time.Clock;
import java.util.Objects;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.projectnessie.events.api.EventType;
import org.projectnessie.events.service.EventConfig;
import org.projectnessie.events.spi.EventSubscription;

class TestTracingEventDelivery extends TestRetriableEventDelivery<TracingEventDelivery> {

  @Mock EventSubscription subscription;
  @Mock SpanProcessor processor;
  @Mock EventConfig config;

  StandardEventDelivery delegate;

  ArgumentMatcher<ReadWriteSpan> deliverySpanMatcherNoRetry =
      s ->
          s.getName().equals("nessie.events.subscribers.COMMIT delivery")
              && s.getKind() == SpanKind.INTERNAL
              && Objects.equals(s.getAttribute(EVENT_TYPE_KEY), "COMMIT")
              && Objects.equals(s.getAttribute(EVENT_ID_KEY), "test")
              && Objects.equals(s.getAttribute(SUBSCRIPTION_ID_KEY), "test")
              && Objects.equals(s.getAttribute(SUBSCRIPTION_ID_KEY), "test");

  ArgumentMatcher<ReadWriteSpan> deliverySpanMatcherWithRetries =
      s ->
          s.getName().equals("nessie.events.subscribers.COMMIT delivery")
              && s.getKind() == SpanKind.INTERNAL
              && Objects.equals(s.getAttribute(EVENT_TYPE_KEY), "COMMIT")
              && Objects.equals(s.getAttribute(EVENT_ID_KEY), "test")
              && Objects.equals(s.getAttribute(SUBSCRIPTION_ID_KEY), "test")
              && Objects.equals(s.getAttribute(RETRIES_KEY), 2L);
  ArgumentMatcher<ReadWriteSpan> attempt1SpanMatcher =
      s ->
          s.getName().equals("nessie.events.subscribers.COMMIT attempt 1")
              && s.getKind() == SpanKind.INTERNAL
              && Objects.equals(s.getAttribute(EVENT_TYPE_KEY), "COMMIT")
              && Objects.equals(s.getAttribute(EVENT_ID_KEY), "test")
              && Objects.equals(s.getAttribute(SUBSCRIPTION_ID_KEY), "test")
              && Objects.equals(s.getAttribute(DELIVERY_ATTEMPT_KEY), 1L);
  ArgumentMatcher<ReadWriteSpan> attempt2SpanMatcher =
      s ->
          s.getName().equals("nessie.events.subscribers.COMMIT attempt 2")
              && s.getKind() == SpanKind.INTERNAL
              && Objects.equals(s.getAttribute(EVENT_TYPE_KEY), "COMMIT")
              && Objects.equals(s.getAttribute(EVENT_ID_KEY), "test")
              && Objects.equals(s.getAttribute(SUBSCRIPTION_ID_KEY), "test")
              && Objects.equals(s.getAttribute(DELIVERY_ATTEMPT_KEY), 2L);
  ArgumentMatcher<ReadWriteSpan> attempt3SpanMatcher =
      s ->
          s.getName().equals("nessie.events.subscribers.COMMIT attempt 3")
              && s.getKind() == SpanKind.INTERNAL
              && Objects.equals(s.getAttribute(EVENT_TYPE_KEY), "COMMIT")
              && Objects.equals(s.getAttribute(EVENT_ID_KEY), "test")
              && Objects.equals(s.getAttribute(SUBSCRIPTION_ID_KEY), "test")
              && Objects.equals(s.getAttribute(DELIVERY_ATTEMPT_KEY), 3L);

  @Override
  TracingEventDelivery newDelivery() {
    when(event.getType()).thenReturn(EventType.COMMIT);
    when(event.getIdAsText()).thenReturn("test");
    when(subscription.getIdAsText()).thenReturn("test");
    when(config.getClock()).thenReturn(Clock.systemUTC());
    delegate = spy(new StandardEventDelivery(event, subscriber, retryConfig, vertx));
    @SuppressWarnings("resource")
    SdkTracerProvider provider = SdkTracerProvider.builder().addSpanProcessor(processor).build();
    Tracer tracer = provider.get("test");
    return new TracingEventDelivery(delegate, event, subscription, config, tracer);
  }

  @Override
  @Test
  void testDeliverySuccessNoRetry() {
    when(processor.isStartRequired()).thenReturn(true);
    when(processor.isEndRequired()).thenReturn(true);
    super.testDeliverySuccessNoRetry();
    verify(delegate).deliverySuccessful(1);
    verify(processor).onStart(any(), argThat(deliverySpanMatcherNoRetry));
    verify(processor).onEnd(argThat(deliverySpanMatcherNoRetry));
    verify(processor).onStart(any(), argThat(attempt1SpanMatcher));
    verify(processor).onEnd(argThat(attempt1SpanMatcher));
    verifyNoMoreInteractions(processor);
  }

  @Override
  @Test
  void testDeliverySuccessWithRetry() {
    when(processor.isStartRequired()).thenReturn(true);
    when(processor.isEndRequired()).thenReturn(true);
    super.testDeliverySuccessWithRetry();
    verify(delegate).deliverySuccessful(3);
    verify(processor).onStart(any(), argThat(deliverySpanMatcherWithRetries));
    verify(processor).onEnd(argThat(deliverySpanMatcherWithRetries));
    verify(processor).onStart(any(), argThat(attempt1SpanMatcher));
    verify(processor).onEnd(argThat(attempt1SpanMatcher));
    verify(processor).onStart(any(), argThat(attempt2SpanMatcher));
    verify(processor).onEnd(argThat(attempt2SpanMatcher));
    verify(processor).onStart(any(), argThat(attempt3SpanMatcher));
    verify(processor).onEnd(argThat(attempt3SpanMatcher));
    verifyNoMoreInteractions(processor);
  }

  @Override
  @Test
  void testDeliveryFailureWithRetry() {
    when(processor.isStartRequired()).thenReturn(true);
    when(processor.isEndRequired()).thenReturn(true);
    super.testDeliveryFailureWithRetry();
    verify(delegate).deliveryFailed(eq(3), any());
    verify(processor).onStart(any(), argThat(deliverySpanMatcherWithRetries));
    verify(processor).onEnd(argThat(deliverySpanMatcherWithRetries));
    verify(processor).onStart(any(), argThat(attempt1SpanMatcher));
    verify(processor).onEnd(argThat(attempt1SpanMatcher));
    verify(processor).onStart(any(), argThat(attempt2SpanMatcher));
    verify(processor).onEnd(argThat(attempt2SpanMatcher));
    verify(processor).onStart(any(), argThat(attempt3SpanMatcher));
    verify(processor).onEnd(argThat(attempt3SpanMatcher));
    verifyNoMoreInteractions(processor);
  }

  @Override
  @Test
  void testDeliveryRejected() {
    when(processor.isStartRequired()).thenReturn(true);
    when(processor.isEndRequired()).thenReturn(true);
    super.testDeliveryRejected();
    verify(delegate).deliveryRejected();
    verify(processor).onStart(any(), argThat(deliverySpanMatcherNoRetry));
    verify(processor).onEnd(argThat(deliverySpanMatcherNoRetry));
    verifyNoMoreInteractions(processor);
  }
}
