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

import static io.opentelemetry.api.trace.StatusCode.ERROR;
import static io.opentelemetry.api.trace.StatusCode.OK;
import static org.projectnessie.events.quarkus.collector.QuarkusTracingResultCollector.ENDUSER_ID;
import static org.projectnessie.events.quarkus.collector.QuarkusTracingResultCollector.PEER_SERVICE;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import org.projectnessie.events.api.Event;
import org.projectnessie.events.service.EventConfig;
import org.projectnessie.events.spi.EventSubscription;

public class TracingEventDelivery extends DelegatingEventDelivery {

  public static final String NESSIE_EVENTS_SPAN_NAME_PREFIX = "nessie.events.subscribers.";

  public static final AttributeKey<String> EVENT_TYPE_KEY =
      AttributeKey.stringKey("nessie.events.type");
  public static final AttributeKey<String> SUBSCRIPTION_ID_KEY =
      AttributeKey.stringKey("nessie.events.subscription-id");
  public static final AttributeKey<String> EVENT_ID_KEY =
      AttributeKey.stringKey("nessie.events.id");
  public static final AttributeKey<Long> DELIVERY_ATTEMPT_KEY =
      AttributeKey.longKey("nessie.events.delivery-attempt");
  public static final AttributeKey<Long> RETRIES_KEY =
      AttributeKey.longKey("nessie.events.retries");

  private final Event event;
  private final EventSubscription subscription;
  private final Tracer tracer;
  private final String spanNamePrefix;
  private final Clock clock;

  private Span deliverySpan;
  private Span attemptSpan;

  TracingEventDelivery(
      RetriableEventDelivery delegate,
      Event event,
      EventSubscription subscription,
      EventConfig config,
      Tracer tracer) {
    super(delegate);
    this.event = event;
    this.subscription = subscription;
    this.tracer = tracer;
    clock = config.getClock();
    spanNamePrefix = NESSIE_EVENTS_SPAN_NAME_PREFIX + event.getType();
    setSelf(this);
  }

  @Override
  public void start() {
    deliverySpan = newDeliverySpan();
    try (Scope ignored = deliverySpan.makeCurrent()) {
      super.start();
    }
  }

  @Override
  void startAttempt(int currentAttempt, Duration nextDelay, Throwable previousError) {
    attemptSpan = newAttemptSpan(currentAttempt);
    try (Scope ignored = attemptSpan.makeCurrent()) {
      super.startAttempt(currentAttempt, nextDelay, previousError);
    }
  }

  @Override
  void tryDeliver(int currentAttempt) {
    try (Scope ignored = attemptSpan.makeCurrent()) {
      super.tryDeliver(currentAttempt);
    }
  }

  @Override
  void deliverySuccessful(int lastAttempt) {
    try (Scope ignored = deliverySpan.makeCurrent()) {
      super.deliverySuccessful(lastAttempt);
      if (lastAttempt > 1) {
        deliverySpan.setAttribute(RETRIES_KEY, (long) lastAttempt - 1);
      }
      deliverySpan.addEvent("delivery complete", clock.instant());
      attemptSpan.setStatus(OK);
      deliverySpan.setStatus(OK);
    } finally {
      attemptSpan.end();
      deliverySpan.end();
    }
  }

  @Override
  void deliveryFailed(int lastAttempt, Throwable error) {
    try (Scope ignored = deliverySpan.makeCurrent()) {
      super.deliveryFailed(lastAttempt, error);
      if (lastAttempt > 1) {
        deliverySpan.setAttribute(RETRIES_KEY, (long) lastAttempt - 1);
      }
      deliverySpan.addEvent("delivery failed", clock.instant());
      deliverySpan.recordException(error);
      attemptSpan.setStatus(ERROR);
      deliverySpan.setStatus(ERROR);
    } finally {
      attemptSpan.end();
      deliverySpan.end();
    }
  }

  @Override
  void deliveryRejected() {
    try (Scope ignored = deliverySpan.makeCurrent()) {
      super.deliveryRejected();
      Instant end = clock.instant();
      deliverySpan.addEvent("delivery rejected", end);
      deliverySpan.setStatus(OK);
    } finally {
      deliverySpan.end();
    }
  }

  @Override
  void attemptFailed(int lastAttempt, Duration nextDelay, Throwable error) {
    try (Scope ignored = attemptSpan.makeCurrent()) {
      attemptSpan.setStatus(ERROR);
      attemptSpan.recordException(error);
      super.attemptFailed(lastAttempt, nextDelay, error);
    }
    // don't end the attempt span here, it will be ended in scheduleRetry or deliveryFailed
  }

  @Override
  void scheduleRetry(int lastAttempt, Duration nextDelay, Throwable lastError) {
    attemptSpan.end();
    super.scheduleRetry(lastAttempt, nextDelay, lastError);
  }

  private Span newDeliverySpan() {
    return spanBuilder(spanNamePrefix + " delivery").startSpan();
  }

  private Span newAttemptSpan(int currentAttempt) {
    return spanBuilder(spanNamePrefix + " attempt " + currentAttempt)
        .setParent(Context.current().with(deliverySpan))
        .setAttribute(DELIVERY_ATTEMPT_KEY, (long) currentAttempt)
        .startSpan();
  }

  private SpanBuilder spanBuilder(String spanName) {
    SpanBuilder spanBuilder =
        tracer
            .spanBuilder(spanName)
            .setSpanKind(SpanKind.INTERNAL)
            .setStartTimestamp(clock.instant())
            .setAttribute(EVENT_TYPE_KEY, event.getType().name())
            .setAttribute(EVENT_ID_KEY, event.getIdAsText())
            .setAttribute(SUBSCRIPTION_ID_KEY, subscription.getIdAsText())
            .setAttribute(PEER_SERVICE, "Nessie");
    event.getEventInitiator().ifPresent(u -> spanBuilder.setAttribute(ENDUSER_ID, u));
    return spanBuilder;
  }
}
