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

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.projectnessie.events.spi.EventSubscription;
import org.slf4j.Logger;

@SuppressWarnings({
  "Slf4jLoggerShouldBeFinal",
  "Slf4jLoggerShouldBePrivate",
  "Slf4jFormatShouldBeConst"
})
class TestLoggingEventDelivery extends TestRetriableEventDelivery<LoggingEventDelivery> {

  @Mock EventSubscription subscription;

  @Mock Logger logger;

  StandardEventDelivery delegate;

  ArgumentMatcher<Throwable> fail1Matcher =
      t -> t instanceof RuntimeException && t.getMessage().equals("fail1");

  ArgumentMatcher<Throwable> fail2Matcher =
      t ->
          t instanceof RuntimeException
              && t.getMessage().equals("fail2")
              && t.getSuppressed()[0].getMessage().equals("fail1");
  ArgumentMatcher<Throwable> fail3Matcher =
      t ->
          t instanceof RuntimeException
              && t.getMessage().equals("fail3")
              && t.getSuppressed()[0].getMessage().equals("fail2")
              && t.getSuppressed()[0].getSuppressed()[0].getMessage().equals("fail1");

  @Override
  LoggingEventDelivery newDelivery() {
    when(subscription.getIdAsText()).thenReturn("subscription-id");
    when(event.getIdAsText()).thenReturn("event-id");
    delegate = spy(new StandardEventDelivery(event, subscriber, retryConfig, vertx));
    return new LoggingEventDelivery(delegate, event, subscription, logger);
  }

  @Override
  @Test
  void testDeliverySuccessNoRetry() {
    super.testDeliverySuccessNoRetry();
    verify(delegate).deliverySuccessful(1);
    verify(logger).debug("Starting delivery for event: {}", event);
    verify(logger).debug("Event delivered successfully");
    verifyNoMoreInteractions(logger);
  }

  @Override
  @Test
  void testDeliverySuccessWithRetry() {
    super.testDeliverySuccessWithRetry();
    verify(delegate).deliverySuccessful(3);
    verify(logger).debug("Starting delivery for event: {}", event);
    verify(logger)
        .debug(
            eq("Event delivery attempt {} failed, retrying in {}"),
            eq(1),
            eq(Duration.ofSeconds(1)),
            argThat(fail1Matcher));
    verify(logger)
        .debug(
            eq("Event delivery attempt {} failed, retrying in {}"),
            eq(2),
            eq(Duration.ofSeconds(2)),
            argThat(fail2Matcher));
    verify(logger).debug("Event delivered successfully");
    verifyNoMoreInteractions(logger);
  }

  @Override
  @Test
  void testDeliveryFailureWithRetry() {
    super.testDeliveryFailureWithRetry();
    verify(delegate).deliveryFailed(eq(3), any());
    verify(logger).debug("Starting delivery for event: {}", event);
    verify(logger)
        .debug(
            eq("Event delivery attempt {} failed, retrying in {}"),
            eq(1),
            eq(Duration.ofSeconds(1)),
            argThat(fail1Matcher));
    verify(logger)
        .debug(
            eq("Event delivery attempt {} failed, retrying in {}"),
            eq(2),
            eq(Duration.ofSeconds(2)),
            argThat(fail2Matcher));
    verify(logger).debug(eq("Event delivery failed"), argThat(fail3Matcher));
    verifyNoMoreInteractions(logger);
  }

  @Override
  @Test
  void testDeliveryRejected() {
    super.testDeliveryRejected();
    verify(delegate).deliveryRejected();
    verify(logger).debug("Starting delivery for event: {}", event);
    verify(logger).debug("Subscriber rejected event, aborting delivery");
    verifyNoMoreInteractions(logger);
  }
}
