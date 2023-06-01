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

import static org.projectnessie.events.service.EventService.EVENT_ID_MDC_KEY;
import static org.projectnessie.events.service.EventService.SUBSCRIPTION_ID_MDC_KEY;

import java.time.Duration;
import org.projectnessie.events.api.Event;
import org.projectnessie.events.spi.EventSubscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

class LoggingEventDelivery extends DelegatingEventDelivery {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoggingEventDelivery.class);

  static boolean isLoggingEnabled() {
    return LOGGER.isDebugEnabled();
  }

  private final Event event;
  private final EventSubscription subscription;

  private final Logger logger;

  LoggingEventDelivery(
      RetriableEventDelivery delegate, Event event, EventSubscription subscription) {
    this(delegate, event, subscription, LOGGER);
  }

  LoggingEventDelivery(
      RetriableEventDelivery delegate, Event event, EventSubscription subscription, Logger logger) {
    super(delegate);
    this.event = event;
    this.subscription = subscription;
    this.logger = logger;
    setSelf(this);
  }

  @Override
  public void start() {
    mdcPut();
    logger.debug("Starting delivery for event: {}", event);
    try {
      super.start();
    } finally {
      mdcRemove();
    }
  }

  @Override
  void deliverySuccessful(int lastAttempt) {
    mdcPut();
    logger.debug("Event delivered successfully");
    try {
      super.deliverySuccessful(lastAttempt);
    } finally {
      mdcRemove();
    }
  }

  @Override
  void deliveryFailed(int lastAttempt, Throwable error) {
    mdcPut();
    logger.debug("Event delivery failed", error);
    try {
      super.deliveryFailed(lastAttempt, error);
    } finally {
      mdcRemove();
    }
  }

  @Override
  void deliveryRejected() {
    mdcPut();
    logger.debug("Subscriber rejected event, aborting delivery");
    try {
      super.deliveryRejected();
    } finally {
      mdcRemove();
    }
  }

  @Override
  void scheduleRetry(int lastAttempt, Duration nextDelay, Throwable lastError) {
    mdcPut();
    logger.debug(
        "Event delivery attempt {} failed, retrying in {}", lastAttempt, nextDelay, lastError);
    try {
      super.scheduleRetry(lastAttempt, nextDelay, lastError);
    } finally {
      mdcRemove();
    }
  }

  private void mdcPut() {
    MDC.put(SUBSCRIPTION_ID_MDC_KEY, subscription.getIdAsText());
    MDC.put(EVENT_ID_MDC_KEY, event.getIdAsText());
  }

  private void mdcRemove() {
    MDC.remove(SUBSCRIPTION_ID_MDC_KEY);
    MDC.remove(EVENT_ID_MDC_KEY);
  }
}
