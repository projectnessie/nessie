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

import io.vertx.core.Vertx;
import java.time.Duration;
import org.projectnessie.events.api.Event;
import org.projectnessie.events.quarkus.config.QuarkusEventConfig;
import org.projectnessie.events.spi.EventSubscriber;

/**
 * A Non-blocking {@link RetriableEventDelivery} that executes delivery attempts directly on Vert.x
 * event loop. Suitable only for subscribers that do not block the event loop.
 */
public class StandardEventDelivery extends RetriableEventDelivery {

  private final Event event;
  private final EventSubscriber subscriber;
  private final QuarkusEventConfig.RetryConfig config;
  private final Vertx vertx;
  private RetriableEventDelivery self = this;

  // For testing only: 0 = not finished, 1 = success, 2 = failure, 3 = rejected
  static final byte STATE_NOT_FINISHED = 0;
  static final byte STATE_SUCCESS = 1;
  static final byte STATE_FAILURE = 2;
  static final byte STATE_REJECTED = 3;
  byte state = STATE_NOT_FINISHED;

  public StandardEventDelivery(
      Event event, EventSubscriber subscriber, QuarkusEventConfig.RetryConfig config, Vertx vertx) {
    this.event = event;
    this.subscriber = subscriber;
    this.config = config;
    this.vertx = vertx;
  }

  @Override
  public void start() {
    if (subscriber.accepts(event)) {
      self.startAttempt(1, config.getInitialDelay(), null);
    } else {
      self.deliveryRejected();
    }
  }

  @Override
  protected void startAttempt(int currentAttempt, Duration nextDelay, Throwable previousError) {
    try {
      self.tryDeliver(currentAttempt);
      self.deliverySuccessful(currentAttempt);
    } catch (Exception e) {
      self.attemptFailed(currentAttempt, nextDelay, addSuppressed(e, previousError));
    }
  }

  @Override
  protected void tryDeliver(int currentAttempt) {
    subscriber.onEvent(event);
  }

  @Override
  protected void attemptFailed(int lastAttempt, Duration nextDelay, Throwable error) {
    if (lastAttempt < config.getMaxAttempts()) {
      self.scheduleRetry(lastAttempt, nextDelay, error);
    } else {
      self.deliveryFailed(lastAttempt, error);
    }
  }

  @Override
  protected void scheduleRetry(int lastAttempt, Duration nextDelay, Throwable lastError) {
    vertx.setTimer(
        nextDelay.toMillis(),
        id -> self.startAttempt(lastAttempt + 1, config.getNextDelay(nextDelay), lastError));
  }

  @Override
  protected void deliverySuccessful(int lastAttempt) {
    state = STATE_SUCCESS;
  }

  @Override
  protected void deliveryFailed(int lastAttempt, Throwable error) {
    state = STATE_FAILURE;
  }

  @Override
  protected void deliveryRejected() {
    state = STATE_REJECTED;
  }

  @Override
  protected final void setSelf(RetriableEventDelivery self) {
    this.self = self;
  }

  @Override
  protected final RetriableEventDelivery getSelf() {
    return self;
  }

  protected static Throwable addSuppressed(Throwable current, Throwable previous) {
    if (previous != null) {
      current.addSuppressed(previous);
    }
    return current;
  }
}
