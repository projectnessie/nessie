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

import java.time.Duration;
import org.projectnessie.events.api.Event;
import org.projectnessie.events.service.EventService;
import org.projectnessie.events.spi.EventSubscriber;

/**
 * Abstract base class for event delivery implementations with retries and backoff.
 *
 * <p>The core delivery logic is implemented by {@link StandardEventDelivery}.
 *
 * <p>For blocking subscribers, {@link BlockingEventDelivery} should be used instead.
 *
 * <p>Other implementations use the delegate pattern to inject additional behavior: {@link
 * LoggingEventDelivery}, {@link TracingEventDelivery}, and {@link MetricsEventDelivery}.
 */
public abstract class RetriableEventDelivery implements EventDelivery {

  /**
   * Starts the delivery.
   *
   * @implSpec Implementers must call {@link #startAttempt(int, Duration, Throwable)}.
   */
  @Override
  public abstract void start();

  /**
   * Starts a new delivery attempt.
   *
   * @param currentAttempt The current delivery attempt, starting at 1.
   * @param nextDelay The delay to wait before the next attempt, if this attempt fails.
   * @param previousError The error from the previous attempt, or {@code null} if this is the first
   *     attempt.
   * @implSpec Implementers must call {@link #tryDeliver(int)}, then either {@link
   *     #deliverySuccessful(int)} or, if the attempt failed, {@link #attemptFailed(int, Duration,
   *     Throwable)}.
   */
  protected abstract void startAttempt(
      int currentAttempt, Duration nextDelay, Throwable previousError);

  /**
   * Tries to deliver the event to the subscriber.
   *
   * @implSpec Implementers should call {@link EventService#notifySubscriber(EventSubscriber,
   *     Event)}.
   * @param currentAttempt The current delivery attempt, starting at 1.
   */
  protected abstract void tryDeliver(int currentAttempt);

  /**
   * Called when the event was successfully delivered. Called at most once per event delivery.
   *
   * @param lastAttempt The last delivery attempt, starting at 1.
   */
  protected void deliverySuccessful(int lastAttempt) {
    // no-op
  }

  /**
   * Called when the delivery failed after the maximum number of retries. Called at most once per
   * event delivery.
   *
   * @param lastAttempt The last delivery attempt, starting at 1.
   * @param error The error that caused the delivery to fail.
   */
  protected void deliveryFailed(int lastAttempt, Throwable error) {
    // no-op
  }

  /**
   * Called when the event delivery was rejected by the subscriber's event filter. Called at most
   * once per event delivery.
   */
  protected void deliveryRejected() {
    // no-op
  }

  /**
   * Called when an attempt to deliver the event failed.
   *
   * @param lastAttempt The last delivery attempt, starting at 1.
   * @param nextDelay The delay to wait before the next attempt.
   * @param error The error that caused the attempt to fail.
   * @implSpec If the maximum number of retries has been reached, implementers should call {@link
   *     #deliveryFailed(int, Throwable)}. Otherwise, they should call {@link #scheduleRetry(int,
   *     Duration, Throwable)}.
   */
  protected abstract void attemptFailed(int lastAttempt, Duration nextDelay, Throwable error);

  /**
   * Schedules a retry of the event delivery.
   *
   * @param lastAttempt The last delivery attempt, starting at 1.
   * @param nextDelay The delay to wait before retrying.
   * @param lastError The error that caused the attempt to fail.
   */
  protected abstract void scheduleRetry(int lastAttempt, Duration nextDelay, Throwable lastError);

  /**
   * Returns the {@link RetriableEventDelivery} instance to use as a receiver. Enables proper
   * dispatching of delegate method calls.
   *
   * @implSpec Implementations should never use {@code this} to call instance methods of this class,
   *     but always the instance returned by this method.
   */
  protected abstract RetriableEventDelivery getSelf();

  /**
   * Sets the {@link RetriableEventDelivery} instance to use as a receiver. Enables proper
   * dispatching of delegate method calls.
   *
   * @implSpec Implementations based on {@link DelegatingEventDelivery} MUST call {@code
   *     delegate.setSelf(this)} inside their constructors.
   */
  protected abstract void setSelf(RetriableEventDelivery self);
}
