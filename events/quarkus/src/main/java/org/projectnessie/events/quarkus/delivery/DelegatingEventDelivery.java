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

public abstract class DelegatingEventDelivery extends RetriableEventDelivery {

  private final RetriableEventDelivery delegate;

  protected DelegatingEventDelivery(RetriableEventDelivery delegate) {
    this.delegate = delegate;
  }

  @Override
  public void start() {
    delegate.start();
  }

  @Override
  protected void startAttempt(int currentAttempt, Duration nextDelay, Throwable previousError) {
    delegate.startAttempt(currentAttempt, nextDelay, previousError);
  }

  @Override
  protected void tryDeliver(int currentAttempt) {
    delegate.tryDeliver(currentAttempt);
  }

  @Override
  protected void deliverySuccessful(int lastAttempt) {
    delegate.deliverySuccessful(lastAttempt);
  }

  @Override
  protected void deliveryFailed(int lastAttempt, Throwable error) {
    delegate.deliveryFailed(lastAttempt, error);
  }

  @Override
  protected void deliveryRejected() {
    delegate.deliveryRejected();
  }

  @Override
  protected void attemptFailed(int lastAttempt, Duration nextDelay, Throwable error) {
    delegate.attemptFailed(lastAttempt, nextDelay, error);
  }

  @Override
  protected void scheduleRetry(int lastAttempt, Duration nextDelay, Throwable lastError) {
    delegate.scheduleRetry(lastAttempt, nextDelay, lastError);
  }

  @Override
  protected final RetriableEventDelivery getSelf() {
    return delegate.getSelf();
  }

  @Override
  protected final void setSelf(RetriableEventDelivery self) {
    delegate.setSelf(self);
  }
}
