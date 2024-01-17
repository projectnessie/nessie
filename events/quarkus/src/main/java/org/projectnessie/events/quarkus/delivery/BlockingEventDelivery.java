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
 * A {@link RetriableEventDelivery} that executes delivery attempts on a Vert.x worker thread pool,
 * thus not blocking the Vert.x event loop. Suitable for blocking subscribers.
 */
class BlockingEventDelivery extends StandardEventDelivery {

  private final Vertx vertx;

  BlockingEventDelivery(
      Event event, EventSubscriber subscriber, QuarkusEventConfig.RetryConfig config, Vertx vertx) {
    super(event, subscriber, config, vertx);
    this.vertx = vertx;
  }

  @Override
  void startAttempt(int currentAttempt, Duration nextDelay, Throwable previousError) {
    vertx.<Void>executeBlocking(
        () -> {
          try {
            getSelf().tryDeliver(currentAttempt);
            getSelf().deliverySuccessful(currentAttempt);
          } catch (Exception e) {
            getSelf().attemptFailed(currentAttempt, nextDelay, addSuppressed(e, previousError));
          }
          return null;
        },
        false // accept concurrent executions (ordering is not guaranteed)
        );
  }
}
