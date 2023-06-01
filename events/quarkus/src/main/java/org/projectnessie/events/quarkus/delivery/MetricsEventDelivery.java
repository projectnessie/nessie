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

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import org.projectnessie.events.api.Event;

public class MetricsEventDelivery extends DelegatingEventDelivery {

  public enum DeliveryStatus {
    SUCCESSFUL,
    FAILED,
    REJECTED
  }

  /** The total number of events that have been put into delivery, exposed as a timer. */
  public static final String NESSIE_EVENTS_TOTAL = "nessie.events.total";

  /** The number of events that have been successfully delivered, exposed as a counter. */
  public static final String NESSIE_EVENTS_SUCCESSFUL = "nessie.events.successful";

  /** The number of events that have failed to be delivered, exposed as a counter. */
  public static final String NESSIE_EVENTS_FAILED = "nessie.events.failed";

  /** The number of retries, exposed as a counter. */
  public static final String NESSIE_EVENTS_RETRIES = "nessie.events.retries";

  /** The number of events that have been rejected, exposed as a counter. */
  public static final String NESSIE_EVENTS_REJECTED = "nessie.events.rejected";

  public static final String EVENT_TYPE_TAG_NAME = "type";
  public static final String STATUS_TAG_NAME = "status";

  private final MeterRegistry registry;
  private final Clock clock;
  private final Tags tags;

  private Timer.Sample sample;

  MetricsEventDelivery(
      RetriableEventDelivery delegate, Event event, MeterRegistry registry, Clock clock) {
    super(delegate);
    this.registry = registry;
    this.clock = clock;
    tags = Tags.of(EVENT_TYPE_TAG_NAME, event.getType().name());
    setSelf(this);
  }

  @Override
  public void start() {
    sample = Timer.start(clock);
    super.start();
  }

  @Override
  void deliverySuccessful(int lastAttempt) {
    super.deliverySuccessful(lastAttempt);
    sample.stop(totalTimer(DeliveryStatus.SUCCESSFUL));
    registry.counter(NESSIE_EVENTS_SUCCESSFUL, tags).increment();
    if (lastAttempt > 1) {
      registry.counter(NESSIE_EVENTS_RETRIES, tags).increment(lastAttempt - 1);
    }
  }

  @Override
  void deliveryFailed(int lastAttempt, Throwable error) {
    super.deliveryFailed(lastAttempt, error);
    sample.stop(totalTimer(DeliveryStatus.FAILED));
    registry.counter(NESSIE_EVENTS_FAILED, tags).increment();
    if (lastAttempt > 1) {
      registry.counter(NESSIE_EVENTS_RETRIES, tags).increment(lastAttempt - 1);
    }
  }

  @Override
  void deliveryRejected() {
    super.deliveryRejected();
    sample.stop(totalTimer(DeliveryStatus.REJECTED));
    registry.counter(NESSIE_EVENTS_REJECTED, tags).increment();
  }

  private Timer totalTimer(DeliveryStatus status) {
    return Timer.builder(NESSIE_EVENTS_TOTAL)
        .tags(tags.and(STATUS_TAG_NAME, status.name()))
        .publishPercentileHistogram()
        .publishPercentiles(0.5, 0.95, 0.99)
        .register(registry);
  }
}
