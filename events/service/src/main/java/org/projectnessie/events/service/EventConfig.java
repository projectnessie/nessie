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
package org.projectnessie.events.service;

import java.time.Clock;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import org.projectnessie.events.api.Event;
import org.projectnessie.events.spi.EventSystemConfiguration;
import org.projectnessie.events.spi.ImmutableEventSystemConfiguration;
import org.projectnessie.model.NessieConfiguration;

public interface EventConfig {

  /**
   * The {@link EventSystemConfiguration} for this server. This is used to create event
   * subscriptions.
   */
  default EventSystemConfiguration getSystemConfiguration() {
    NessieConfiguration modelNessieConfig = NessieConfiguration.getBuiltInConfig();
    return ImmutableEventSystemConfiguration.builder()
        .specVersion(modelNessieConfig.getSpecVersion())
        .minSupportedApiVersion(modelNessieConfig.getMinSupportedApiVersion())
        .maxSupportedApiVersion(modelNessieConfig.getMaxSupportedApiVersion())
        .build();
  }

  /**
   * A map of static {@linkplain Event#getProperties() event properties} that will be included in
   * every event produced by this server.
   */
  default Map<String, String> getStaticProperties() {
    return Collections.emptyMap();
  }

  /** The UUID generator for event IDs. */
  default Supplier<UUID> getIdGenerator() {
    return UUID::randomUUID;
  }

  /** The clock used to generate timestamps for events. */
  default Clock getClock() {
    return Clock.systemUTC();
  }

  /** Retry configuration for event deliveries. */
  default RetryConfig getRetryConfig() {
    return new RetryConfig() {};
  }

  interface RetryConfig {

    int DEFAULT_MAX_RETRIES = 3;
    Duration DEFAULT_INITIAL_DELAY = Duration.ofSeconds(1);
    Duration DEFAULT_MAX_DELAY = Duration.ofSeconds(5);

    /** The maximum number of retries for an event delivery. */
    default int getMaxRetries() {
      return DEFAULT_MAX_RETRIES;
    }

    /** The initial delay for the first retry of a failed event delivery. */
    default Duration getInitialDelay() {
      return DEFAULT_INITIAL_DELAY;
    }

    /** The maximum delay for a retry of a failed event delivery. */
    default Duration getMaxDelay() {
      return DEFAULT_MAX_DELAY;
    }

    /**
     * Computes the next delay for a retry of a failed event.
     *
     * @implSpec The default implementation doubles the previous delay, but does not exceed the
     *     {@linkplain #getMaxDelay() maximum delay}.
     * @param previous the previous delay
     * @return the next delay
     */
    default Duration getNextDelay(Duration previous) {
      Duration next = previous.multipliedBy(2);
      Duration max = getMaxDelay();
      return next.compareTo(max) > 0 ? max : next;
    }
  }
}
