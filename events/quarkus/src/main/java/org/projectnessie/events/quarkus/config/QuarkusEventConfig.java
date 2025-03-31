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
package org.projectnessie.events.quarkus.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;
import java.time.Duration;
import java.util.Map;
import org.projectnessie.events.service.EventConfig;

@ConfigMapping(prefix = "nessie.version.store.events")
public interface QuarkusEventConfig extends EventConfig {

  /**
   * Whether events for the version store are enabled (enabled by default). In order for events to
   * be published, it's not enough to enable them in the configuration; you also need to provide at
   * least one implementation of Nessie's EventListener SPI.
   */
  @WithName("enable")
  @WithDefault("true")
  boolean isEnabled();

  @WithName("static-properties")
  @WithDefault("{}")
  @Override
  Map<String, String> getStaticProperties();

  @WithName("retry")
  RetryConfig getRetryConfig();

  interface RetryConfig {

    /**
     * The maximum number of attempts to deliver an event, including the initial attempt. The
     * default is 1, which means that retries are disabled.
     */
    @WithName("max-attempts")
    @WithDefault("1")
    int getMaxAttempts();

    @WithName("initial-delay")
    @WithDefault("PT1S")
    Duration getInitialDelay();

    @WithName("max-delay")
    @WithDefault("PT5S")
    Duration getMaxDelay();

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
