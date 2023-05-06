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
}
