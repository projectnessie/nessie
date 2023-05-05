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
package org.projectnessie.events.quarkus.collector;

import static org.projectnessie.events.quarkus.QuarkusEventService.NESSIE_EVENTS_SERVICE_ADDR;

import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import java.security.Principal;
import org.projectnessie.events.quarkus.QuarkusEventService;
import org.projectnessie.events.service.EventSubscribers;
import org.projectnessie.events.service.ResultCollector;
import org.projectnessie.events.service.VersionStoreEvent;

/**
 * A Quarkus-specific {@link ResultCollector} that publishes results to the Vert.x event bus on an
 * internal address that is consumed by {@link
 * QuarkusEventService#onVersionStoreEvent(VersionStoreEvent)}.
 */
public class QuarkusResultCollector extends ResultCollector {

  public QuarkusResultCollector(
      EventSubscribers subscribers,
      String repositoryId,
      Principal user,
      EventBus bus,
      DeliveryOptions options) {
    super(
        subscribers,
        repositoryId,
        user,
        event -> bus.publish(NESSIE_EVENTS_SERVICE_ADDR, event, options));
  }
}
