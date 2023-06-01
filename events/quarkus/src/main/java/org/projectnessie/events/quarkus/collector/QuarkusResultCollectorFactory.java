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

import io.micrometer.core.instrument.MeterRegistry;
import io.opentelemetry.api.trace.Tracer;
import io.quarkus.arc.lookup.LookupIfProperty;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import java.security.Principal;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.enterprise.context.RequestScoped;
import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.enterprise.inject.Produces;
import javax.inject.Named;
import org.projectnessie.events.quarkus.config.QuarkusEventConfig;
import org.projectnessie.events.service.EventSubscribers;
import org.projectnessie.versioned.Result;

public class QuarkusResultCollectorFactory {

  /** The name of the CDI bean that provides the repository ID. */
  public static final String REPOSITORY_ID_BEAN_NAME = "nessie.beans.repository-id";

  @Produces
  @RequestScoped
  @LookupIfProperty(name = "nessie.version.store.events.enable", stringValue = "true")
  public Consumer<Result> newResultCollector(
      QuarkusEventConfig config,
      EventSubscribers subscribers,
      EventBus bus,
      DeliveryOptions options,
      @Named(REPOSITORY_ID_BEAN_NAME) Instance<String> repositoryIds,
      @Any Instance<Supplier<Principal>> users,
      @Any Instance<Tracer> tracers,
      @Any Instance<MeterRegistry> registries) {
    Principal principal = users.isResolvable() ? users.get().get() : null;
    String repositoryId = repositoryIds.isResolvable() ? repositoryIds.get() : "";
    Consumer<Result> collector;
    if (config.isMetricsEnabled() && registries.isResolvable()) {
      collector =
          new QuarkusMetricsResultCollector(
              subscribers, repositoryId, principal, bus, options, registries.get());
    } else {
      collector = new QuarkusResultCollector(subscribers, repositoryId, principal, bus, options);
    }
    if (config.isTracingEnabled() && tracers.isResolvable()) {
      String user = principal != null ? principal.getName() : null;
      collector = new QuarkusTracingResultCollector(collector, user, tracers.get());
    }
    return collector;
  }
}
