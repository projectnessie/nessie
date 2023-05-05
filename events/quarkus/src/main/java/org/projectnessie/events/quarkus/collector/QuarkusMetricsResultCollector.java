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
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import java.security.Principal;
import org.projectnessie.events.service.EventSubscribers;
import org.projectnessie.versioned.Result;

public class QuarkusMetricsResultCollector extends QuarkusResultCollector {

  /** The total number of results collected from the Version Store, exposed as a counter. */
  public static final String NESSIE_RESULTS_TOTAL = "nessie.results.total";

  /**
   * The total number of results rejected by the collector, based on the event type filters exposed
   * by the subscribers.
   */
  public static final String NESSIE_RESULTS_REJECTED = "nessie.results.rejected";

  private final MeterRegistry registry;

  public QuarkusMetricsResultCollector(
      EventSubscribers subscribers,
      String repositoryId,
      Principal principal,
      EventBus bus,
      DeliveryOptions options,
      MeterRegistry registry) {
    super(subscribers, repositoryId, principal, bus, options);
    this.registry = registry;
  }

  @Override
  public void accept(Result result) {
    registry.counter(NESSIE_RESULTS_TOTAL).increment();
    super.accept(result);
  }

  @Override
  protected boolean shouldProcess(Result result) {
    boolean shouldProcess = super.shouldProcess(result);
    if (!shouldProcess) {
      registry.counter(NESSIE_RESULTS_REJECTED).increment();
    }
    return shouldProcess;
  }
}
