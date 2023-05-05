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

import static org.projectnessie.events.quarkus.config.VersionStoreConfigConstants.NESSIE_VERSION_STORE_TRACE_ENABLE;

import io.quarkus.runtime.StartupEvent;
import io.quarkus.vertx.LocalEventBusCodec;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.tracing.TracingPolicy;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.Produces;
import javax.inject.Named;
import org.eclipse.microprofile.config.inject.ConfigProperty;

public class EventBusConfigurer {

  public static final String LOCAL_CODEC_NAME = "local";
  public static final LocalEventBusCodec<Object> LOCAL_CODEC =
      new LocalEventBusCodec<>(LOCAL_CODEC_NAME);
  public static final String EVENTS_DELIVERY_OPTIONS_BEAN_NAME =
      "nessie.beans.events.delivery-options";

  @Produces
  @ApplicationScoped
  @Named(EVENTS_DELIVERY_OPTIONS_BEAN_NAME)
  public DeliveryOptions configureDeliveryOptions(
      @ConfigProperty(name = NESSIE_VERSION_STORE_TRACE_ENABLE, defaultValue = "false")
          boolean tracingEnabled) {
    // FIXME: currently, tracing policy is ignored by Quarkus, see
    // https://github.com/quarkusio/quarkus/issues/25417
    // Concretely, this means that Vertx always creates send and receive spans,
    // even if tracing is disabled.
    return new DeliveryOptions()
        .setLocalOnly(true)
        .setCodecName(LOCAL_CODEC_NAME)
        .setTracingPolicy(tracingEnabled ? TracingPolicy.ALWAYS : TracingPolicy.IGNORE);
  }

  void configureEventBus(@Observes StartupEvent ev, EventBus eventBus) {
    eventBus.registerCodec(LOCAL_CODEC);
  }
}
