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
package org.projectnessie.events.quarkus;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.vertx.ConsumeEvent;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import java.util.Map;
import org.projectnessie.events.api.Event;
import org.projectnessie.events.api.EventType;
import org.projectnessie.events.quarkus.config.EventBusConfigurer;
import org.projectnessie.events.quarkus.delivery.EventDelivery;
import org.projectnessie.events.quarkus.delivery.EventDeliveryFactory;
import org.projectnessie.events.service.EventConfig;
import org.projectnessie.events.service.EventFactory;
import org.projectnessie.events.service.EventService;
import org.projectnessie.events.service.EventSubscribers;
import org.projectnessie.events.service.VersionStoreEvent;
import org.projectnessie.events.spi.EventSubscriber;
import org.projectnessie.events.spi.EventSubscription;

@ApplicationScoped
public class QuarkusEventService extends EventService {

  /**
   * The local event bus address used to exchange messages of type {@link VersionStoreEvent} between
   * {@link org.projectnessie.events.quarkus.collector.QuarkusResultCollector} and {@link
   * QuarkusEventService}.
   */
  public static final String NESSIE_EVENTS_SERVICE_ADDR = "nessie.events.service";

  /**
   * The prefix for all local event bus addresses used to exchange messages of type {@link Event}
   * between {@link QuarkusEventService} and {@link EventSubscriber}s.
   *
   * <p>Addresses are of the form {@code nessie.events.<event-type>}, where {@code <event-type>} is
   * the {@link EventType} name, e.g. {@code nessie.events.COMMIT}.
   */
  public static final String NESSIE_EVENTS_SUBSCRIBERS_ADDR_PREFIX = "nessie.events.subscribers.";

  private final EventBus bus;
  private final EventDeliveryFactory deliveryFactory;
  private final DeliveryOptions deliveryOptions;

  // Mandatory for CDI.
  @SuppressWarnings("unused")
  public QuarkusEventService() {
    this(null, null, null, null, null, null);
  }

  @Inject
  public QuarkusEventService(
      EventConfig config,
      EventFactory factory,
      EventSubscribers subscribers,
      EventBus bus,
      EventDeliveryFactory deliveryFactory,
      @Named(EventBusConfigurer.EVENTS_DELIVERY_OPTIONS_BEAN_NAME)
          DeliveryOptions deliveryOptions) {
    super(config, factory, subscribers);
    this.bus = bus;
    this.deliveryFactory = deliveryFactory;
    this.deliveryOptions = deliveryOptions;
  }

  public void onStartup(@Observes StartupEvent event) {
    start();
    for (Map.Entry<EventSubscription, EventSubscriber> entry :
        subscribers.getSubscriptions().entrySet()) {
      EventSubscription subscription = entry.getKey();
      EventSubscriber subscriber = entry.getValue();
      Handler<Message<Event>> handler = e -> deliverEvent(e.body(), subscriber, subscription);
      for (EventType eventType : EventType.values()) {
        if (subscriber.accepts(eventType)) {
          String address = NESSIE_EVENTS_SUBSCRIBERS_ADDR_PREFIX + eventType;
          MessageConsumer<Event> consumer = bus.localConsumer(address);
          consumer.handler(handler);
        }
      }
    }
  }

  public void onShutdown(@Observes ShutdownEvent event) {
    close();
  }

  @ConsumeEvent(NESSIE_EVENTS_SERVICE_ADDR)
  @Override
  public void onVersionStoreEvent(VersionStoreEvent event) {
    super.onVersionStoreEvent(event);
  }

  @Override
  protected void fireEvent(Event event) {
    // Publish the event to all interested subscribers that are listening to this address.
    String address = NESSIE_EVENTS_SUBSCRIBERS_ADDR_PREFIX + event.getType();
    bus.publish(address, event, deliveryOptions);
  }

  @Override
  protected void deliverEvent(
      Event event, EventSubscriber subscriber, EventSubscription subscription) {
    EventDelivery delivery = deliveryFactory.create(event, subscriber, subscription);
    delivery.start();
  }
}
