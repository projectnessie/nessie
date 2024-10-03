/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.events.ri.messaging.kafka.json;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.projectnessie.events.api.Event;
import org.projectnessie.events.ri.messaging.config.MessagingEventSubscribersConfig;
import org.projectnessie.events.ri.messaging.config.MessagingEventSubscribersConfig.EventSubscriberConfig;
import org.projectnessie.events.ri.messaging.kafka.AbstractKafkaEventSubscriber;
import org.projectnessie.events.spi.EventSubscriber;

/** An {@link EventSubscriber} that publishes events to a Kafka topic using JSON. */
@ApplicationScoped
public class KafkaJsonEventSubscriber extends AbstractKafkaEventSubscriber<Event> {

  public static final String CHANNEL = "nessie-kafka-json";

  /** Constructor required by CDI. */
  @SuppressWarnings("unused")
  public KafkaJsonEventSubscriber() {
    super(null, null);
  }

  @Inject
  public KafkaJsonEventSubscriber(
      @Channel(CHANNEL) Emitter<Event> emitter, MessagingEventSubscribersConfig config) {
    super(emitter, config.subscribers().getOrDefault(CHANNEL, EventSubscriberConfig.EMPTY));
  }

  @Override
  public void onEvent(Event event) {
    fireEvent(event, event);
  }
}
