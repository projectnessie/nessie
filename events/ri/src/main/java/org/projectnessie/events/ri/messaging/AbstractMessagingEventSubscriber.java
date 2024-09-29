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
package org.projectnessie.events.ri.messaging;

import static org.projectnessie.events.ri.messaging.MessageHeaders.API_VERSION;
import static org.projectnessie.events.ri.messaging.MessageHeaders.COMMIT_CREATION_TIME;
import static org.projectnessie.events.ri.messaging.MessageHeaders.EVENT_CREATION_TIME;
import static org.projectnessie.events.ri.messaging.MessageHeaders.EVENT_ID;
import static org.projectnessie.events.ri.messaging.MessageHeaders.EVENT_TYPE;
import static org.projectnessie.events.ri.messaging.MessageHeaders.INITIATOR;
import static org.projectnessie.events.ri.messaging.MessageHeaders.REPOSITORY_ID;
import static org.projectnessie.events.ri.messaging.MessageHeaders.SPEC_VERSION;

import java.time.Instant;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.projectnessie.events.api.CommitEvent;
import org.projectnessie.events.api.ContentEvent;
import org.projectnessie.events.api.Event;
import org.projectnessie.events.api.EventType;
import org.projectnessie.events.ri.messaging.config.MessagingEventSubscribersConfig.EventSubscriberConfig;
import org.projectnessie.events.spi.EventFilter;
import org.projectnessie.events.spi.EventSubscriber;
import org.projectnessie.events.spi.EventSubscription;
import org.projectnessie.events.spi.EventTypeFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link EventSubscriber} that publishes events to any compatible messaging system.
 *
 * @param <T> The record value type emitted by this subscriber.
 */
public abstract class AbstractMessagingEventSubscriber<T> implements EventSubscriber {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(AbstractMessagingEventSubscriber.class);

  private final Emitter<T> emitter;
  private final EventSubscriberConfig config;

  private String serverSpecVersion;
  private String apiVersion;

  private EventFilter eventFilter;
  private EventTypeFilter eventTypeFilter;

  public AbstractMessagingEventSubscriber(Emitter<T> emitter, EventSubscriberConfig config) {
    this.emitter = emitter;
    this.config = config;
  }

  @Override
  public boolean isBlocking() {
    // We don't wait for the broker acknowledgement, so we can tell Nessie
    // that we don't do blocking I/O.
    return false;
  }

  @Override
  public void onSubscribe(EventSubscription subscription) {
    // Use this method to initialize the subscriber.
    // You can also use the passed-in EventSubscription to inspect the Nessie system configuration.

    // Save these for later, they will be included in all outgoing message headers.
    serverSpecVersion = subscription.getSystemConfiguration().getSpecVersion();
    apiVersion = String.valueOf(subscription.getSystemConfiguration().getMaxSupportedApiVersion());

    // Create the event filter based on the repositoryIds set in the configuration.
    // If the repositoryIds set is empty, we want to watch all repositories.
    // Otherwise, we only want to watch to the repositories in the set, and discard all others
    // (Nessie won't even send events from such repositories to us).
    Set<String> ids = config.repositoryIds().orElse(Set.of());
    eventFilter = ids.isEmpty() ? EventFilter.all() : e -> ids.contains(e.getRepositoryId());

    // Create the event type filter based on the eventTypes set in the configuration.
    // If the eventTypes set is empty, we want to watch all event types.
    // Otherwise, we only want to watch the event types in the set, and discard all others
    // (Nessie won't even send events of such types to us).
    Set<EventType> eventTypes = config.eventTypes().orElse(Set.of());
    eventTypeFilter = eventTypes.isEmpty() ? EventTypeFilter.all() : EventTypeFilter.of(eventTypes);

    LOGGER.info("Nessie subscription id: {}", subscription.getId());
  }

  @Override
  public final EventFilter getEventFilter() {
    return eventFilter;
  }

  @Override
  public final EventTypeFilter getEventTypeFilter() {
    return eventTypeFilter;
  }

  @Override
  public void close() {
    // Called when the Nessie server is stopped. Use it to close any resources, such as the
    // underlying message producer.
    emitter.complete();
    LOGGER.info("Event subscriber closed");
  }

  /**
   * Sends the event to the messaging system.
   *
   * <p>This method is invoked on a Nessie thread, so it should not block for too long. It is
   * therefore advisable to use either the fire-and-forget method, or the so-called asynchronous
   * send method with callback, so that the Nessie server is not blocked by a slow broker.
   */
  protected void fireEvent(Event upstreamEvent, T downstreamEvent) {
    try {
      Message<T> message = createMessage(upstreamEvent, downstreamEvent);
      message =
          message.withAckWithMetadata(this::onWriteAck).withNackWithMetadata(this::onWriteNack);
      emitter.send(message);
    } catch (RuntimeException e) {
      LOGGER.error("Failed to send event with id={}", upstreamEvent.getIdAsText(), e);
      throw e;
    }
  }

  /**
   * Creates a message to be sent to the messaging system.
   *
   * @param upstreamEvent The event received from Nessie.
   * @param messagePayload The message payload to send to the messaging system.
   * @return The message to be sent to the messaging system.
   */
  protected abstract Message<T> createMessage(Event upstreamEvent, T messagePayload);

  /**
   * Creates the headers for the message to be sent to the messaging system.
   *
   * @param event The event to be sent.
   * @param headers A consumer that accepts the header name and value.
   */
  protected void createHeaders(Event event, BiConsumer<String, String> headers) {
    headers.accept(EVENT_ID.key(), event.getIdAsText());
    headers.accept(SPEC_VERSION.key(), serverSpecVersion);
    headers.accept(API_VERSION.key(), apiVersion);
    headers.accept(EVENT_TYPE.key(), event.getType().name());
    headers.accept(REPOSITORY_ID.key(), event.getRepositoryId());
    event.getEventInitiator().ifPresent(user -> headers.accept(INITIATOR.key(), user));
    headers.accept(EVENT_CREATION_TIME.key(), event.getEventCreationTimestamp().toString());
    // For commits and operations, add the commit creation time as well
    if (event instanceof CommitEvent commitEvent) {
      Instant commitTime = Objects.requireNonNull(commitEvent.getCommitMeta().getCommitTime());
      headers.accept(COMMIT_CREATION_TIME.key(), commitTime.toString());
    } else if (event instanceof ContentEvent contentEvent) {
      Instant commitTime = contentEvent.getCommitCreationTimestamp();
      headers.accept(COMMIT_CREATION_TIME.key(), commitTime.toString());
    }
  }

  /**
   * Called when the message has been successfully written to the messaging system.
   *
   * @param metadata The metadata of the message that was written.
   * @return A completion stage that completes when the ack has been processed.
   */
  protected abstract CompletionStage<Void> onWriteAck(Metadata metadata);

  /**
   * Called when the message could not be written to the messaging system.
   *
   * @param error The error that occurred.
   * @param metadata The metadata of the message that was not written.
   * @return A completion stage that completes when the nack has been processed.
   */
  protected abstract CompletionStage<Void> onWriteNack(Throwable error, Metadata metadata);
}
