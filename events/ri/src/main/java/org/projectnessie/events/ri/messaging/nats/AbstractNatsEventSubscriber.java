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
package org.projectnessie.events.ri.messaging.nats;

import io.quarkiverse.reactive.messaging.nats.jetstream.client.api.PublishMessageMetadata;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.projectnessie.events.api.Event;
import org.projectnessie.events.ri.messaging.AbstractMessagingEventSubscriber;
import org.projectnessie.events.ri.messaging.config.MessagingEventSubscribersConfig.EventSubscriberConfig;
import org.projectnessie.events.spi.EventSubscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link EventSubscriber} that publishes events to a NATS stream.
 *
 * @param <T> The NATS record value type emitted by this subscriber.
 */
public abstract class AbstractNatsEventSubscriber<T> extends AbstractMessagingEventSubscriber<T> {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractNatsEventSubscriber.class);

  public AbstractNatsEventSubscriber(Emitter<T> emitter, EventSubscriberConfig config) {
    super(emitter, config);
  }

  @Override
  protected Message<T> createMessage(Event upstreamEvent, T messagePayload) {
    Map<String, List<String>> headers = new HashMap<>();
    createHeaders(upstreamEvent, (name, value) -> headers.put(name, List.of(value)));
    PublishMessageMetadata metadata =
        PublishMessageMetadata.builder()
            .messageId(upstreamEvent.getIdAsText())
            .headers(headers)
            .stream("nessie-events")
            .subject(subject(upstreamEvent))
            .build();
    return Message.of(messagePayload, Metadata.of(metadata));
  }

  @Override
  protected CompletionStage<Void> onWriteAck(Metadata metadata) {
    // Do NOT enable this log statement in production!
    if (LOGGER.isDebugEnabled()) {
      PublishMessageMetadata jetStreamMetadata =
          metadata.get(PublishMessageMetadata.class).orElseThrow();
      String id = jetStreamMetadata.messageId();
      String subject = jetStreamMetadata.subject();
      LOGGER.debug("Event written: messageId={}, subject={}", id, subject);
    }
    return CompletableFuture.completedFuture(null); // immediate ack
  }

  @Override
  protected CompletionStage<Void> onWriteNack(Throwable error, Metadata metadata) {
    PublishMessageMetadata jetStreamMetadata =
        metadata.get(PublishMessageMetadata.class).orElseThrow();
    String id = jetStreamMetadata.messageId();
    String subject = jetStreamMetadata.subject();
    LOGGER.error("Failed to write event: messageId={}, subject={}", id, subject, error);
    return CompletableFuture.completedFuture(null); // immediate ack
  }

  public static String subject(Event event) {
    return switch (event.getType()) {
      case MERGE -> "nessie.events.merge";
      case TRANSPLANT -> "nessie.events.transplant";
      case COMMIT -> "nessie.events.commit";
      case CONTENT_STORED -> "nessie.events.commit.content.stored";
      case CONTENT_REMOVED -> "nessie.events.commit.content.removed";
      case REFERENCE_CREATED -> "nessie.events.reference.created";
      case REFERENCE_UPDATED -> "nessie.events.reference.updated";
      case REFERENCE_DELETED -> "nessie.events.reference.deleted";
    };
  }
}
